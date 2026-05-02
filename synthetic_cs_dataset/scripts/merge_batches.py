#!/usr/bin/env python3
"""Merge multiple generated synthetic dataset batches into one folder."""

from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
from pathlib import Path
from typing import Any


METADATA_FIELDS = [
    "file_name",
    "text",
    "source",
    "domain",
    "language_mix",
    "speaker_id",
    "is_synthetic",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="Merged output folder.")
    parser.add_argument("--batches", type=Path, nargs="+", required=True, help="Batch folders to merge.")
    parser.add_argument("--overwrite", action="store_true", help="Replace output folder if it already exists.")
    parser.add_argument(
        "--dedupe-text",
        action="store_true",
        help="Keep only the first row for each exact normalized transcript.",
    )
    return parser.parse_args()


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def normalize_text(text: str) -> str:
    text = re.sub(r"\s+", " ", text.strip().lower())
    return re.sub(r"[،,.!?؟;:]+", "", text)


def safe_part(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip())
    return cleaned.strip("._") or "batch"


def language_mix_to_metadata(value: Any) -> str:
    if isinstance(value, list):
        return "|".join(str(item) for item in value)
    return str(value or "")


def source_audio_path(batch_dir: Path, row: dict[str, Any]) -> tuple[Path, str]:
    file_name = str(row.get("file_name") or row.get("audio") or "").strip()
    if not file_name:
        raise FileNotFoundError(f"Row has no file_name/audio: {row.get('id', '<missing id>')}")
    path = Path(file_name)
    if not path.is_absolute():
        path = batch_dir / path
    return path, file_name


def prepare_output(output: Path, overwrite: bool) -> None:
    if output.exists():
        if not overwrite:
            raise SystemExit(f"Output folder already exists: {output}. Use --overwrite or choose another folder.")
        shutil.rmtree(output)
    (output / "audio").mkdir(parents=True, exist_ok=True)


def merge_batches(batch_dirs: list[Path], output: Path, dedupe_text: bool) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    dataset_rows: list[dict[str, Any]] = []
    metadata_rows: list[dict[str, Any]] = []
    seen_texts: set[str] = set()
    row_index = 1

    for batch_index, batch_dir in enumerate(batch_dirs, start=1):
        batch_dir = batch_dir.resolve()
        rows = read_jsonl(batch_dir / "dataset.jsonl")
        if not rows:
            raise SystemExit(f"No dataset.jsonl rows found in {batch_dir}")

        batch_label = safe_part(batch_dir.name or f"batch_{batch_index:02d}")
        for row in rows:
            text = str(row.get("text") or "").strip()
            if dedupe_text:
                key = normalize_text(text)
                if key in seen_texts:
                    continue
                seen_texts.add(key)

            source_audio, original_file = source_audio_path(batch_dir, row)
            if not source_audio.exists():
                raise SystemExit(f"Missing audio file: {source_audio}")

            new_id = f"cs_{row_index:06d}"
            extension = source_audio.suffix or ".wav"
            new_file_name = f"audio/{new_id}_{batch_label}{extension}"
            shutil.copy2(source_audio, output / new_file_name)

            merged = dict(row)
            merged["id"] = new_id
            merged["source_id"] = row.get("id", "")
            merged["source_batch"] = batch_label
            merged["source_file_name"] = original_file
            merged["file_name"] = new_file_name
            merged["audio"] = new_file_name
            dataset_rows.append(merged)

            metadata_rows.append(
                {
                    "file_name": new_file_name,
                    "text": text,
                    "source": row.get("source", ""),
                    "domain": row.get("domain", ""),
                    "language_mix": language_mix_to_metadata(row.get("language_mix", "")),
                    "speaker_id": row.get("speaker_id", ""),
                    "is_synthetic": str(row.get("is_synthetic", True)).lower(),
                }
            )
            row_index += 1

    return dataset_rows, metadata_rows


def write_metadata(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=METADATA_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in METADATA_FIELDS})


def write_texts(path: Path, dataset_rows: list[dict[str, Any]]) -> None:
    seen: set[str] = set()
    text_rows: list[dict[str, Any]] = []
    for row in dataset_rows:
        key = normalize_text(str(row.get("text", "")))
        if key in seen:
            continue
        seen.add(key)
        text_rows.append(
            {
                "id": row["id"],
                "domain": row.get("domain", ""),
                "text": row.get("text", ""),
                "language_mix": row.get("language_mix", []),
                "contains_code_switch": row.get("contains_code_switch", False),
            }
        )
    write_jsonl(path, text_rows)


def main() -> None:
    args = parse_args()
    output = args.output.resolve()
    prepare_output(output, args.overwrite)
    dataset_rows, metadata_rows = merge_batches(args.batches, output, args.dedupe_text)

    write_jsonl(output / "dataset.jsonl", dataset_rows)
    write_metadata(output / "metadata.csv", metadata_rows)
    write_texts(output / "texts.jsonl", dataset_rows)

    print(f"Merged {len(dataset_rows)} audio rows into {output}")
    print(f"Wrote {output / 'dataset.jsonl'}")
    print(f"Wrote {output / 'metadata.csv'}")
    print(f"Audio folder: {output / 'audio'}")


if __name__ == "__main__":
    main()

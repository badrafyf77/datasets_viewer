#!/usr/bin/env python3
"""Prepare dataset files for the static Darija Dataset Viewer.

This script is meant to run on the machine that has the cleaned datasets.
It copies browser-readable files, converts Parquet or Hugging Face datasets
when optional dependencies are installed, and writes viewer-manifest.json.
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Any, Iterable


DATA_EXTENSIONS = {".csv", ".tsv", ".json", ".jsonl", ".ndjson", ".txt"}
AUDIO_EXTENSIONS = {".wav", ".mp3", ".m4a", ".ogg", ".oga", ".aac", ".flac", ".webm"}
CONVERTIBLE_EXTENSIONS = {".parquet"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare datasets for the browser viewer.")
    parser.add_argument("source", type=Path, help="Folder or dataset file to scan.")
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("viewer_data"),
        help="Output folder for copied or converted files.",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("viewer-manifest.json"),
        help="Manifest path to write.",
    )
    parser.add_argument(
        "--copy-audio",
        action="store_true",
        help="Copy audio files into OUT/audio, preserving relative paths.",
    )
    parser.add_argument(
        "--name-prefix",
        default="",
        help="Optional prefix added to dataset names in the manifest.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source = args.source.expanduser().resolve()
    out_dir = args.out.resolve()
    manifest_path = args.manifest.resolve()

    out_dir.mkdir(parents=True, exist_ok=True)
    datasets: list[dict[str, Any]] = []

    if source.is_file():
        datasets.extend(prepare_file(source, source.parent, out_dir, args.name_prefix))
    else:
        datasets.extend(prepare_tree(source, out_dir, args.name_prefix))
        if args.copy_audio:
            copied_audio = copy_audio_files(source, out_dir / "audio")
            print(f"Copied {copied_audio} audio file(s).")

    manifest = {
        "audioRoot": relative_url(out_dir / "audio", manifest_path.parent)
        if args.copy_audio
        else "",
        "datasets": datasets,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(f"Wrote {manifest_path}")
    print(f"Prepared {len(datasets)} dataset entr{'y' if len(datasets) == 1 else 'ies'}.")


def prepare_tree(source: Path, out_dir: Path, name_prefix: str) -> list[dict[str, Any]]:
    datasets: list[dict[str, Any]] = []
    skipped_hf_children: set[Path] = set()

    for path in sorted(source.rglob("*")):
        if not path.exists() or any(parent in skipped_hf_children for parent in path.parents):
            continue

        if path.is_dir() and is_huggingface_dataset(path):
            datasets.extend(export_huggingface_dataset(path, source, out_dir, name_prefix))
            skipped_hf_children.add(path)
            continue

        if path.is_file():
            datasets.extend(prepare_file(path, source, out_dir, name_prefix))

    return datasets


def prepare_file(path: Path, root: Path, out_dir: Path, name_prefix: str) -> list[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix in AUDIO_EXTENSIONS:
        return []

    relative = safe_relative(path, root)
    if suffix in DATA_EXTENSIONS:
        target = out_dir / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
        return [manifest_entry(target, out_dir.parent, name_prefix, final=is_final_name(path))]

    if suffix in CONVERTIBLE_EXTENSIONS:
        target = (out_dir / relative).with_suffix(".jsonl")
        export_parquet(path, target)
        return [manifest_entry(target, out_dir.parent, name_prefix, final=is_final_name(path))]

    return []


def export_parquet(path: Path, target: Path) -> None:
    try:
        import pandas as pd
    except ImportError as exc:
        raise SystemExit("Install pandas and pyarrow to convert Parquet files.") from exc

    target.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.read_parquet(path)
    write_jsonl((clean_value(record) for record in frame.to_dict(orient="records")), target)


def export_huggingface_dataset(
    path: Path, root: Path, out_dir: Path, name_prefix: str
) -> list[dict[str, Any]]:
    try:
        from datasets import Audio, Dataset, DatasetDict, load_from_disk
    except ImportError as exc:
        raise SystemExit("Install datasets to convert Hugging Face save_to_disk folders.") from exc

    loaded = load_from_disk(str(path))
    relative = safe_relative(path, root)
    entries: list[dict[str, Any]] = []

    if isinstance(loaded, DatasetDict):
        for split, dataset in loaded.items():
            prepared = prepare_hf_split(dataset, Audio)
            target = out_dir / relative / f"{split}.jsonl"
            write_jsonl((clean_value(row) for row in prepared), target)
            entries.append(
                manifest_entry(
                    target,
                    out_dir.parent,
                    name_prefix,
                    name=f"{path.name} {split}",
                    final=is_final_name(path),
                )
            )
        return entries

    if isinstance(loaded, Dataset):
        prepared = prepare_hf_split(loaded, Audio)
        target = out_dir / relative.with_suffix(".jsonl")
        write_jsonl((clean_value(row) for row in prepared), target)
        return [
            manifest_entry(
                target,
                out_dir.parent,
                name_prefix,
                name=path.name,
                final=is_final_name(path),
            )
        ]

    return entries


def prepare_hf_split(dataset: Any, audio_type: Any) -> Any:
    prepared = dataset
    for column, feature in getattr(dataset, "features", {}).items():
        if feature.__class__.__name__ == "Audio":
            prepared = prepared.cast_column(column, audio_type(decode=False))
    return prepared


def write_jsonl(records: Iterable[Any], target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(clean_value(record), ensure_ascii=False) + "\n")


def clean_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        return f"[bytes:{len(value)}]"
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): clean_value(item) for key, item in value.items() if key != "bytes"}
    if isinstance(value, (list, tuple)):
        if len(value) > 24:
            return f"[array:{len(value)} items]"
        return [clean_value(item) for item in value]
    if hasattr(value, "item"):
        try:
            return clean_value(value.item())
        except Exception:
            pass
    if hasattr(value, "shape"):
        shape = "x".join(str(part) for part in value.shape)
        return f"[array:{shape}]"
    return str(value)


def copy_audio_files(source: Path, audio_out: Path) -> int:
    copied = 0
    for path in source.rglob("*"):
        if path.is_file() and path.suffix.lower() in AUDIO_EXTENSIONS:
            target = audio_out / safe_relative(path, source)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)
            copied += 1
    return copied


def is_huggingface_dataset(path: Path) -> bool:
    return (path / "dataset_info.json").exists() or (path / "dataset_dict.json").exists()


def is_final_name(path: Path) -> bool:
    lowered = str(path).lower().replace("\\", "/")
    markers = ("final", "clean-final", "cleaned-final", "final-clean", "final_dataset", "gold", "cleaned")
    return any(marker in lowered for marker in markers)


def manifest_entry(
    target: Path,
    site_root: Path,
    name_prefix: str,
    *,
    name: str | None = None,
    final: bool = False,
) -> dict[str, Any]:
    label = name or target.stem.replace("_", " ").replace("-", " ").title()
    if name_prefix:
        label = f"{name_prefix} {label}".strip()
    return {
        "name": label,
        "path": relative_url(target, site_root),
        "final": final,
    }


def relative_url(path: Path, root: Path) -> str:
    try:
        relative = path.resolve().relative_to(root.resolve())
    except ValueError:
        relative = path.resolve()
    return relative.as_posix()


def safe_relative(path: Path, root: Path) -> Path:
    try:
        return path.resolve().relative_to(root.resolve())
    except ValueError:
        return Path(path.name)


if __name__ == "__main__":
    main()

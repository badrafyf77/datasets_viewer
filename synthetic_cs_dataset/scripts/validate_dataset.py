#!/usr/bin/env python3
"""Validate a generated synthetic ASR dataset folder."""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import soundfile as sf


ARABIC_RE = re.compile(r"[\u0600-\u06FF]")
LATIN_RE = re.compile(r"[A-Za-zÀ-ÖØ-öø-ÿ]")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data_dir", type=Path, required=True)
    parser.add_argument("--allowed-duplicate-text-versions", type=int, default=4)
    return parser.parse_args()


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def normalize_text(text: str) -> str:
    text = re.sub(r"\s+", " ", text.strip().lower())
    return re.sub(r"[،,.!?؟;:]+", "", text)


def audio_duration(path: Path) -> tuple[float, str]:
    try:
        info = sf.info(path)
    except Exception as exc:
        return 0.0, f"audio_read_failed:{exc}"
    if not info.samplerate:
        return 0.0, "missing_samplerate"
    return float(info.frames / info.samplerate), ""


def language_mix_key(row: dict[str, Any]) -> str:
    value = row.get("language_mix", [])
    if isinstance(value, list):
        return "|".join(value)
    return str(value)


def script_stats(texts: list[str]) -> dict[str, float]:
    arabic = sum(len(ARABIC_RE.findall(text)) for text in texts)
    latin = sum(len(LATIN_RE.findall(text)) for text in texts)
    total = arabic + latin
    return {
        "arabic_chars": arabic,
        "latin_chars": latin,
        "arabic_ratio": round(arabic / total, 4) if total else 0.0,
        "latin_ratio": round(latin / total, 4) if total else 0.0,
    }


def write_bad_samples(path: Path, bad_rows: list[dict[str, Any]]) -> None:
    fields = [
        "id",
        "file_name",
        "text",
        "domain",
        "language_mix",
        "split",
        "duration_seconds",
        "reason",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in bad_rows:
            output = dict(row)
            if isinstance(output.get("language_mix"), list):
                output["language_mix"] = "|".join(output["language_mix"])
            writer.writerow({field: output.get(field, "") for field in fields})


def main() -> None:
    args = parse_args()
    data_dir = args.data_dir.resolve()
    dataset_path = data_dir / "dataset.jsonl"
    bad_path = data_dir / "bad_samples.csv"
    if not dataset_path.exists():
        raise SystemExit(f"Missing {dataset_path}")

    rows = read_jsonl(dataset_path)
    bad_rows: list[dict[str, Any]] = []
    durations: list[float] = []
    text_counts = Counter(normalize_text(str(row.get("text", ""))) for row in rows)
    domain_counts = Counter(str(row.get("domain", "")) for row in rows)
    mix_counts = Counter(language_mix_key(row) for row in rows)
    split_counts = Counter(str(row.get("split", "")) for row in rows)
    latin_presence = Counter()
    duration_by_split: dict[str, float] = defaultdict(float)

    for row in rows:
        reasons: list[str] = []
        text = str(row.get("text", "")).strip()
        file_name = str(row.get("file_name") or row.get("audio") or "")
        audio_path = Path(file_name)
        if not audio_path.is_absolute():
            audio_path = data_dir / audio_path

        if not file_name or not audio_path.exists():
            reasons.append("missing_audio")
            duration = 0.0
        else:
            duration, audio_error = audio_duration(audio_path)
            if audio_error:
                reasons.append(audio_error)
            else:
                durations.append(duration)
                row["duration_seconds"] = round(duration, 3)
                duration_by_split[str(row.get("split", ""))] += duration

        if not text:
            reasons.append("empty_transcript")
        if text and not ARABIC_RE.search(text):
            reasons.append("no_arabic_script")
        if text and LATIN_RE.search(text):
            latin_presence["with_latin"] += 1
        else:
            latin_presence["without_latin"] += 1

        mix_value = language_mix_key(row)
        if ("french" in mix_value or "english" in mix_value) and not LATIN_RE.search(text):
            reasons.append("declared_code_switch_without_latin")
        if text_counts[normalize_text(text)] > args.allowed_duplicate_text_versions:
            reasons.append("too_many_duplicate_text_versions")

        if reasons:
            bad = dict(row)
            bad["reason"] = "|".join(reasons)
            bad_rows.append(bad)

    write_bad_samples(bad_path, bad_rows)

    total_seconds = sum(durations)
    print("Dataset validation")
    print(f"Rows: {len(rows)}")
    print(f"Audio files checked: {len(durations)}")
    print(f"Total hours: {total_seconds / 3600.0:.3f}")
    if durations:
        print(f"Duration min/mean/max: {min(durations):.2f}s / {sum(durations) / len(durations):.2f}s / {max(durations):.2f}s")
    print(f"Bad samples: {len(bad_rows)} -> {bad_path}")
    print(f"Splits: {dict(split_counts)}")
    print(f"Hours by split: {dict((key, round(value / 3600.0, 3)) for key, value in duration_by_split.items())}")
    print(f"Domains: {dict(domain_counts)}")
    print(f"Language mix: {dict(mix_counts)}")
    print(f"Latin presence: {dict(latin_presence)}")
    print(f"Script ratios: {script_stats([str(row.get('text', '')) for row in rows])}")


if __name__ == "__main__":
    main()

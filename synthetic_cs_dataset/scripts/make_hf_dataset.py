#!/usr/bin/env python3
"""Create a Hugging Face DatasetDict from generated dataset.jsonl."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data_dir", type=Path, required=True)
    parser.add_argument("--output_dir", type=Path, default=None)
    parser.add_argument("--sampling-rate", type=int, default=24000)
    return parser.parse_args()


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def main() -> None:
    args = parse_args()
    try:
        from datasets import Audio, Dataset, DatasetDict
    except ImportError as exc:  # pragma: no cover - startup dependency check
        raise SystemExit("Install datasets first: pip install datasets") from exc

    data_dir = args.data_dir.resolve()
    dataset_path = data_dir / "dataset.jsonl"
    output_dir = args.output_dir.resolve() if args.output_dir else data_dir / "hf_dataset"
    if not dataset_path.exists():
        raise SystemExit(f"Missing {dataset_path}")

    rows = read_jsonl(dataset_path)
    by_split: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        file_name = Path(str(row.get("file_name") or row.get("audio")))
        audio_path = file_name if file_name.is_absolute() else data_dir / file_name
        item = dict(row)
        item["audio"] = str(audio_path)
        by_split[str(row.get("split", "train"))].append(item)

    dataset_dict = DatasetDict()
    for split, split_rows in sorted(by_split.items()):
        dataset = Dataset.from_list(split_rows)
        dataset = dataset.cast_column("audio", Audio(sampling_rate=args.sampling_rate))
        dataset_dict[split] = dataset

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    dataset_dict.save_to_disk(str(output_dir))
    print(f"Saved Hugging Face DatasetDict to {output_dir}")
    for split, dataset in dataset_dict.items():
        print(f"{split}: {len(dataset)} rows")


if __name__ == "__main__":
    main()

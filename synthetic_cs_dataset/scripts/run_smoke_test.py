#!/usr/bin/env python3
"""Run a one-text, one-audio smoke test for the synthetic CS pipeline."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/smoke_generation.yaml"),
        help="Smoke-test config relative to the synthetic_cs_dataset folder.",
    )
    parser.add_argument("--keep-existing", action="store_true", help="Do not clear data/smoke_test first.")
    parser.add_argument("--timeout", type=int, default=900, help="Timeout per stage in seconds.")
    return parser.parse_args()


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def run_step(command: list[str], cwd: Path, timeout: int) -> dict[str, Any]:
    result = subprocess.run(
        command,
        cwd=str(cwd),
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    payload = {
        "command": command,
        "returncode": result.returncode,
        "stdout": result.stdout[-4000:],
        "stderr": result.stderr[-4000:],
    }
    if result.returncode != 0:
        raise RuntimeError(json.dumps(payload, ensure_ascii=False))
    return payload


def main() -> None:
    args = parse_args()
    project_dir = Path(__file__).resolve().parent.parent
    config_path = args.config if args.config.is_absolute() else project_dir / args.config
    smoke_dir = project_dir / "data" / "smoke_test"

    if not args.keep_existing and smoke_dir.exists():
        shutil.rmtree(smoke_dir)
    smoke_dir.mkdir(parents=True, exist_ok=True)

    steps: list[dict[str, Any]] = []
    try:
        steps.append(
            run_step(
                [
                    sys.executable,
                    "scripts/generate_texts.py",
                    "--config",
                    str(config_path),
                    "--num-texts",
                    "1",
                    "--batch-size",
                    "1",
                ],
                project_dir,
                args.timeout,
            )
        )
        steps.append(
            run_step(
                [
                    sys.executable,
                    "scripts/generate_audio.py",
                    "--config",
                    str(config_path),
                    "--limit",
                    "1",
                    "--target-hours",
                    "0.01",
                ],
                project_dir,
                args.timeout,
            )
        )
    except Exception as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": str(exc),
                    "data_dir": str(smoke_dir),
                    "steps": steps,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        raise SystemExit(1) from exc

    dataset_path = smoke_dir / "dataset.jsonl"
    text_path = smoke_dir / "texts.jsonl"
    rows = read_jsonl(dataset_path) if dataset_path.exists() else []
    text_rows = read_jsonl(text_path) if text_path.exists() else []
    if not rows:
        raise SystemExit("Smoke test finished but no dataset rows were created.")

    first = rows[0]
    audio_path = smoke_dir / str(first.get("file_name") or first.get("audio") or "")
    payload = {
        "ok": True,
        "data_dir": str(smoke_dir),
        "texts_path": str(text_path),
        "dataset_path": str(dataset_path),
        "metadata_path": str(smoke_dir / "metadata.csv"),
        "audio_path": str(audio_path),
        "text": first.get("text") or (text_rows[0].get("text") if text_rows else ""),
        "rows": rows,
        "steps": steps,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

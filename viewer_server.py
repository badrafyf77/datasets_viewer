#!/usr/bin/env python3
"""Serve the Darija Dataset Viewer and read a local dataset path.

Run this on Lightning instead of `python3 -m http.server`:

    python3 viewer_server.py /teamspace/studios/this_studio/darija_clean --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import os
import subprocess
import sys
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urlparse


AUDIO_EXTENSIONS = {".wav", ".mp3", ".m4a", ".ogg", ".oga", ".aac", ".flac", ".webm"}
DEFAULT_DATASET_PATH = Path("/teamspace/studios/this_studio/darija_clean")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Serve the dataset viewer with a local dataset API.")
    parser.add_argument(
        "dataset_path",
        nargs="?",
        type=Path,
        default=Path(os.environ.get("DATASET_PATH", DEFAULT_DATASET_PATH)),
        help="Path to a Hugging Face save_to_disk dataset or dataset_dict folder.",
    )
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind.")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind.")
    parser.add_argument(
        "--max-rows",
        type=int,
        default=int(os.environ.get("VIEWER_MAX_ROWS", "0")),
        help="Rows per split to send. 0 means all rows.",
    )
    return parser.parse_args()


class DatasetViewerHandler(SimpleHTTPRequestHandler):
    dataset_path: Path
    max_rows: int
    site_root: Path
    dataset_cache: dict | None = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(self.site_root), **kwargs)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/datasets":
            payload = self.load_datasets()
            if payload is not None:
                self.send_json(payload)
            return
        if parsed.path == "/api/audio":
            self.send_audio(parsed.query)
            return
        if parsed.path == "/api/audio-row":
            self.send_audio_row(parsed.query)
            return
        if parsed.path == "/api/synthetic-test-audio":
            self.send_synthetic_test_audio(parsed.query)
            return
        super().do_GET()

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/synthetic-test":
            self.run_synthetic_test()
            return
        self.send_error(HTTPStatus.NOT_FOUND, "Unknown endpoint")

    def send_json(self, payload: object) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_error_json(self, status: HTTPStatus, message: str) -> None:
        body = json.dumps({"error": message}, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def load_datasets(self) -> dict | None:
        if self.__class__.dataset_cache is not None:
            return self.__class__.dataset_cache

        path = self.dataset_path.expanduser().resolve()
        try:
            datasets = load_huggingface_dataset(path, self.max_rows)
        except Exception as exc:
            self.send_error_json(HTTPStatus.INTERNAL_SERVER_ERROR, str(exc))
            return None

        payload = {"path": str(path), "datasets": datasets}
        self.__class__.dataset_cache = payload
        return payload

    def send_audio(self, query: str) -> None:
        params = parse_qs(query)
        raw_path = params.get("path", [""])[0]
        if not raw_path:
            self.send_error(HTTPStatus.BAD_REQUEST, "Missing path")
            return

        dataset_root = self.dataset_path.expanduser().resolve()
        audio_path = Path(unquote(raw_path)).expanduser()
        if not audio_path.is_absolute():
            audio_path = dataset_root / audio_path
        audio_path = audio_path.resolve()

        if not is_allowed_audio_path(audio_path, dataset_root):
            self.send_error(HTTPStatus.FORBIDDEN, "Audio path is outside the dataset folder")
            return
        if not audio_path.exists() or not audio_path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND, "Audio file not found")
            return

        content_type = mimetypes.guess_type(audio_path.name)[0] or "application/octet-stream"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(audio_path.stat().st_size))
        self.end_headers()
        with audio_path.open("rb") as handle:
            self.copyfile(handle, self.wfile)

    def run_synthetic_test(self) -> None:
        pipeline_dir = self.site_root / "synthetic_cs_dataset"
        script_path = pipeline_dir / "scripts" / "run_smoke_test.py"
        if not script_path.exists():
            self.send_error_json(HTTPStatus.NOT_FOUND, "Synthetic pipeline script was not found.")
            return

        try:
            result = subprocess.run(
                [sys.executable, str(script_path)],
                cwd=str(pipeline_dir),
                check=False,
                capture_output=True,
                text=True,
                timeout=int(os.environ.get("SYNTHETIC_TEST_TIMEOUT", "1200")),
            )
        except subprocess.TimeoutExpired:
            self.send_error_json(HTTPStatus.GATEWAY_TIMEOUT, "Synthetic smoke test timed out.")
            return

        if result.returncode != 0:
            message = parse_smoke_error(result.stdout) or result.stderr.strip() or "Synthetic smoke test failed."
            self.send_error_json(HTTPStatus.INTERNAL_SERVER_ERROR, message[-4000:])
            return

        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError:
            self.send_error_json(HTTPStatus.INTERNAL_SERVER_ERROR, result.stdout[-4000:] or "Invalid smoke test output.")
            return

        rows = payload.get("rows") if isinstance(payload, dict) else []
        if not isinstance(rows, list) or not rows:
            self.send_error_json(HTTPStatus.INTERNAL_SERVER_ERROR, "Smoke test did not create any rows.")
            return

        records = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            record = dict(row)
            file_name = str(record.get("file_name") or record.get("audio") or "")
            if file_name:
                record["_play_audio"] = f"api/synthetic-test-audio?path={quote(file_name)}"
            records.append(record)

        response = {
            "ok": True,
            "data_dir": payload.get("data_dir"),
            "text": payload.get("text"),
            "audio_path": payload.get("audio_path"),
            "dataset": {
                "name": "Synthetic 1-sample smoke test",
                "path": payload.get("dataset_path") or payload.get("data_dir") or "synthetic smoke test",
                "records": records,
            },
        }
        self.send_json(response)

    def send_synthetic_test_audio(self, query: str) -> None:
        params = parse_qs(query)
        raw_path = params.get("path", [""])[0]
        if not raw_path:
            self.send_error(HTTPStatus.BAD_REQUEST, "Missing path")
            return

        smoke_root = (self.site_root / "synthetic_cs_dataset" / "data" / "smoke_test").resolve()
        audio_path = (smoke_root / unquote(raw_path)).resolve()
        if not is_allowed_audio_path(audio_path, smoke_root):
            self.send_error(HTTPStatus.FORBIDDEN, "Audio path is outside the smoke-test folder")
            return
        if not audio_path.exists() or not audio_path.is_file():
            self.send_error(HTTPStatus.NOT_FOUND, "Audio file not found")
            return
        self.send_file_audio(audio_path)

    def send_audio_row(self, query: str) -> None:
        params = parse_qs(query)
        split = params.get("split", [""])[0]
        column = params.get("column", [""])[0]
        row_text = params.get("row", [""])[0]
        if not split or not column or not row_text.isdigit():
            self.send_error(HTTPStatus.BAD_REQUEST, "Missing split, row, or column")
            return

        try:
            audio_value = load_audio_cell(
                self.dataset_path.expanduser().resolve(),
                split,
                int(row_text),
                column,
            )
            self.send_audio_value(audio_value)
        except FileNotFoundError as exc:
            self.send_error(HTTPStatus.NOT_FOUND, str(exc))
        except Exception as exc:
            self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(exc))

    def send_audio_value(self, audio_value) -> None:
        if not isinstance(audio_value, dict):
            self.send_error(HTTPStatus.NOT_FOUND, "Audio cell is not a Hugging Face Audio value")
            return

        audio_bytes = audio_value.get("bytes")
        audio_path_value = audio_value.get("path")

        if audio_bytes:
            path_hint = str(audio_path_value or "audio.wav")
            content_type = mimetypes.guess_type(path_hint)[0] or "audio/wav"
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(audio_bytes)))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            self.wfile.write(audio_bytes)
            return

        if audio_path_value:
            audio_path = Path(str(audio_path_value)).expanduser()
            dataset_root = self.dataset_path.expanduser().resolve()
            if not audio_path.is_absolute():
                audio_path = dataset_root / audio_path
            audio_path = audio_path.resolve()
            if not audio_path.exists() or not audio_path.is_file():
                raise FileNotFoundError(f"Audio file not found: {audio_path}")
            self.send_file_audio(audio_path)
            return

        self.send_error(HTTPStatus.NOT_FOUND, "Audio cell has no bytes or path")

    def send_file_audio(self, audio_path: Path) -> None:
        content_type = mimetypes.guess_type(audio_path.name)[0] or "application/octet-stream"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(audio_path.stat().st_size))
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()
        with audio_path.open("rb") as handle:
            self.copyfile(handle, self.wfile)


def load_huggingface_dataset(path: Path, max_rows: int) -> list[dict]:
    try:
        from datasets import Audio, Dataset, DatasetDict, load_from_disk
    except ImportError as exc:
        raise RuntimeError("Install the `datasets` package on Lightning to read this folder.") from exc

    loaded = load_from_disk(str(path))
    output: list[dict] = []

    if isinstance(loaded, DatasetDict):
        for split_name, split_dataset in loaded.items():
            output.append(dataset_payload(split_name, split_dataset, path, max_rows, Audio))
        return output

    if isinstance(loaded, Dataset):
        output.append(dataset_payload(path.name, loaded, path, max_rows, Audio))
        return output

    raise RuntimeError(f"Unsupported dataset type at {path}")


def load_audio_cell(path: Path, split: str, row_index: int, column: str):
    try:
        from datasets import Audio, Dataset, DatasetDict, load_from_disk
    except ImportError as exc:
        raise RuntimeError("Install the `datasets` package on Lightning to read audio rows.") from exc

    loaded = load_from_disk(str(path))
    if isinstance(loaded, DatasetDict):
        if split not in loaded:
            raise FileNotFoundError(f"Split not found: {split}")
        dataset = loaded[split]
    elif isinstance(loaded, Dataset):
        dataset = loaded
    else:
        raise RuntimeError(f"Unsupported dataset type at {path}")

    if column not in dataset.column_names:
        raise FileNotFoundError(f"Audio column not found: {column}")
    if row_index < 0 or row_index >= len(dataset):
        raise FileNotFoundError(f"Row not found: {row_index}")

    feature = getattr(dataset, "features", {}).get(column)
    if feature and feature.__class__.__name__ == "Audio":
        dataset = dataset.cast_column(column, Audio(decode=False))

    return dataset[row_index][column]


def dataset_payload(name: str, dataset, root: Path, max_rows: int, audio_type) -> dict:
    prepared = prepare_audio_columns(dataset, audio_type)
    audio_columns = get_audio_columns(prepared)
    row_count = len(prepared)
    limit = row_count if max_rows <= 0 else min(max_rows, row_count)
    records = []

    for index in range(limit):
        record = clean_value(prepared[index])
        record["_split"] = name
        record["_row"] = index
        for column in audio_columns:
            record[f"_play_{column}"] = (
                f"./api/audio-row?split={quote(name)}&row={index}&column={quote(column)}"
            )
        records.append(record)

    label = f"{root.name} {name}".strip()
    if max_rows > 0 and row_count > max_rows:
        label = f"{label} preview"

    return {
        "name": label,
        "path": f"{root}/{name}",
        "records": records,
        "rows": row_count,
        "shownRows": limit,
        "final": "final" in str(root).lower() or "clean" in str(root).lower(),
    }


def prepare_audio_columns(dataset, audio_type):
    prepared = dataset
    for column, feature in getattr(dataset, "features", {}).items():
        if feature.__class__.__name__ == "Audio":
            prepared = prepared.cast_column(column, audio_type(decode=False))
    return prepared


def get_audio_columns(dataset) -> list[str]:
    columns = []
    for column, feature in getattr(dataset, "features", {}).items():
        if feature.__class__.__name__ == "Audio":
            columns.append(column)
    return columns


def clean_value(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, bytes):
        return f"[bytes:{len(value)}]"
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        cleaned = {}
        for key, item in value.items():
            if key == "bytes":
                continue
            cleaned[str(key)] = clean_value(item)
        return cleaned
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


def parse_smoke_error(stdout: str) -> str:
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        return ""
    if isinstance(payload, dict):
        return str(payload.get("error") or "")
    return ""


def is_allowed_audio_path(path: Path, dataset_root: Path) -> bool:
    if path.suffix.lower() not in AUDIO_EXTENSIONS:
        return False
    try:
        path.relative_to(dataset_root)
        return True
    except ValueError:
        return False


def main() -> None:
    args = parse_args()
    handler = DatasetViewerHandler
    handler.dataset_path = args.dataset_path
    handler.max_rows = args.max_rows
    handler.site_root = Path(__file__).resolve().parent

    with ThreadingHTTPServer((args.host, args.port), handler) as server:
        print(f"Serving viewer on http://{args.host}:{args.port}/")
        print(f"Reading dataset from {args.dataset_path}")
        server.serve_forever()


if __name__ == "__main__":
    main()

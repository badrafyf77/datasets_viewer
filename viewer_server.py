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
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse


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
        super().do_GET()

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


def dataset_payload(name: str, dataset, root: Path, max_rows: int, audio_type) -> dict:
    prepared = prepare_audio_columns(dataset, audio_type)
    row_count = len(prepared)
    limit = row_count if max_rows <= 0 else min(max_rows, row_count)
    records = []

    for index in range(limit):
        record = clean_value(prepared[index])
        record["_split"] = name
        record["_row"] = index
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

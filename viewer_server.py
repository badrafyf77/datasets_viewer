#!/usr/bin/env python3
"""Serve Moroccan ASR Dataset Studio and read local dataset paths.

Run this on Lightning instead of `python3 -m http.server`:

    python3 viewer_server.py /teamspace/studios/this_studio/darija_clean --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urlparse


AUDIO_EXTENSIONS = {".wav", ".mp3", ".m4a", ".ogg", ".oga", ".aac", ".flac", ".webm"}
TRANSCRIPT_COLUMN_CANDIDATES = [
    "text",
    "sentence",
    "transcript",
    "transcription",
    "expected_transcript",
    "normalized_text",
]
AUDIO_COLUMN_CANDIDATES = [
    "audio",
    "audio_path",
    "file_name",
    "filepath",
    "path",
    "wav",
    "clip",
]
DEFAULT_DATASET_PATH = Path("/teamspace/studios/this_studio/darija_clean")
GENERATION_JOBS: dict[str, dict] = {}
GENERATION_LOCK = threading.Lock()
HF_DATASET_JOBS: dict[str, dict] = {}
HF_DATASET_LOCK = threading.Lock()
CLEANER_JOBS: dict[str, dict] = {}
CLEANER_LOCK = threading.Lock()
WHISPER_MODELS: dict[str, object] = {}
WHISPER_LOCK = threading.Lock()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Serve Moroccan ASR Dataset Studio with local APIs.")
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
    dataset_cache: dict[str, dict] = {}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(self.site_root), **kwargs)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/datasets":
            payload = self.load_datasets(parsed.query)
            if payload is not None:
                self.send_json(payload)
            return
        if parsed.path == "/api/server-info":
            self.send_json(
                {
                    "ok": True,
                    "server": "viewer_server.py",
                    "features": [
                        "datasets",
                        "audio",
                        "synthetic-test",
                        "generation-jobs",
                        "hf-dataset-import",
                        "hf-dataset-merge",
                        "hf-dataset-push",
                        "dataset-cleaner",
                    ],
                    "default_dataset_path": str(self.dataset_path),
                    "default_max_rows": self.max_rows,
                }
            )
            return
        if parsed.path == "/api/cleaner/status":
            self.send_dataset_cleaner_status(parsed.query)
            return
        if parsed.path == "/api/generation/status":
            self.send_generation_status(parsed.query)
            return
        if parsed.path == "/api/hf/status":
            self.send_hf_dataset_status(parsed.query)
            return
        if parsed.path == "/api/hf/columns":
            self.send_hf_dataset_columns(parsed.query)
            return
        if parsed.path == "/api/audio":
            self.send_audio(parsed.query)
            return
        if parsed.path == "/api/audio-row":
            self.send_audio_row(parsed.query)
            return
        if parsed.path == "/api/synthetic-test":
            self.run_synthetic_test()
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
        if parsed.path == "/api/generation/start":
            self.start_generation()
            return
        if parsed.path == "/api/hf/merge/start":
            self.start_hf_dataset_merge()
            return
        if parsed.path == "/api/hf/import/start":
            self.start_hf_dataset_import()
            return
        if parsed.path == "/api/hf/push/start":
            self.start_hf_dataset_push()
            return
        if parsed.path == "/api/cleaner/bad-samples/start":
            self.start_dataset_cleaner()
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
        body = json.dumps({"ok": False, "error": message}, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def load_datasets(self, query: str = "") -> dict | None:
        params = parse_qs(query)
        path_value = params.get("path", [""])[0].strip()
        max_rows_value = params.get("max_rows", [""])[0].strip()
        path = Path(unquote(path_value)).expanduser() if path_value else self.dataset_path.expanduser()
        path = path.resolve()
        max_rows = self.max_rows
        if max_rows_value:
            try:
                max_rows = max(0, int(max_rows_value))
            except ValueError:
                self.send_error_json(HTTPStatus.BAD_REQUEST, "max_rows must be a number.")
                return None

        cache_key = f"{path}::{max_rows}"
        if cache_key in self.__class__.dataset_cache:
            self.__class__.dataset_path = path
            self.__class__.max_rows = max_rows
            return self.__class__.dataset_cache[cache_key]

        try:
            datasets = load_huggingface_dataset(path, max_rows)
        except Exception as exc:
            self.send_error_json(HTTPStatus.INTERNAL_SERVER_ERROR, str(exc))
            return None

        payload = {"path": str(path), "datasets": datasets}
        self.__class__.dataset_path = path
        self.__class__.max_rows = max_rows
        self.__class__.dataset_cache[cache_key] = payload
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

        params = {}
        if self.headers.get("Content-Length"):
            try:
                params = self.read_json_body()
            except ValueError as exc:
                self.send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
                return

        command = [sys.executable, str(script_path)]
        reference_audio = str(params.get("reference_audio") or "").strip()
        reference_text = str(params.get("reference_text") or "").strip()
        if reference_audio:
            command.extend(["--reference-audio", reference_audio])
        if reference_text:
            command.extend(["--reference-text", reference_text])

        try:
            result = subprocess.run(
                command,
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

    def start_generation(self) -> None:
        try:
            params = self.read_json_body()
        except ValueError as exc:
            self.send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
            return

        job_id = uuid.uuid4().hex[:12]
        job = {
            "id": job_id,
            "ok": True,
            "status": "queued",
            "stage": "Queued",
            "percent": 0,
            "message": "Waiting to start generation.",
            "error": "",
            "log_tail": "",
            "started_at": time.time(),
            "updated_at": time.time(),
            "params": params,
        }
        with GENERATION_LOCK:
            GENERATION_JOBS[job_id] = job

        thread = threading.Thread(
            target=run_generation_job,
            args=(job_id, params, self.site_root),
            daemon=True,
        )
        thread.start()
        self.send_json({"ok": True, "job_id": job_id, "job": public_job(job)})

    def send_generation_status(self, query: str) -> None:
        params = parse_qs(query)
        job_id = params.get("id", [""])[0]
        if not job_id:
            self.send_error_json(HTTPStatus.BAD_REQUEST, "Missing generation job id.")
            return
        with GENERATION_LOCK:
            job = GENERATION_JOBS.get(job_id)
            if job:
                job = dict(job)
        if not job:
            self.send_error_json(HTTPStatus.NOT_FOUND, "Generation job not found.")
            return
        self.send_json({"ok": True, "job": public_job(job)})

    def start_hf_dataset_merge(self) -> None:
        try:
            params = self.read_json_body()
        except ValueError as exc:
            self.send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
            return

        job_id = uuid.uuid4().hex[:12]
        job = {
            "id": job_id,
            "ok": True,
            "status": "queued",
            "stage": "Queued",
            "percent": 0,
            "message": "Waiting to merge Hugging Face datasets.",
            "error": "",
            "log_tail": "",
            "started_at": time.time(),
            "updated_at": time.time(),
            "params": params,
            "result": {},
        }
        with HF_DATASET_LOCK:
            HF_DATASET_JOBS[job_id] = job

        thread = threading.Thread(
            target=run_hf_dataset_merge_job,
            args=(job_id, params, self.site_root),
            daemon=True,
        )
        thread.start()
        self.send_json({"ok": True, "job_id": job_id, "job": public_job(job)})

    def start_hf_dataset_import(self) -> None:
        try:
            params = self.read_json_body()
        except ValueError as exc:
            self.send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
            return

        job_id = uuid.uuid4().hex[:12]
        job = {
            "id": job_id,
            "ok": True,
            "status": "queued",
            "stage": "Queued",
            "percent": 0,
            "message": "Waiting to import a Hugging Face dataset.",
            "error": "",
            "log_tail": "",
            "started_at": time.time(),
            "updated_at": time.time(),
            "params": params,
            "result": {},
        }
        with HF_DATASET_LOCK:
            HF_DATASET_JOBS[job_id] = job

        thread = threading.Thread(
            target=run_hf_dataset_import_job,
            args=(job_id, params, self.site_root),
            daemon=True,
        )
        thread.start()
        self.send_json({"ok": True, "job_id": job_id, "job": public_job(job)})

    def start_hf_dataset_push(self) -> None:
        try:
            params = self.read_json_body()
        except ValueError as exc:
            self.send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
            return

        job_id = uuid.uuid4().hex[:12]
        job = {
            "id": job_id,
            "ok": True,
            "status": "queued",
            "stage": "Queued",
            "percent": 0,
            "message": "Waiting to push the dataset to Hugging Face.",
            "error": "",
            "log_tail": "",
            "started_at": time.time(),
            "updated_at": time.time(),
            "params": params,
            "result": {},
        }
        with HF_DATASET_LOCK:
            HF_DATASET_JOBS[job_id] = job

        thread = threading.Thread(
            target=run_hf_dataset_push_job,
            args=(job_id, params, self.site_root),
            daemon=True,
        )
        thread.start()
        self.send_json({"ok": True, "job_id": job_id, "job": public_job(job)})

    def start_dataset_cleaner(self) -> None:
        try:
            params = self.read_json_body()
        except ValueError as exc:
            self.send_error_json(HTTPStatus.BAD_REQUEST, str(exc))
            return

        job_id = uuid.uuid4().hex[:12]
        job = {
            "id": job_id,
            "ok": True,
            "status": "queued",
            "stage": "Queued",
            "percent": 0,
            "message": "Waiting to remove bad samples.",
            "error": "",
            "log_tail": "",
            "started_at": time.time(),
            "updated_at": time.time(),
            "params": params,
            "result": {},
        }
        with CLEANER_LOCK:
            CLEANER_JOBS[job_id] = job

        thread = threading.Thread(
            target=run_dataset_cleaner_job,
            args=(job_id, params, self.site_root),
            daemon=True,
        )
        thread.start()
        self.send_json({"ok": True, "job_id": job_id, "job": public_job(job)})

    def send_dataset_cleaner_status(self, query: str) -> None:
        params = parse_qs(query)
        job_id = params.get("id", [""])[0]
        if not job_id:
            self.send_error_json(HTTPStatus.BAD_REQUEST, "Missing dataset cleaner job id.")
            return
        with CLEANER_LOCK:
            job = CLEANER_JOBS.get(job_id)
            if job:
                job = dict(job)
        if not job:
            self.send_error_json(HTTPStatus.NOT_FOUND, "Dataset cleaner job not found.")
            return
        self.send_json({"ok": True, "job": public_job(job)})

    def send_hf_dataset_status(self, query: str) -> None:
        params = parse_qs(query)
        job_id = params.get("id", [""])[0]
        if not job_id:
            self.send_error_json(HTTPStatus.BAD_REQUEST, "Missing Hugging Face dataset job id.")
            return
        with HF_DATASET_LOCK:
            job = HF_DATASET_JOBS.get(job_id)
            if job:
                job = dict(job)
        if not job:
            self.send_error_json(HTTPStatus.NOT_FOUND, "Hugging Face dataset job not found.")
            return
        self.send_json({"ok": True, "job": public_job(job)})

    def send_hf_dataset_columns(self, query: str) -> None:
        params = parse_qs(query)
        path_value = params.get("path", [""])[0].strip()
        if not path_value:
            self.send_error_json(HTTPStatus.BAD_REQUEST, "Missing Hugging Face dataset path.")
            return

        pipeline_dir = self.site_root / "synthetic_cs_dataset"
        try:
            path = resolve_hf_dataset_path(unquote(path_value), self.site_root, pipeline_dir, must_exist=True)
            if not path.exists():
                raise RuntimeError(f"Hugging Face dataset path was not found: {path}")
            split_map = load_hf_dataset_splits(path)
            self.send_json({"ok": True, "path": str(path), **hf_columns_payload(split_map)})
        except Exception as exc:
            self.send_error_json(HTTPStatus.INTERNAL_SERVER_ERROR, str(exc))

    def read_json_body(self) -> dict:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise ValueError("Invalid request body length.") from exc
        if length <= 0:
            return {}
        raw = self.rfile.read(length).decode("utf-8")
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError("Request body must be valid JSON.") from exc
        if not isinstance(value, dict):
            raise ValueError("Request body must be a JSON object.")
        return value

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


def public_job(job: dict) -> dict:
    return {
        "id": job.get("id"),
        "status": job.get("status"),
        "stage": job.get("stage"),
        "percent": job.get("percent", 0),
        "message": job.get("message", ""),
        "error": job.get("error", ""),
        "log_tail": job.get("log_tail", ""),
        "started_at": job.get("started_at"),
        "updated_at": job.get("updated_at"),
        "params": job.get("params", {}),
        "result": job.get("result", {}),
    }


def update_generation_job(job_id: str, **updates) -> None:
    with GENERATION_LOCK:
        job = GENERATION_JOBS.get(job_id)
        if not job:
            return
        job.update(updates)
        job["updated_at"] = time.time()


def update_hf_dataset_job(job_id: str, **updates) -> None:
    with HF_DATASET_LOCK:
        job = HF_DATASET_JOBS.get(job_id)
        if not job:
            return
        job.update(updates)
        job["updated_at"] = time.time()


def update_cleaner_job(job_id: str, **updates) -> None:
    with CLEANER_LOCK:
        job = CLEANER_JOBS.get(job_id)
        if not job:
            return
        job.update(updates)
        job["updated_at"] = time.time()


def collect_dataset_path_values(value) -> list[str]:
    if isinstance(value, str):
        candidates = value.replace(",", "\n").splitlines()
    elif isinstance(value, list):
        candidates = value
    else:
        candidates = []

    paths = []
    for candidate in candidates:
        path = str(candidate or "").strip().strip("\"'")
        if path:
            paths.append(path)
    return paths


def collect_column_values(value) -> list[str]:
    if isinstance(value, str):
        candidates = value.replace(",", "\n").splitlines()
    elif isinstance(value, list):
        candidates = value
    else:
        candidates = []

    columns = []
    seen = set()
    for candidate in candidates:
        column = str(candidate or "").strip().strip("\"'")
        if column and column not in seen:
            seen.add(column)
            columns.append(column)
    return columns


def normalize_hf_dataset_repo_id(value: str) -> str:
    text = str(value or "").strip().strip("\"'")
    if text.startswith(("http://", "https://")):
        parsed = urlparse(text)
        if not parsed.netloc.endswith("huggingface.co"):
            raise RuntimeError("Hugging Face dataset URL must be on huggingface.co.")
        parts = [part for part in parsed.path.strip("/").split("/") if part]
        if parts and parts[0] == "datasets":
            parts = parts[1:]
        if len(parts) >= 2:
            text = "/".join(parts[:2])
        elif parts:
            text = parts[0]
        else:
            text = ""

    if text.startswith("datasets/"):
        text = text[len("datasets/") :]
    text = text.strip("/")
    if not text:
        raise RuntimeError("Enter a Hugging Face dataset repo id, for example `mozilla-foundation/common_voice_17_0`.")
    if re.search(r"\s", text):
        raise RuntimeError("Hugging Face dataset repo id cannot contain spaces.")
    if not re.match(r"^[A-Za-z0-9][A-Za-z0-9._-]*(/[A-Za-z0-9][A-Za-z0-9._-]*)?$", text):
        raise RuntimeError("Enter a valid Hugging Face dataset repo id like `owner/name`.")
    return text


def safe_slug(*parts: str) -> str:
    raw = "-".join(str(part or "").strip() for part in parts if str(part or "").strip())
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", raw.replace("/", "-")).strip("-._")
    return slug[:120] or "dataset"


def unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    for index in range(2, 1000):
        candidate = path.with_name(f"{path.name}-{index}")
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"Could not choose a unique output path near {path}")


def default_hf_import_path(site_root: Path, repo_id: str, config_name: str, split: str, revision: str) -> Path:
    pipeline_dir = site_root / "synthetic_cs_dataset"
    slug = safe_slug(repo_id, config_name, split, revision)
    return (pipeline_dir / "data" / "hf_imports" / slug).resolve()


def resolve_hf_dataset_path(path_value: str, site_root: Path, pipeline_dir: Path, must_exist: bool) -> Path:
    raw = Path(str(path_value or "").strip()).expanduser()
    if raw.is_absolute():
        return raw.resolve()

    candidates = [site_root / raw, pipeline_dir / raw]
    if must_exist:
        for candidate in candidates:
            if candidate.exists():
                return candidate.resolve()
    return candidates[0].resolve()


def load_hf_dataset_splits(path: Path) -> dict[str, object]:
    try:
        from datasets import Dataset, DatasetDict, load_from_disk
    except ImportError as exc:
        raise RuntimeError("Install the `datasets` package before merging Hugging Face datasets.") from exc

    loaded = load_from_disk(str(path))
    if isinstance(loaded, DatasetDict):
        return dict(loaded.items())
    if isinstance(loaded, Dataset):
        return {"train": loaded}
    raise RuntimeError(f"Unsupported Hugging Face dataset type at {path}")


def hf_columns_payload(split_map: dict[str, object]) -> dict:
    columns_by_split = {
        split: list(getattr(dataset, "column_names", []) or [])
        for split, dataset in split_map.items()
    }
    row_counts = {split: len(dataset) for split, dataset in split_map.items()}
    if not columns_by_split:
        return {"columns": [], "columns_by_split": {}, "rows_by_split": {}}

    ordered_common = list(next(iter(columns_by_split.values())))
    for columns in list(columns_by_split.values())[1:]:
        allowed = set(columns)
        ordered_common = [column for column in ordered_common if column in allowed]

    return {
        "columns": ordered_common,
        "columns_by_split": columns_by_split,
        "rows_by_split": row_counts,
    }


def filter_dataset_columns(loaded, selected_columns: list[str]):
    if not selected_columns:
        return loaded

    try:
        from datasets import Dataset, DatasetDict
    except ImportError as exc:
        raise RuntimeError("Install the `datasets` package before filtering columns.") from exc

    if isinstance(loaded, Dataset):
        missing = [column for column in selected_columns if column not in loaded.column_names]
        if missing:
            raise RuntimeError(f"Selected column(s) not found: {', '.join(missing)}")
        remove_columns = [column for column in loaded.column_names if column not in selected_columns]
        return loaded.remove_columns(remove_columns) if remove_columns else loaded

    if isinstance(loaded, DatasetDict):
        filtered = DatasetDict()
        missing_by_split = {}
        for split, dataset in loaded.items():
            missing = [column for column in selected_columns if column not in dataset.column_names]
            if missing:
                missing_by_split[split] = missing
                continue
            remove_columns = [column for column in dataset.column_names if column not in selected_columns]
            filtered[split] = dataset.remove_columns(remove_columns) if remove_columns else dataset
        if missing_by_split:
            details = "; ".join(f"{split}: {', '.join(columns)}" for split, columns in missing_by_split.items())
            raise RuntimeError(f"Selected column(s) are missing from some splits: {details}")
        return filtered

    raise RuntimeError("Unsupported Hugging Face dataset type.")


def loaded_columns_by_split(loaded) -> dict[str, list[str]]:
    try:
        from datasets import Dataset, DatasetDict
    except ImportError as exc:
        raise RuntimeError("Install the `datasets` package before reading columns.") from exc

    if isinstance(loaded, Dataset):
        return {"train": list(loaded.column_names)}
    if isinstance(loaded, DatasetDict):
        return {split: list(dataset.column_names) for split, dataset in loaded.items()}
    return {}


def read_simple_env_token(env_path: Path) -> str:
    if not env_path.exists():
        return ""
    with env_path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            if key.strip() in {"HF_TOKEN", "HUGGINGFACE_TOKEN"}:
                return value.strip().strip("\"'")
    return ""


def get_hf_token(site_root: Path) -> str:
    return (
        os.environ.get("HF_TOKEN")
        or os.environ.get("HUGGINGFACE_TOKEN")
        or read_simple_env_token(site_root / "synthetic_cs_dataset" / ".env")
        or read_simple_env_token(site_root / ".env")
    )


def guarded_remove_output(path: Path, site_root: Path, pipeline_dir: Path) -> None:
    resolved = path.resolve()
    protected = {
        Path("/").resolve(),
        Path.home().resolve(),
        site_root.resolve(),
        pipeline_dir.resolve(),
    }
    if resolved in protected or len(resolved.parts) < 3:
        raise RuntimeError(f"Refusing to overwrite protected path: {resolved}")
    if resolved.is_dir():
        shutil.rmtree(resolved)
    elif resolved.exists():
        resolved.unlink()


def dataset_shape_payload(loaded, single_split_name: str = "train") -> dict:
    try:
        from datasets import Dataset, DatasetDict
    except ImportError as exc:
        raise RuntimeError("Install the `datasets` package before reading Hugging Face dataset details.") from exc

    if isinstance(loaded, Dataset):
        split_name = single_split_name or "train"
        return {
            "rows_by_split": {split_name: len(loaded)},
            "columns_by_split": {split_name: list(loaded.column_names)},
        }
    if isinstance(loaded, DatasetDict):
        return {
            "rows_by_split": {split: len(dataset) for split, dataset in loaded.items()},
            "columns_by_split": {split: list(dataset.column_names) for split, dataset in loaded.items()},
        }
    raise RuntimeError("Unsupported Hugging Face dataset type.")


def load_dataset_from_hub(
    repo_id: str,
    config_name: str,
    split: str,
    revision: str,
    token: str,
    trust_remote_code: bool,
):
    try:
        from datasets import Dataset, DatasetDict, load_dataset
    except ImportError as exc:
        raise RuntimeError("Install the `datasets` package before importing datasets from Hugging Face.") from exc

    kwargs = {}
    if config_name:
        kwargs["name"] = config_name
    if split:
        kwargs["split"] = split
    if revision:
        kwargs["revision"] = revision
    if token:
        kwargs["token"] = token
    if trust_remote_code:
        kwargs["trust_remote_code"] = True

    try:
        loaded = load_dataset(repo_id, **kwargs)
    except TypeError as exc:
        fallback_kwargs = dict(kwargs)
        changed = False
        text = str(exc)
        if "token" in text and "token" in fallback_kwargs:
            fallback_kwargs.pop("token", None)
            fallback_kwargs["use_auth_token"] = token
            changed = True
        if "trust_remote_code" in text and "trust_remote_code" in fallback_kwargs:
            fallback_kwargs.pop("trust_remote_code", None)
            changed = True
        if not changed:
            raise
        loaded = load_dataset(repo_id, **fallback_kwargs)

    if not isinstance(loaded, (Dataset, DatasetDict)):
        raise RuntimeError(f"Unsupported dataset type returned by Hugging Face for {repo_id}")
    return loaded


def run_hf_dataset_import_job(job_id: str, params: dict, site_root: Path) -> None:
    pipeline_dir = site_root / "synthetic_cs_dataset"
    output_path = None
    try:
        repo_id = normalize_hf_dataset_repo_id(str(params.get("repo_id") or ""))
        config_name = str(params.get("config_name") or "").strip().strip("\"'")
        split = str(params.get("split") or "").strip().strip("\"'")
        revision = str(params.get("revision") or "").strip().strip("\"'")
        output_value = str(params.get("output_path") or "").strip().strip("\"'")
        overwrite = bool(params.get("overwrite"))
        trust_remote_code = bool(params.get("trust_remote_code"))

        if output_value:
            output_path = resolve_hf_dataset_path(output_value, site_root, pipeline_dir, must_exist=False)
            if output_path.exists() and not overwrite:
                raise RuntimeError(f"Output path already exists: {output_path}. Enable overwrite or choose another folder.")
        else:
            output_path = unique_path(default_hf_import_path(site_root, repo_id, config_name, split, revision))

        update_hf_dataset_job(
            job_id,
            status="running",
            stage="Preparing",
            percent=4,
            message=f"Preparing to import {repo_id}.",
            result={"repo_id": repo_id, "output_path": str(output_path)},
        )

        if output_path.exists() and overwrite:
            update_hf_dataset_job(
                job_id,
                stage="Preparing",
                percent=7,
                message=f"Removing existing local folder: {output_path}",
            )
            guarded_remove_output(output_path, site_root, pipeline_dir)

        token = get_hf_token(site_root)
        load_label = repo_id
        if config_name:
            load_label += f" / {config_name}"
        if split:
            load_label += f" / {split}"

        update_hf_dataset_job(
            job_id,
            stage="Downloading",
            percent=12,
            message=f"Loading {load_label} from Hugging Face. Large datasets can take a while.",
        )
        loaded = load_dataset_from_hub(repo_id, config_name, split, revision, token, trust_remote_code)
        shape = dataset_shape_payload(loaded, split or "train")

        update_hf_dataset_job(
            job_id,
            stage="Saving",
            percent=78,
            message=f"Saving imported dataset to {output_path}",
            result={
                "repo_id": repo_id,
                "config_name": config_name,
                "split": split,
                "revision": revision,
                "output_path": str(output_path),
                **shape,
            },
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        loaded.save_to_disk(str(output_path))
        DatasetViewerHandler.dataset_cache.clear()

        total_rows = sum(shape["rows_by_split"].values())
        update_hf_dataset_job(
            job_id,
            status="completed",
            stage="Completed",
            percent=100,
            message=f"Imported {total_rows} row(s) from {repo_id} into {output_path}",
            result={
                "repo_id": repo_id,
                "config_name": config_name,
                "split": split,
                "revision": revision,
                "output_path": str(output_path),
                **shape,
            },
        )
    except Exception as exc:
        hint = ""
        text = str(exc)
        if "401" in text or "Unauthorized" in text or "token" in text.lower():
            hint = (
                "\nFor private or gated datasets, set HF_TOKEN or HUGGINGFACE_TOKEN in the environment running "
                "viewer_server.py, add HF_TOKEN to synthetic_cs_dataset/.env, or run `huggingface-cli login` there."
            )
        update_hf_dataset_job(
            job_id,
            status="failed",
            stage="Failed",
            percent=100,
            message="Hugging Face dataset import failed.",
            error=f"{text}{hint}",
            log_tail=f"{text}{hint}",
            result={"output_path": str(output_path) if output_path else ""},
        )


def run_hf_dataset_merge_job(job_id: str, params: dict, site_root: Path) -> None:
    pipeline_dir = site_root / "synthetic_cs_dataset"
    try:
        path_values = collect_dataset_path_values(params.get("dataset_paths"))
        if len(path_values) < 2:
            raise RuntimeError("Enter at least two Hugging Face dataset paths to merge.")

        output_value = str(params.get("output_path") or "").strip()
        if not output_value:
            raise RuntimeError("Choose an output folder for the merged Hugging Face dataset.")
        output_path = resolve_hf_dataset_path(output_value, site_root, pipeline_dir, must_exist=False)
        overwrite = bool(params.get("overwrite"))

        update_hf_dataset_job(
            job_id,
            status="running",
            stage="Loading",
            percent=5,
            message=f"Loading {len(path_values)} Hugging Face dataset folder(s).",
        )

        loaded_sources: list[tuple[Path, dict[str, object]]] = []
        source_rows: dict[str, dict[str, int]] = {}
        for index, path_value in enumerate(path_values):
            path = resolve_hf_dataset_path(path_value, site_root, pipeline_dir, must_exist=True)
            if not path.exists():
                raise RuntimeError(f"Hugging Face dataset path was not found: {path}")
            split_map = load_hf_dataset_splits(path)
            loaded_sources.append((path, split_map))
            source_rows[str(path)] = {split: len(dataset) for split, dataset in split_map.items()}
            percent = 5 + round(((index + 1) / len(path_values)) * 35, 1)
            update_hf_dataset_job(
                job_id,
                percent=percent,
                message=f"Loaded {path.name}: {', '.join(f'{split}={len(dataset)}' for split, dataset in split_map.items())}",
                result={"source_rows": source_rows},
            )

        try:
            from datasets import DatasetDict, concatenate_datasets
        except ImportError as exc:
            raise RuntimeError("Install the `datasets` package before merging Hugging Face datasets.") from exc

        split_names = sorted({split for _, split_map in loaded_sources for split in split_map})
        merged = DatasetDict()
        merged_rows: dict[str, int] = {}
        merged_columns: dict[str, list[str]] = {}

        for index, split_name in enumerate(split_names):
            update_hf_dataset_job(
                job_id,
                stage="Merging",
                percent=45 + round((index / max(len(split_names), 1)) * 30, 1),
                message=f"Merging split: {split_name}",
            )
            split_datasets = [split_map[split_name] for _, split_map in loaded_sources if split_name in split_map]
            try:
                merged_split = split_datasets[0] if len(split_datasets) == 1 else concatenate_datasets(split_datasets)
            except Exception as exc:
                raise RuntimeError(
                    f"Could not merge split `{split_name}`. Make sure every hf_dataset has the same columns and features. {exc}"
                ) from exc
            merged[split_name] = merged_split
            merged_rows[split_name] = len(merged_split)
            merged_columns[split_name] = list(merged_split.column_names)

        if len(merged) == 0:
            raise RuntimeError("No splits were found in the provided Hugging Face datasets.")

        update_hf_dataset_job(
            job_id,
            stage="Saving",
            percent=82,
            message=f"Saving merged dataset to {output_path}",
            result={
                "output_path": str(output_path),
                "source_rows": source_rows,
                "rows_by_split": merged_rows,
                "columns_by_split": merged_columns,
            },
        )
        if output_path.exists():
            if not overwrite:
                raise RuntimeError(f"Output path already exists: {output_path}. Enable overwrite or choose a new folder.")
            guarded_remove_output(output_path, site_root, pipeline_dir)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        merged.save_to_disk(str(output_path))
        DatasetViewerHandler.dataset_cache.clear()

        total_rows = sum(merged_rows.values())
        update_hf_dataset_job(
            job_id,
            status="completed",
            stage="Completed",
            percent=100,
            message=f"Merged {total_rows} row(s) into {output_path}",
            result={
                "output_path": str(output_path),
                "source_rows": source_rows,
                "rows_by_split": merged_rows,
                "columns_by_split": merged_columns,
            },
        )
    except Exception as exc:
        update_hf_dataset_job(
            job_id,
            status="failed",
            stage="Failed",
            percent=100,
            message="Hugging Face dataset merge failed.",
            error=str(exc),
            log_tail=str(exc),
        )


def run_hf_dataset_push_job(job_id: str, params: dict, site_root: Path) -> None:
    pipeline_dir = site_root / "synthetic_cs_dataset"
    try:
        dataset_value = str(params.get("dataset_path") or "").strip()
        repo_id = str(params.get("repo_id") or "").strip()
        selected_columns = collect_column_values(params.get("columns"))
        private = bool(params.get("private"))
        if not dataset_value:
            raise RuntimeError("Missing Hugging Face dataset path.")
        if not repo_id:
            raise RuntimeError("Enter a Hugging Face dataset repo id, for example `username/darija-asr`.")

        dataset_path = resolve_hf_dataset_path(dataset_value, site_root, pipeline_dir, must_exist=True)
        if not dataset_path.exists():
            raise RuntimeError(f"Hugging Face dataset path was not found: {dataset_path}")

        update_hf_dataset_job(
            job_id,
            status="running",
            stage="Loading",
            percent=10,
            message=f"Loading merged dataset from {dataset_path}",
        )
        try:
            from datasets import Dataset, DatasetDict, load_from_disk
        except ImportError as exc:
            raise RuntimeError("Install the `datasets` package before pushing to Hugging Face.") from exc

        loaded = load_from_disk(str(dataset_path))
        if not isinstance(loaded, (Dataset, DatasetDict)):
            raise RuntimeError(f"Unsupported Hugging Face dataset type at {dataset_path}")

        if selected_columns:
            update_hf_dataset_job(
                job_id,
                stage="Filtering columns",
                percent=22,
                message=f"Keeping {len(selected_columns)} selected column(s): {', '.join(selected_columns)}",
            )
            loaded = filter_dataset_columns(loaded, selected_columns)

        row_count = len(loaded) if isinstance(loaded, Dataset) else sum(len(dataset) for dataset in loaded.values())
        token = get_hf_token(site_root)
        kwargs = {"repo_id": repo_id, "private": private}
        if token:
            kwargs["token"] = token

        update_hf_dataset_job(
            job_id,
            stage="Pushing",
            percent=35,
            message=f"Pushing {row_count} row(s) to {repo_id}.",
            result={
                "dataset_path": str(dataset_path),
                "repo_id": repo_id,
                "columns_by_split": loaded_columns_by_split(loaded),
                "selected_columns": selected_columns,
            },
        )
        loaded.push_to_hub(**kwargs)
        repo_url = f"https://huggingface.co/datasets/{repo_id}"
        update_hf_dataset_job(
            job_id,
            status="completed",
            stage="Completed",
            percent=100,
            message=f"Pushed dataset to {repo_url}",
            result={
                "dataset_path": str(dataset_path),
                "repo_id": repo_id,
                "repo_url": repo_url,
                "rows": row_count,
                "columns_by_split": loaded_columns_by_split(loaded),
                "selected_columns": selected_columns,
            },
        )
    except Exception as exc:
        hint = ""
        text = str(exc)
        if "401" in text or "Unauthorized" in text or "token" in text.lower():
            hint = (
                "\nSet HF_TOKEN or HUGGINGFACE_TOKEN in the environment running viewer_server.py, "
                "add HF_TOKEN to synthetic_cs_dataset/.env, or run `huggingface-cli login` there."
            )
        update_hf_dataset_job(
            job_id,
            status="failed",
            stage="Failed",
            percent=100,
            message="Hugging Face push failed.",
            error=f"{text}{hint}",
            log_tail=f"{text}{hint}",
        )


def parse_cleaner_float(params: dict, key: str, default: float) -> float:
    value = params.get(key)
    if value in (None, ""):
        return float(default)
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"{key} must be a number.") from exc


def cleaner_bool(params: dict, key: str, default: bool) -> bool:
    if key not in params:
        return default
    return bool(params.get(key))


def match_column(columns: list[str], value: str) -> str:
    requested = str(value or "").strip()
    if not requested:
        return ""
    if requested in columns:
        return requested
    requested_lower = requested.lower()
    for column in columns:
        if column.lower() == requested_lower:
            return column
    return ""


def column_feature_is_audio(dataset, column: str) -> bool:
    feature = getattr(dataset, "features", {}).get(column)
    return bool(feature and feature.__class__.__name__ == "Audio")


def infer_common_cleaner_column(
    split_map: dict[str, object],
    requested: str,
    candidates: list[str],
    label: str,
    audio: bool = False,
) -> str:
    if not split_map:
        raise RuntimeError("The dataset has no splits.")

    first_dataset = next(iter(split_map.values()))
    first_columns = list(getattr(first_dataset, "column_names", []) or [])
    if not first_columns:
        raise RuntimeError("The dataset has no columns.")

    if requested:
        column = match_column(first_columns, requested)
        if not column:
            raise RuntimeError(f"{label} column was not found: {requested}")
    else:
        column = ""
        if audio:
            for candidate in first_columns:
                if column_feature_is_audio(first_dataset, candidate):
                    column = candidate
                    break
        if not column:
            for candidate in candidates:
                column = match_column(first_columns, candidate)
                if column:
                    break
        if not column and audio:
            for candidate in first_columns:
                lower = candidate.lower()
                if any(token in lower for token in ("audio", "sound", "recording", "voice", "clip", "file")):
                    column = candidate
                    break
        if not column:
            raise RuntimeError(
                f"Could not infer the {label} column. Enter the column name explicitly."
            )

    missing = [
        split
        for split, dataset in split_map.items()
        if column not in list(getattr(dataset, "column_names", []) or [])
    ]
    if missing:
        raise RuntimeError(f"{label} column `{column}` is missing from split(s): {', '.join(missing)}")
    return column


def prepare_cleaner_audio_column(dataset, audio_column: str, audio_type):
    if column_feature_is_audio(dataset, audio_column):
        return dataset.cast_column(audio_column, audio_type(decode=False))
    return dataset


def normalized_asr_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def resolve_existing_audio_path(path_value: str, dataset_path: Path, site_root: Path) -> Path:
    raw_text = str(path_value or "").strip().strip("\"'")
    if not raw_text:
        return Path()

    raw = Path(raw_text).expanduser()
    if raw.is_absolute():
        return raw.resolve()

    candidates = [
        (dataset_path / raw).resolve(),
        (dataset_path.parent / raw).resolve(),
        (dataset_path.parent / "audio" / raw.name).resolve(),
        (site_root / raw).resolve(),
        (site_root / "synthetic_cs_dataset" / raw).resolve(),
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0]


def materialize_cleaner_audio(value, dataset_path: Path, site_root: Path) -> tuple[Path | None, Path | None]:
    path_value = ""
    bytes_value = None
    array_value = None
    sampling_rate = None

    if isinstance(value, dict):
        path_value = str(value.get("path") or value.get("file_name") or value.get("audio") or "").strip()
        bytes_value = value.get("bytes")
        array_value = value.get("array")
        sampling_rate = value.get("sampling_rate")
    elif value is not None:
        path_value = str(value).strip()

    if path_value:
        path = resolve_existing_audio_path(path_value, dataset_path, site_root)
        if path.exists():
            return path, None

    suffix = Path(path_value).suffix if path_value else ".wav"
    if not suffix:
        suffix = ".wav"

    if bytes_value:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as handle:
            handle.write(bytes_value)
            return Path(handle.name), Path(handle.name)

    if array_value is not None and sampling_rate:
        try:
            import soundfile as sf
        except ImportError as exc:
            raise RuntimeError("Install soundfile before cleaning decoded audio arrays.") from exc
        with tempfile.NamedTemporaryFile(delete=False, suffix=".wav") as handle:
            temp_path = Path(handle.name)
        sf.write(str(temp_path), array_value, int(sampling_rate))
        return temp_path, temp_path

    if path_value:
        return resolve_existing_audio_path(path_value, dataset_path, site_root), None
    return None, None


def cleaner_audio_duration_seconds(path: Path) -> float:
    try:
        import librosa

        try:
            return float(librosa.get_duration(path=str(path)))
        except TypeError:
            return float(librosa.get_duration(filename=str(path)))
    except Exception:
        try:
            import soundfile as sf
        except ImportError as exc:
            raise RuntimeError("Install librosa or soundfile before checking audio duration.") from exc
        info = sf.info(str(path))
        if not info.samplerate:
            return 0.0
        return float(info.frames / info.samplerate)


def cleaner_jiwer_cer():
    try:
        import jiwer
    except ImportError as exc:
        raise RuntimeError("Install cleaner dependencies first: pip install openai-whisper jiwer") from exc
    return jiwer.cer


def get_whisper_model(model_name: str):
    try:
        import whisper
    except ImportError as exc:
        raise RuntimeError("Install cleaner dependencies first: pip install openai-whisper jiwer") from exc

    name = str(model_name or "large-v3-turbo").strip() or "large-v3-turbo"
    with WHISPER_LOCK:
        model = WHISPER_MODELS.get(name)
        if model is None:
            model = whisper.load_model(name)
            WHISPER_MODELS[name] = model
    return model


def bad_sample_identity(row: dict) -> str:
    for column in ("id", "file_name", "audio", "audio_path", "path"):
        value = row.get(column)
        if value:
            if isinstance(value, dict):
                value = value.get("path") or value.get("file_name") or ""
            return str(value)
    return ""


def evaluate_cleaner_row(
    row: dict,
    split: str,
    index: int,
    dataset_path: Path,
    site_root: Path,
    transcript_column: str,
    audio_column: str,
    use_cps: bool,
    min_cps: float,
    max_cps: float,
    use_whisper: bool,
    whisper_model,
    cer_fn,
    cer_threshold: float,
    language: str,
) -> tuple[bool, dict]:
    reasons: list[str] = []
    details: dict = {
        "split": split,
        "row": index,
        "id": bad_sample_identity(row),
        "reason": "",
    }

    expected_text = normalized_asr_text(row.get(transcript_column, ""))
    details["expected"] = expected_text
    if not expected_text:
        reasons.append("missing_transcript")

    temp_audio = None
    try:
        audio_path, temp_audio = materialize_cleaner_audio(row.get(audio_column), dataset_path, site_root)
        if audio_path is not None:
            details["audio_path"] = str(audio_path)
        if audio_path is None:
            reasons.append("missing_audio")
        elif not audio_path.exists():
            reasons.append("audio_not_found")
        elif not audio_path.is_file():
            reasons.append("audio_not_file")
        else:
            if use_cps:
                try:
                    duration = cleaner_audio_duration_seconds(audio_path)
                    details["duration_seconds"] = round(duration, 3)
                    if duration <= 0:
                        reasons.append("invalid_duration")
                    else:
                        cps = len(expected_text.strip()) / duration if expected_text else 0.0
                        details["chars_per_second"] = round(cps, 3)
                        if cps < min_cps or cps > max_cps:
                            reasons.append("chars_per_second_out_of_range")
                except Exception as exc:
                    details["duration_error"] = str(exc)
                    reasons.append("duration_check_failed")

            if use_whisper and expected_text:
                try:
                    kwargs = {"language": language} if language else {}
                    result = whisper_model.transcribe(str(audio_path), **kwargs)
                    predicted = normalized_asr_text(result.get("text", ""))
                    cer = float(cer_fn(expected_text, predicted))
                    details["whisper"] = predicted
                    details["cer"] = round(cer, 4)
                    if cer > cer_threshold:
                        reasons.append("cer_above_threshold")
                except Exception as exc:
                    details["whisper_error"] = str(exc)
                    reasons.append("whisper_check_failed")
    finally:
        if temp_audio:
            try:
                temp_audio.unlink(missing_ok=True)
            except Exception:
                pass

    details["reason"] = "|".join(reasons)
    return bool(reasons), details


def cleaner_report_path(output_path: Path) -> Path:
    stamp = time.strftime("%Y%m%d-%H%M%S")
    return output_path.parent / f"{output_path.name}_cleaner_report_{stamp}.json"


def run_dataset_cleaner_job(job_id: str, params: dict, site_root: Path) -> None:
    pipeline_dir = site_root / "synthetic_cs_dataset"
    temp_output: Path | None = None
    output_path: Path | None = None
    try:
        dataset_value = str(params.get("dataset_path") or "").strip()
        if not dataset_value:
            raise RuntimeError("Missing dataset path.")

        output_mode = str(params.get("output_mode") or "copy").strip()
        if output_mode not in {"copy", "overwrite"}:
            raise RuntimeError("Save mode must be `copy` or `overwrite`.")

        dataset_path = resolve_hf_dataset_path(dataset_value, site_root, pipeline_dir, must_exist=True)
        if not dataset_path.exists():
            raise RuntimeError(f"Dataset path was not found: {dataset_path}")

        output_value = str(params.get("output_path") or "").strip()
        overwrite_output = cleaner_bool(params, "overwrite_output", False)
        if output_mode == "overwrite":
            output_path = dataset_path
        else:
            output_path = (
                resolve_hf_dataset_path(output_value, site_root, pipeline_dir, must_exist=False)
                if output_value
                else unique_path(dataset_path.with_name(f"{dataset_path.name}_cleaned"))
            )
            if output_path == dataset_path:
                raise RuntimeError("Use Override original dataset when the output path is the input path.")
            if output_path.exists() and not overwrite_output:
                raise RuntimeError(f"Output path already exists: {output_path}. Enable overwrite or choose a new folder.")

        cer_threshold = parse_cleaner_float(params, "cer_threshold", 0.6)
        min_cps = parse_cleaner_float(params, "min_cps", 5)
        max_cps = parse_cleaner_float(params, "max_cps", 22)
        if cer_threshold < 0:
            raise RuntimeError("CER threshold must be 0 or greater.")
        if min_cps <= 0 or max_cps <= 0 or min_cps > max_cps:
            raise RuntimeError("Chars/sec bounds must be positive, with min <= max.")

        use_whisper = cleaner_bool(params, "use_whisper", True)
        use_cps = cleaner_bool(params, "use_cps", True)
        if not use_whisper and not use_cps:
            raise RuntimeError("Enable at least one cleaning check.")

        whisper_model_name = str(params.get("whisper_model") or "large-v3-turbo").strip() or "large-v3-turbo"
        language = str(params.get("language") or "ar").strip()

        update_cleaner_job(
            job_id,
            status="running",
            stage="Loading",
            percent=5,
            message=f"Loading dataset from {dataset_path}",
            result={"input_path": str(dataset_path), "output_path": str(output_path)},
        )

        try:
            from datasets import Audio, Dataset, DatasetDict, load_from_disk
        except ImportError as exc:
            raise RuntimeError("Install the `datasets` package before cleaning datasets.") from exc

        loaded = load_from_disk(str(dataset_path))
        if isinstance(loaded, DatasetDict):
            split_map = dict(loaded.items())
            single_dataset = False
        elif isinstance(loaded, Dataset):
            split_map = {"train": loaded}
            single_dataset = True
        else:
            raise RuntimeError(f"Unsupported Hugging Face dataset type at {dataset_path}")

        transcript_column = infer_common_cleaner_column(
            split_map,
            str(params.get("transcript_column") or "").strip(),
            TRANSCRIPT_COLUMN_CANDIDATES,
            "Transcript",
        )
        audio_column = infer_common_cleaner_column(
            split_map,
            str(params.get("audio_column") or "").strip(),
            AUDIO_COLUMN_CANDIDATES,
            "Audio",
            audio=True,
        )

        cer_fn = None
        whisper_model = None
        if use_whisper:
            cer_fn = cleaner_jiwer_cer()
            update_cleaner_job(
                job_id,
                stage="Loading Whisper",
                percent=12,
                message=f"Loading Whisper model: {whisper_model_name}",
            )
            whisper_model = get_whisper_model(whisper_model_name)

        rows_by_split = {split: len(dataset) for split, dataset in split_map.items()}
        total_rows = sum(rows_by_split.values())
        if total_rows <= 0:
            raise RuntimeError("The dataset has no rows to clean.")

        cleaned_splits = {}
        kept_by_split: dict[str, int] = {}
        removed_by_split: dict[str, int] = {}
        bad_samples: list[dict] = []
        processed = 0
        last_update = 0.0

        for split, dataset in split_map.items():
            prepared = prepare_cleaner_audio_column(dataset, audio_column, Audio)
            good_indices: list[int] = []
            for index in range(len(prepared)):
                row = dict(prepared[index])
                is_bad, details = evaluate_cleaner_row(
                    row,
                    split,
                    index,
                    dataset_path,
                    site_root,
                    transcript_column,
                    audio_column,
                    use_cps,
                    min_cps,
                    max_cps,
                    use_whisper,
                    whisper_model,
                    cer_fn,
                    cer_threshold,
                    language,
                )
                if is_bad:
                    bad_samples.append(details)
                else:
                    good_indices.append(index)

                processed += 1
                now = time.time()
                if now - last_update >= 1.2 or processed == total_rows:
                    percent = 18 + min(70, (processed / total_rows) * 70)
                    update_cleaner_job(
                        job_id,
                        stage="Checking samples",
                        percent=round(percent, 1),
                        message=(
                            f"Checked {processed}/{total_rows} sample(s). "
                            f"Marked {len(bad_samples)} for removal."
                        ),
                        result={
                            "input_path": str(dataset_path),
                            "output_path": str(output_path),
                            "processed_rows": processed,
                            "removed_rows": len(bad_samples),
                            "rows_by_split": rows_by_split,
                        },
                    )
                    last_update = now

            cleaned_splits[split] = prepared.select(good_indices)
            kept_by_split[split] = len(good_indices)
            removed_by_split[split] = len(prepared) - len(good_indices)

        cleaned = cleaned_splits["train"] if single_dataset else DatasetDict(cleaned_splits)
        removed_rows = len(bad_samples)
        kept_rows = sum(kept_by_split.values())

        update_cleaner_job(
            job_id,
            stage="Saving",
            percent=92,
            message=f"Saving cleaned dataset to {output_path}",
            result={
                "input_path": str(dataset_path),
                "output_path": str(output_path),
                "total_rows": total_rows,
                "kept_rows": kept_rows,
                "removed_rows": removed_rows,
                "kept_by_split": kept_by_split,
                "removed_by_split": removed_by_split,
            },
        )

        if output_mode == "overwrite":
            temp_output = output_path.with_name(f".{output_path.name}.cleaner-{uuid.uuid4().hex[:8]}")
            temp_output.parent.mkdir(parents=True, exist_ok=True)
            cleaned.save_to_disk(str(temp_output))
            guarded_remove_output(output_path, site_root, pipeline_dir)
            shutil.move(str(temp_output), str(output_path))
            temp_output = None
        else:
            if output_path.exists():
                guarded_remove_output(output_path, site_root, pipeline_dir)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            cleaned.save_to_disk(str(output_path))

        report_path = cleaner_report_path(output_path)
        result = {
            "input_path": str(dataset_path),
            "output_path": str(output_path),
            "output_mode": output_mode,
            "report_path": str(report_path),
            "total_rows": total_rows,
            "kept_rows": kept_rows,
            "removed_rows": removed_rows,
            "rows_by_split": rows_by_split,
            "kept_by_split": kept_by_split,
            "removed_by_split": removed_by_split,
            "transcript_column": transcript_column,
            "audio_column": audio_column,
            "checks": {
                "whisper": use_whisper,
                "whisper_model": whisper_model_name if use_whisper else "",
                "language": language,
                "cer_threshold": cer_threshold,
                "chars_per_second": use_cps,
                "min_cps": min_cps,
                "max_cps": max_cps,
            },
            "bad_samples_preview": bad_samples[:25],
        }
        report_payload = dict(result)
        report_payload["bad_samples"] = bad_samples
        report_path.write_text(json.dumps(report_payload, ensure_ascii=False, indent=2), encoding="utf-8")

        DatasetViewerHandler.dataset_cache.clear()
        DatasetViewerHandler.dataset_path = output_path

        update_cleaner_job(
            job_id,
            status="completed",
            stage="Completed",
            percent=100,
            message=f"Removed {removed_rows} bad sample(s). Cleaned dataset: {output_path}",
            result=result,
        )
    except Exception as exc:
        if temp_output and temp_output.exists():
            try:
                guarded_remove_output(temp_output, site_root, pipeline_dir)
            except Exception:
                pass
        update_cleaner_job(
            job_id,
            status="failed",
            stage="Failed",
            percent=100,
            message="Dataset cleaner failed.",
            error=str(exc),
            log_tail=str(exc),
            result={"output_path": str(output_path) if output_path else ""},
        )


def resolve_project_path(path_value: str, site_root: Path, pipeline_dir: Path) -> Path:
    raw = Path(str(path_value or "").strip()).expanduser()
    if raw.is_absolute():
        return raw

    candidates = [
        site_root / raw,
        pipeline_dir / raw,
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    return candidates[0].resolve()


def resolve_pipeline_output(path_value: str, pipeline_dir: Path) -> Path:
    raw = Path(str(path_value or "").strip()).expanduser()
    if raw.is_absolute():
        return raw
    return (pipeline_dir / raw).resolve()


def count_jsonl_lines(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        return sum(1 for line in handle if line.strip())


def count_audio_files(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for item in path.glob("*.wav") if item.is_file())


def read_tail(path: Path, limit: int = 5000) -> str:
    if not path.exists():
        return ""
    data = path.read_bytes()
    return data[-limit:].decode("utf-8", errors="replace")


def prepare_runtime_config(job_id: str, params: dict, site_root: Path, pipeline_dir: Path) -> tuple[Path, dict]:
    try:
        import yaml
    except ImportError as exc:
        raise RuntimeError("Install PyYAML before starting generation: pip install pyyaml") from exc

    config_path = resolve_project_path(
        params.get("config_path") or "synthetic_cs_dataset/configs/generation.yaml",
        site_root,
        pipeline_dir,
    )
    if not config_path.exists():
        raise RuntimeError(f"Config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)

    text_config = config.setdefault("text_generation", {})
    audio_config = config.setdefault("audio_generation", {})
    review_config = config.setdefault("review", {})

    num_texts = int(params.get("num_texts") or text_config.get("num_texts") or 1)
    batch_size = int(params.get("batch_size") or text_config.get("batch_size") or 40)
    target_hours = float(params.get("target_hours") or audio_config.get("target_hours") or 12)
    augmentation_probability = float(
        params.get("augmentation_probability")
        if params.get("augmentation_probability") is not None
        else audio_config.get("augmentation_probability", 0.0)
    )
    output_dir = str(params.get("output_dir") or audio_config.get("output_dir") or "data").strip()
    reference_dir = str(
        params.get("reference_speakers_dir")
        or audio_config.get("reference_speakers_dir")
        or "data/reference_speakers"
    ).strip()

    if num_texts < 1 or num_texts > 30000:
        raise RuntimeError("num_texts must be between 1 and 30000.")
    if batch_size < 1:
        raise RuntimeError("batch_size must be at least 1.")
    if target_hours <= 0:
        raise RuntimeError("target_hours must be greater than 0.")
    if not 0 <= augmentation_probability <= 1:
        raise RuntimeError("augmentation_probability must be between 0 and 1.")

    audio_subdir = str(audio_config.get("audio_subdir", "audio"))
    text_output_path = resolve_pipeline_output(f"{output_dir}/texts.jsonl", pipeline_dir)
    text_config["num_texts"] = num_texts
    text_config["batch_size"] = batch_size
    text_config["resume"] = count_jsonl_lines(text_output_path) > 0
    text_config["output_path"] = f"{output_dir}/texts.jsonl"
    audio_config["texts_path"] = f"{output_dir}/texts.jsonl"
    audio_config["output_dir"] = output_dir
    audio_config["logs_dir"] = f"{output_dir}/generation_logs"
    audio_config["target_hours"] = target_hours
    audio_config["augmentation_probability"] = augmentation_probability
    audio_config["reference_speakers_dir"] = reference_dir
    review_config["output_path"] = f"{output_dir}/review_samples.csv"

    runtime_path = pipeline_dir / "configs" / f"runtime_generation_{job_id}.yaml"
    with runtime_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(config, handle, allow_unicode=True, sort_keys=False)

    paths = {
        "num_texts": num_texts,
        "target_hours": target_hours,
        "config_path": runtime_path,
        "text_output_path": resolve_pipeline_output(text_config["output_path"], pipeline_dir),
        "audio_dir": resolve_pipeline_output(f"{output_dir}/{audio_subdir}", pipeline_dir),
        "log_path": resolve_pipeline_output(f"{output_dir}/generation_logs/web_generation_{job_id}.log", pipeline_dir),
        "output_dir": resolve_pipeline_output(output_dir, pipeline_dir),
    }
    paths["log_path"].parent.mkdir(parents=True, exist_ok=True)
    return runtime_path, paths


def run_tracked_command(job_id: str, command: list[str], cwd: Path, log_path: Path, progress_callback) -> None:
    with log_path.open("a", encoding="utf-8") as log:
        log.write("\n$ " + " ".join(command) + "\n")
        log.flush()
        process = subprocess.Popen(
            command,
            cwd=str(cwd),
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        while process.poll() is None:
            progress_callback()
            time.sleep(2)
        progress_callback()
        if process.returncode != 0:
            raise RuntimeError(read_tail(log_path) or f"Command failed with exit code {process.returncode}.")


def run_generation_job(job_id: str, params: dict, site_root: Path) -> None:
    pipeline_dir = site_root / "synthetic_cs_dataset"
    try:
        update_generation_job(job_id, status="running", stage="Preparing", percent=2, message="Preparing runtime config.")
        runtime_config, paths = prepare_runtime_config(job_id, params, site_root, pipeline_dir)
        log_path = paths["log_path"]
        num_texts = max(1, int(paths["num_texts"]))

        update_generation_job(
            job_id,
            stage="Text generation",
            percent=5,
            message=f"Generating {num_texts} transcript row(s).",
            log_tail=str(log_path),
        )

        def text_progress() -> None:
            accepted = count_jsonl_lines(paths["text_output_path"])
            percent = 5 + min(40, (accepted / num_texts) * 40)
            update_generation_job(
                job_id,
                percent=round(percent, 1),
                message=f"Text rows accepted: {accepted}/{num_texts}",
                log_tail=read_tail(log_path),
            )

        run_tracked_command(
            job_id,
            [
                sys.executable,
                "scripts/generate_texts.py",
                "--config",
                str(runtime_config),
                "--num-texts",
                str(num_texts),
                "--batch-size",
                str(params.get("batch_size") or 40),
            ],
            pipeline_dir,
            log_path,
            text_progress,
        )

        update_generation_job(
            job_id,
            stage="Audio generation",
            percent=50,
            message=f"Generating audio toward {paths['target_hours']} target hour(s).",
            log_tail=read_tail(log_path),
        )

        def audio_progress() -> None:
            audio_count = count_audio_files(paths["audio_dir"])
            percent = 50 + min(45, (audio_count / num_texts) * 45)
            update_generation_job(
                job_id,
                percent=round(percent, 1),
                message=f"Audio files written: {audio_count}",
                log_tail=read_tail(log_path),
            )

        run_tracked_command(
            job_id,
            [
                sys.executable,
                "scripts/generate_audio.py",
                "--config",
                str(runtime_config),
                "--target-hours",
                str(paths["target_hours"]),
            ],
            pipeline_dir,
            log_path,
            audio_progress,
        )

        update_generation_job(
            job_id,
            status="completed",
            stage="Completed",
            percent=100,
            message=f"Generation finished. Output folder: {paths['output_dir']}",
            log_tail=read_tail(log_path),
        )
    except Exception as exc:
        log_tail = ""
        try:
            log_tail = read_tail(paths["log_path"])  # type: ignore[name-defined]
        except Exception:
            pass
        update_generation_job(
            job_id,
            status="failed",
            stage="Failed",
            percent=100,
            message="Generation failed.",
            error=str(exc),
            log_tail=log_tail or str(exc),
        )


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
        error = str(payload.get("error") or "")
        nested = parse_nested_smoke_error(error)
        return nested or friendly_smoke_error(error) or error
    return ""


def parse_nested_smoke_error(error: str) -> str:
    try:
        payload = json.loads(error)
    except json.JSONDecodeError:
        return friendly_smoke_error(error)
    if not isinstance(payload, dict):
        return friendly_smoke_error(error)
    stderr = str(payload.get("stderr") or "").strip()
    stdout = str(payload.get("stdout") or "").strip()
    text = stderr or stdout or error
    return friendly_smoke_error(text) or text


def friendly_smoke_error(text: str) -> str:
    if "No speaker references found" in text:
        return (
            "No OmniVoice reference speaker was found. Put a WAV file in "
            "`synthetic_cs_dataset/data/reference_speakers/` with a matching `.txt` transcript, "
            "or fill the Quick Test reference audio path and reference text fields. "
            "The reference audio path can be absolute, or relative to `synthetic_cs_dataset/`."
        )
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
        print("Synthetic smoke test endpoint: /api/synthetic-test")
        server.serve_forever()


if __name__ == "__main__":
    main()

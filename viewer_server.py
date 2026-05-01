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
import subprocess
import sys
import threading
import time
import uuid
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urlparse


AUDIO_EXTENSIONS = {".wav", ".mp3", ".m4a", ".ogg", ".oga", ".aac", ".flac", ".webm"}
DEFAULT_DATASET_PATH = Path("/teamspace/studios/this_studio/darija_clean")
GENERATION_JOBS: dict[str, dict] = {}
GENERATION_LOCK = threading.Lock()


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
                    "features": ["datasets", "audio", "synthetic-test", "generation-jobs"],
                    "default_dataset_path": str(self.dataset_path),
                    "default_max_rows": self.max_rows,
                }
            )
            return
        if parsed.path == "/api/generation/status":
            self.send_generation_status(parsed.query)
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
    }


def update_generation_job(job_id: str, **updates) -> None:
    with GENERATION_LOCK:
        job = GENERATION_JOBS.get(job_id)
        if not job:
            return
        job.update(updates)
        job["updated_at"] = time.time()


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
        else audio_config.get("augmentation_probability", 0.35)
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
    text_config["num_texts"] = num_texts
    text_config["batch_size"] = batch_size
    text_config["resume"] = False
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

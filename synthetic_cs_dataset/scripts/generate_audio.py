#!/usr/bin/env python3
"""Generate TTS audio for synthetic Darija code-switch transcripts."""

from __future__ import annotations

import argparse
import csv
import json
import random
import re
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import soundfile as sf

try:
    import librosa
except ImportError as exc:  # pragma: no cover - startup dependency check
    raise SystemExit("Install librosa first: pip install librosa") from exc

try:
    import yaml
except ImportError as exc:  # pragma: no cover - startup dependency check
    raise SystemExit("Install PyYAML first: pip install pyyaml") from exc

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover - optional progress dependency
    tqdm = None

from augment_audio import augment_phone_call, peak_limit, rms_normalize, to_mono


@dataclass(frozen=True)
class SpeakerReference:
    speaker_id: str
    audio_path: Path
    ref_text: str


@dataclass(frozen=True)
class AudioCheck:
    ok: bool
    duration_seconds: float = 0.0
    reason: str = ""


class BaseTTS(ABC):
    """Interface for pluggable TTS backends."""

    sample_rate: int

    @abstractmethod
    def synthesize(
        self,
        text: str,
        speaker_reference_audio: str,
        speaker_reference_text: str,
        output_path: Path,
    ) -> None:
        """Generate one utterance and write it to output_path."""


class OmniVoiceTTS(BaseTTS):
    def __init__(self, config: dict[str, Any]):
        try:
            import torch
            from omnivoice import OmniVoice
        except ImportError as exc:  # pragma: no cover - backend dependency check
            raise SystemExit(
                "Install OmniVoice and torch before running audio generation for this backend."
            ) from exc

        dtype_name = str(config.get("dtype", "float16"))
        dtype = {
            "float16": torch.float16,
            "bfloat16": torch.bfloat16,
            "float32": torch.float32,
        }.get(dtype_name, torch.float16)
        self.sample_rate = int(config.get("sample_rate", 24000))
        self.model = OmniVoice.from_pretrained(
            config.get("tts_model_name_or_path", "k2-fsa/OmniVoice"),
            device_map=config.get("device_map", "cuda:0"),
            dtype=dtype,
        )

    def synthesize(
        self,
        text: str,
        speaker_reference_audio: str,
        speaker_reference_text: str,
        output_path: Path,
    ) -> None:
        audio = self.model.generate(
            text=text,
            ref_audio=speaker_reference_audio,
            ref_text=speaker_reference_text,
        )
        waveform = audio[0]
        if hasattr(waveform, "detach"):
            waveform = waveform.detach().cpu().numpy()
        waveform = np.asarray(waveform, dtype=np.float32)
        waveform = to_mono(waveform)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        sf.write(output_path, waveform, self.sample_rate)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("configs/generation.yaml"))
    parser.add_argument("--target-hours", type=float, default=None)
    parser.add_argument("--limit", type=int, default=None, help="Debug limit for number of input texts.")
    return parser.parse_args()


def load_config(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def resolve_path(path_value: str | Path, base_dir: Path) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    return base_dir / path


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def write_log(log_path: Path, payload: dict[str, Any]) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def load_references(config: dict[str, Any], base_dir: Path) -> list[SpeakerReference]:
    refs_dir_value = config.get("reference_speakers_dir") or ""
    refs_dir = resolve_path(refs_dir_value, base_dir) if refs_dir_value else None
    references: list[SpeakerReference] = []

    if refs_dir and refs_dir.exists():
        csv_path = refs_dir / "references.csv"
        if csv_path.exists():
            with csv_path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    audio_value = row.get("audio_path") or row.get("ref_audio") or row.get("file_name")
                    ref_text = row.get("ref_text") or row.get("speaker_reference_text") or ""
                    if not audio_value or not ref_text:
                        continue
                    audio_path = Path(audio_value)
                    if not audio_path.is_absolute():
                        audio_path = refs_dir / audio_path
                    speaker_id = row.get("speaker_id") or audio_path.stem
                    references.append(SpeakerReference(speaker_id, audio_path, ref_text))
        else:
            audio_files = sorted(
                [
                    *refs_dir.glob("*.wav"),
                    *refs_dir.glob("*.flac"),
                    *refs_dir.glob("*.mp3"),
                    *refs_dir.glob("*.m4a"),
                ]
            )
            for audio_path in audio_files:
                sidecar = audio_path.with_suffix(".txt")
                if not sidecar.exists():
                    continue
                references.append(
                    SpeakerReference(audio_path.stem, audio_path, sidecar.read_text(encoding="utf-8").strip())
                )

    default_audio = str(config.get("default_reference_audio") or "").strip()
    default_text = str(config.get("default_reference_text") or "").strip()
    if not references and default_audio and default_text:
        references.append(
            SpeakerReference(
                "default",
                resolve_path(default_audio, base_dir),
                default_text,
            )
        )

    references = [ref for ref in references if ref.audio_path.exists() and ref.ref_text]
    if not references:
        raise SystemExit(
            "No speaker references found. Add WAV files with .txt sidecars or references.csv under "
            f"{refs_dir}, or set default_reference_audio/default_reference_text in the config."
        )
    return references[:30]


def make_tts(config: dict[str, Any]) -> BaseTTS:
    backend = str(config.get("tts_backend", "omnivoice")).lower()
    if backend == "omnivoice":
        return OmniVoiceTTS(config)
    raise SystemExit(f"Unsupported tts_backend: {backend}")


def assign_splits(rows: list[dict[str, Any]], split_config: dict[str, float], seed: int) -> dict[str, str]:
    ids = [str(row["id"]) for row in rows]
    rng = random.Random(seed)
    rng.shuffle(ids)
    total = len(ids)
    train_end = int(total * float(split_config.get("train", 0.90)))
    validation_end = train_end + int(total * float(split_config.get("validation", 0.05)))
    assignments: dict[str, str] = {}
    for index, item_id in enumerate(ids):
        if index < train_end:
            assignments[item_id] = "train"
        elif index < validation_end:
            assignments[item_id] = "validation"
        else:
            assignments[item_id] = "test"
    return assignments


def postprocess_audio(path: Path, sample_rate: int) -> None:
    audio, current_sr = sf.read(path, always_2d=False)
    audio = to_mono(np.asarray(audio, dtype=np.float32))
    if current_sr != sample_rate:
        audio = librosa.resample(audio, orig_sr=current_sr, target_sr=sample_rate)
    trimmed, _index = librosa.effects.trim(audio, top_db=35)
    if trimmed.size:
        audio = trimmed
    audio = rms_normalize(peak_limit(audio), target_dbfs=-20.0)
    sf.write(path, audio.astype(np.float32), sample_rate)


def check_audio(path: Path, min_seconds: float, max_seconds: float) -> AudioCheck:
    if not path.exists():
        return AudioCheck(False, reason="missing_file")
    try:
        audio, sample_rate = sf.read(path, always_2d=False)
    except Exception as exc:  # pragma: no cover - depends on corrupt file
        return AudioCheck(False, reason=f"read_failed:{exc}")
    audio = to_mono(np.asarray(audio, dtype=np.float32))
    duration = float(len(audio) / sample_rate) if sample_rate else 0.0
    if duration < min_seconds:
        return AudioCheck(False, duration, "too_short")
    if duration > max_seconds:
        return AudioCheck(False, duration, "too_long")
    peak = float(np.max(np.abs(audio))) if audio.size else 0.0
    rms = float(np.sqrt(np.mean(np.square(audio)))) if audio.size else 0.0
    if peak < 1e-4 or rms < 1e-5:
        return AudioCheck(False, duration, "silent")
    clipped_fraction = float(np.mean(np.abs(audio) >= 0.999)) if audio.size else 0.0
    if clipped_fraction > 0.001:
        return AudioCheck(False, duration, "clipped")
    return AudioCheck(True, duration)


def metadata_row(
    row: dict[str, Any],
    file_name: str,
    source: str,
    speaker_id: str,
    is_synthetic: bool = True,
) -> dict[str, Any]:
    return {
        "file_name": file_name,
        "text": row["text"],
        "source": source,
        "domain": row.get("domain", ""),
        "language_mix": "|".join(row.get("language_mix", [])),
        "speaker_id": speaker_id,
        "is_synthetic": str(is_synthetic).lower(),
    }


def safe_filename_part(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", value.strip())
    return cleaned.strip("._") or "speaker"


def dataset_row(
    row: dict[str, Any],
    file_name: str,
    source: str,
    speaker_id: str,
    split: str,
    duration_seconds: float,
    is_augmented: bool,
    temporary_synthetic_test: bool,
) -> dict[str, Any]:
    return {
        "id": row["id"],
        "audio": file_name,
        "file_name": file_name,
        "text": row["text"],
        "source": source,
        "domain": row.get("domain", ""),
        "language_mix": row.get("language_mix", []),
        "contains_code_switch": bool(row.get("contains_code_switch", False)),
        "speaker_id": speaker_id,
        "is_synthetic": True,
        "is_augmented": is_augmented,
        "split": split,
        "duration_seconds": round(duration_seconds, 3),
        "is_temporary_synthetic_test": temporary_synthetic_test,
    }


def should_add_second_speaker(rng: random.Random, probability: float, speaker_count: int) -> bool:
    return speaker_count > 1 and rng.random() < probability


def write_outputs(
    output_dir: Path,
    metadata_rows: list[dict[str, Any]],
    dataset_rows: list[dict[str, Any]],
    review_size: int,
    review_path: Path,
    seed: int,
) -> None:
    metadata_path = output_dir / "metadata.csv"
    dataset_path = output_dir / "dataset.jsonl"
    output_dir.mkdir(parents=True, exist_ok=True)

    metadata_fields = [
        "file_name",
        "text",
        "source",
        "domain",
        "language_mix",
        "speaker_id",
        "is_synthetic",
    ]
    with metadata_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=metadata_fields)
        writer.writeheader()
        for row in metadata_rows:
            writer.writerow({field: row.get(field, "") for field in metadata_fields})

    with dataset_path.open("w", encoding="utf-8") as handle:
        for row in dataset_rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    rng = random.Random(seed)
    review_rows = dataset_rows[:]
    rng.shuffle(review_rows)
    review_rows = review_rows[: min(review_size, len(review_rows))]
    review_path.parent.mkdir(parents=True, exist_ok=True)
    review_fields = [
        "id",
        "file_name",
        "text",
        "domain",
        "language_mix",
        "speaker_id",
        "split",
        "source",
        "duration_seconds",
        "is_augmented",
    ]
    with review_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=review_fields)
        writer.writeheader()
        for row in review_rows:
            output = dict(row)
            output["language_mix"] = "|".join(output.get("language_mix", []))
            writer.writerow({field: output.get(field, "") for field in review_fields})


def main() -> None:
    args = parse_args()
    config_path = args.config.resolve()
    base_dir = config_path.parent.parent
    config = load_config(config_path)
    seed = int(config.get("project", {}).get("seed", 42))
    rng = random.Random(seed)

    audio_config = config["audio_generation"]
    if args.target_hours is not None:
        audio_config["target_hours"] = args.target_hours

    texts_path = resolve_path(audio_config["texts_path"], base_dir)
    output_dir = resolve_path(audio_config["output_dir"], base_dir)
    audio_dir = output_dir / str(audio_config.get("audio_subdir", "audio"))
    logs_dir = resolve_path(audio_config.get("logs_dir", output_dir / "generation_logs"), base_dir)
    log_path = logs_dir / "audio_generation.jsonl"
    if log_path.exists():
        log_path.unlink()

    rows = read_jsonl(texts_path)
    if args.limit is not None:
        rows = rows[: args.limit]
    if not rows:
        raise SystemExit(f"No text rows found in {texts_path}")

    references = load_references(audio_config, base_dir)
    split_assignments = assign_splits(rows, config.get("splits", {}), seed)
    sample_rate = int(audio_config.get("sample_rate", 24000))
    min_seconds = float(audio_config.get("min_duration_seconds", 1.0))
    max_seconds = float(audio_config.get("max_duration_seconds", 25.0))
    target_seconds = float(audio_config.get("target_hours", 12.0)) * 3600.0
    duplicate_probability = float(audio_config.get("duplicate_second_speaker_probability", 0.25))
    augmentation_probability = float(audio_config.get("augmentation_probability", 0.35))
    augmentation_suffix = str(audio_config.get("augmentation_suffix", "phone"))
    source = str(audio_config.get("source_label", "synthetic_tts"))
    real_test_metadata = str(config.get("splits", {}).get("real_human_test_metadata") or "").strip()

    tts = make_tts(audio_config)
    metadata_rows: list[dict[str, Any]] = []
    dataset_rows: list[dict[str, Any]] = []
    total_seconds = 0.0

    progress_total = len(rows)
    progress = tqdm(total=progress_total, desc="TTS texts") if tqdm else None

    for text_index, row in enumerate(rows):
        if total_seconds >= target_seconds:
            break
        base_ref_index = text_index % len(references)
        ref_indices = [base_ref_index]
        if should_add_second_speaker(rng, duplicate_probability, len(references)):
            ref_indices.append((base_ref_index + rng.randint(1, len(references) - 1)) % len(references))

        split = split_assignments[str(row["id"])]
        for version_index, ref_index in enumerate(ref_indices):
            if total_seconds >= target_seconds:
                break
            ref = references[ref_index]
            suffix = "" if version_index == 0 else f"_spk{safe_filename_part(ref.speaker_id)}"
            filename = f"{row['id']}{suffix}.wav"
            relative_file = f"{audio_config.get('audio_subdir', 'audio')}/{filename}"
            output_path = audio_dir / filename

            try:
                tts.synthesize(row["text"], str(ref.audio_path), ref.ref_text, output_path)
                postprocess_audio(output_path, sample_rate)
                check = check_audio(output_path, min_seconds, max_seconds)
            except Exception as exc:  # pragma: no cover - backend runtime failures
                write_log(
                    log_path,
                    {
                        "id": row.get("id"),
                        "file_name": relative_file,
                        "speaker_id": ref.speaker_id,
                        "status": "failed",
                        "reason": str(exc),
                    },
                )
                continue

            if not check.ok:
                write_log(
                    log_path,
                    {
                        "id": row.get("id"),
                        "file_name": relative_file,
                        "speaker_id": ref.speaker_id,
                        "status": "rejected",
                        "reason": check.reason,
                        "duration_seconds": check.duration_seconds,
                    },
                )
                continue

            temporary_test = split == "test" and not real_test_metadata
            metadata_rows.append(metadata_row(row, relative_file, source, ref.speaker_id))
            dataset_rows.append(
                dataset_row(
                    row,
                    relative_file,
                    source,
                    ref.speaker_id,
                    split,
                    check.duration_seconds,
                    is_augmented=False,
                    temporary_synthetic_test=temporary_test,
                )
            )
            total_seconds += check.duration_seconds
            write_log(
                log_path,
                {
                    "id": row.get("id"),
                    "file_name": relative_file,
                    "speaker_id": ref.speaker_id,
                    "status": "accepted",
                    "duration_seconds": check.duration_seconds,
                    "split": split,
                },
            )

            if split == "train" and rng.random() < augmentation_probability:
                aug_filename = f"{output_path.stem}_{augmentation_suffix}.wav"
                aug_path = audio_dir / aug_filename
                aug_relative = f"{audio_config.get('audio_subdir', 'audio')}/{aug_filename}"
                audio, sr = sf.read(output_path, always_2d=False)
                augmented = augment_phone_call(audio, sr, seed=rng.randint(0, 2**31 - 1))
                sf.write(aug_path, augmented, sr)
                aug_check = check_audio(aug_path, min_seconds, max_seconds)
                if aug_check.ok:
                    metadata_rows.append(metadata_row(row, aug_relative, f"{source}_phone_aug", ref.speaker_id))
                    dataset_rows.append(
                        dataset_row(
                            row,
                            aug_relative,
                            f"{source}_phone_aug",
                            ref.speaker_id,
                            split,
                            aug_check.duration_seconds,
                            is_augmented=True,
                            temporary_synthetic_test=False,
                        )
                    )
                    total_seconds += aug_check.duration_seconds
                    write_log(
                        log_path,
                        {
                            "id": row.get("id"),
                            "file_name": aug_relative,
                            "speaker_id": ref.speaker_id,
                            "status": "accepted_augmented",
                            "duration_seconds": aug_check.duration_seconds,
                            "split": split,
                        },
                    )
                else:
                    write_log(
                        log_path,
                        {
                            "id": row.get("id"),
                            "file_name": aug_relative,
                            "speaker_id": ref.speaker_id,
                            "status": "rejected_augmented",
                            "reason": aug_check.reason,
                            "duration_seconds": aug_check.duration_seconds,
                        },
                    )
        if progress:
            progress.update(1)
        elif text_index and text_index % 25 == 0:
            hours = total_seconds / 3600.0
            print(f"Generated {hours:.2f} hours", file=sys.stderr)

    if progress:
        progress.close()

    review_config = config.get("review", {})
    review_path = resolve_path(review_config.get("output_path", "data/review_samples.csv"), base_dir)
    write_outputs(
        output_dir,
        metadata_rows,
        dataset_rows,
        int(review_config.get("sample_size", 300)),
        review_path,
        seed,
    )

    summary = {
        "accepted_audio_files": len(dataset_rows),
        "total_hours": round(total_seconds / 3600.0, 3),
        "target_hours": float(audio_config.get("target_hours", 12.0)),
        "speaker_references": len(references),
        "synthetic_test_is_temporary": not bool(real_test_metadata),
    }
    write_log(log_path, {"status": "summary", **summary})
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

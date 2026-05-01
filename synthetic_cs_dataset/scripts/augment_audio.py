#!/usr/bin/env python3
"""Light phone-call augmentation for synthetic ASR audio."""

from __future__ import annotations

import argparse
import random
from pathlib import Path

import numpy as np
import soundfile as sf


def to_mono(audio: np.ndarray) -> np.ndarray:
    if audio.ndim == 1:
        return audio.astype(np.float32)
    return np.mean(audio, axis=1).astype(np.float32)


def peak_limit(audio: np.ndarray, peak: float = 0.98) -> np.ndarray:
    max_abs = float(np.max(np.abs(audio))) if audio.size else 0.0
    if max_abs > peak:
        audio = audio * (peak / max_abs)
    return audio.astype(np.float32)


def rms_normalize(audio: np.ndarray, target_dbfs: float = -20.0) -> np.ndarray:
    rms = float(np.sqrt(np.mean(np.square(audio)))) if audio.size else 0.0
    if rms < 1e-8:
        return audio.astype(np.float32)
    target = 10 ** (target_dbfs / 20.0)
    return peak_limit(audio * (target / rms))


def fft_bandpass(audio: np.ndarray, sample_rate: int, low_hz: float = 300.0, high_hz: float = 3400.0) -> np.ndarray:
    if audio.size == 0:
        return audio
    spectrum = np.fft.rfft(audio)
    freqs = np.fft.rfftfreq(audio.size, d=1.0 / sample_rate)
    mask = (freqs >= low_hz) & (freqs <= high_hz)
    spectrum *= mask
    filtered = np.fft.irfft(spectrum, n=audio.size)
    return filtered.astype(np.float32)


def compress(audio: np.ndarray, threshold: float = 0.22, ratio: float = 2.5) -> np.ndarray:
    sign = np.sign(audio)
    magnitude = np.abs(audio)
    compressed = np.where(
        magnitude > threshold,
        threshold + (magnitude - threshold) / ratio,
        magnitude,
    )
    return (compressed * sign).astype(np.float32)


def add_room_reverb(audio: np.ndarray, sample_rate: int, rng: random.Random) -> np.ndarray:
    decay_ms = rng.uniform(35, 90)
    impulse_len = max(8, int(sample_rate * decay_ms / 1000.0))
    times = np.linspace(0.0, 1.0, impulse_len, dtype=np.float32)
    impulse = np.exp(-times * rng.uniform(5.0, 8.0)).astype(np.float32)
    impulse[0] = 1.0
    impulse *= rng.uniform(0.015, 0.045)
    impulse[0] = 1.0
    wet = np.convolve(audio, impulse, mode="full")[: audio.size]
    return peak_limit((0.88 * audio) + (0.12 * wet))


def add_noise_at_snr(audio: np.ndarray, rng: random.Random, snr_db: float) -> np.ndarray:
    rms = float(np.sqrt(np.mean(np.square(audio)))) if audio.size else 0.0
    if rms < 1e-8:
        return audio
    np_rng = np.random.default_rng(rng.randint(0, 2**32 - 1))
    noise_samples = np_rng.normal(0.0, 1.0, size=audio.size).astype(np.float32)
    noise_rms = float(np.sqrt(np.mean(np.square(noise_samples)))) or 1.0
    target_noise_rms = rms / (10 ** (snr_db / 20.0))
    noise_samples *= target_noise_rms / noise_rms
    return peak_limit(audio + noise_samples)


def mu_law_degrade(audio: np.ndarray, mu: int = 255) -> np.ndarray:
    clipped = np.clip(audio, -1.0, 1.0)
    encoded = np.sign(clipped) * np.log1p(mu * np.abs(clipped)) / np.log1p(mu)
    quantized = np.round((encoded + 1.0) * 127.5) / 127.5 - 1.0
    decoded = np.sign(quantized) * (1.0 / mu) * ((1.0 + mu) ** np.abs(quantized) - 1.0)
    return decoded.astype(np.float32)


def augment_phone_call(audio: np.ndarray, sample_rate: int, seed: int | None = None) -> np.ndarray:
    """Apply light, train-only phone-call degradation."""
    rng = random.Random(seed)
    augmented = to_mono(audio)
    augmented = fft_bandpass(augmented, sample_rate)
    augmented = compress(augmented, threshold=rng.uniform(0.18, 0.28), ratio=rng.uniform(1.8, 3.0))
    if rng.random() < 0.65:
        augmented = add_room_reverb(augmented, sample_rate, rng)
    augmented = add_noise_at_snr(augmented, rng, snr_db=rng.uniform(15.0, 30.0))
    if rng.random() < 0.30:
        augmented = mu_law_degrade(augmented)
    gain_db = rng.uniform(-3.0, 3.0)
    augmented *= 10 ** (gain_db / 20.0)
    return rms_normalize(peak_limit(augmented), target_dbfs=-20.0)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    audio, sample_rate = sf.read(args.input, always_2d=False)
    augmented = augment_phone_call(audio, sample_rate, seed=args.seed)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    sf.write(args.output, augmented, sample_rate)
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()

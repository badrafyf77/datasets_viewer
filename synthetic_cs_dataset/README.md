# Synthetic Moroccan Darija Code-Switch ASR Dataset

This pipeline generates synthetic phone-call style audio/transcription pairs for fine-tuning Whisper on Moroccan Darija with natural French and English code-switching.

The transcript convention is strict:

```text
دارجة بالعربية + French/English in Latin script
```

Good:

```text
السلام، اليوم عندي rendez-vous مع le client باش نهضرو على l'avancement ديال projet.
```

Bad:

```text
عندي رنديفو مع الكليان
```

## Setup

Install the Python packages you need for the stages you will run:

```bash
pip install openai pyyaml tqdm pandas soundfile librosa numpy datasets jsonlines
```

For audio generation, also install your OmniVoice environment and make sure GPU/CUDA dependencies are available.

Do not put API keys in the repo. Export your Lightning/OpenAI-compatible key:

```bash
export LIGHTNING_API_KEY="..."
```

## Configure

Edit `configs/generation.yaml`.

Important fields:

- `text_generation.num_texts`: default `10000`, set up to `30000`.
- `audio_generation.target_hours`: default `12.0`, suitable for the v1 target of 10-15 hours.
- `audio_generation.reference_speakers_dir`: folder with 10-30 speaker references if available.
- `audio_generation.augmentation_probability`: default `0.35`, applied only to train audio.
- `audio_generation.tts_model_name_or_path`: default `k2-fsa/OmniVoice`.

Speaker references can be provided as either:

```text
data/reference_speakers/
  speaker_01.wav
  speaker_01.txt
  speaker_02.wav
  speaker_02.txt
```

or:

```text
data/reference_speakers/references.csv
```

with columns:

```text
speaker_id,audio_path,ref_text
```

## Commands

Run from this folder:

```bash
python scripts/generate_texts.py --config configs/generation.yaml
python scripts/generate_audio.py --config configs/generation.yaml
python scripts/validate_dataset.py --data_dir data/
python scripts/make_hf_dataset.py --data_dir data/
```

To test the whole path with only one generated transcript and one TTS audio file:

```bash
python scripts/run_smoke_test.py
```

This writes to `data/smoke_test/` and does not overwrite the full dataset outputs.

You can also use the web UI from the repo root:

```bash
python3 viewer_server.py --host 0.0.0.0 --port 8000
```

Open **Darija Code-Switch Generator** to run the one-sample test or start a full generation job with a progress bar.

For a small smoke test:

```bash
python scripts/generate_texts.py --config configs/generation.yaml --num-texts 100 --batch-size 10
python scripts/generate_audio.py --config configs/generation.yaml --limit 20 --target-hours 0.05
```

## Outputs

```text
data/
  texts.jsonl
  metadata.csv
  dataset.jsonl
  audio/
    cs_000001.wav
    cs_000002.wav
  review_samples.csv
  bad_samples.csv
  hf_dataset/
```

The one-sample button in the viewer writes:

```text
data/smoke_test/
  texts.jsonl
  metadata.csv
  dataset.jsonl
  audio/
```

`metadata.csv` contains:

```text
file_name,text,source,domain,language_mix,speaker_id,is_synthetic
```

`dataset.jsonl` contains the same core fields plus split, duration, augmentation flags, and `is_temporary_synthetic_test` for synthetic test rows.

## Generation Design

Stage 1 uses an OpenAI-compatible chat API. The prompt forces mixed-script output:

- Darija in Arabic script.
- French and English in Latin script.
- No Arabized French/English words.
- Mostly short conversational phone-call utterances.

The generator targets this mix:

- 50% Darija + French
- 25% Darija + English
- 15% Darija + French + English
- 10% pure Darija

Rows are deduplicated and rejected when they are empty, too short, too long, missing Arabic script, missing Latin code-switching for mixed rows, too MSA-heavy, repeated, or likely to contain private numeric/email data.

Stage 2 uses the modular `BaseTTS` interface. The included backend is `OmniVoiceTTS`, matching:

```python
audio = model.generate(
    text=text,
    ref_audio=speaker_reference_audio,
    ref_text=speaker_reference_text,
)
```

The audio script distributes speaker references evenly, creates a second speaker version for about 25% of texts, trims silence, normalizes level, rejects bad files, and writes logs under `data/generation_logs/`.

Phone-call augmentation is train-only and light:

- 300-3400 Hz bandpass
- light compression
- light background noise
- small room reverb
- random volume gain
- optional mu-law style degradation

Validation and test splits stay clean. If you do not provide a real human code-switch test set, synthetic test rows are marked temporary.

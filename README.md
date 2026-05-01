# Darija Dataset Viewer

A portable local website for inspecting cleaned speech datasets in tidy tables with inline audio controls.

## Use It On Lightning

For your current Lightning dataset:

```bash
cd ~/datasets_viewer
python3 viewer_server.py /teamspace/studios/this_studio/darija_clean --host 0.0.0.0 --port 8000
```

Then open your Lightning forwarded port URL. The page will automatically read:

```text
/teamspace/studios/this_studio/darija_clean
```

and show the `train`, `val`, and `test` splits as separate tables.

If the dataset is huge and the page feels heavy, start with a preview:

```bash
python3 viewer_server.py /teamspace/studios/this_studio/darija_clean --host 0.0.0.0 --port 8000 --max-rows 2000
```

## Use Static Folder Mode

Open `index.html` in a browser, then choose **Open Folder** and select the folder that contains your dataset files and audio files. The viewer supports:

- Dataset files: `.csv`, `.tsv`, `.json`, `.jsonl`, `.ndjson`, `.txt`
- Audio files: `.wav`, `.mp3`, `.m4a`, `.ogg`, `.oga`, `.aac`, `.flac`, `.webm`
- Audio matching by full path, relative path, or filename
- Final dataset highlighting when a file path contains names like `final`, `cleaned`, or `gold`

For static cloud usage, move this whole folder to the cloud machine. You can either open `index.html` from the browser and pick a folder, or serve it:

```bash
python3 -m http.server 8000
```

Then open `http://localhost:8000`.

## Optional Manifest

If you serve the website and want it to load datasets automatically, create `viewer-manifest.json` beside `index.html`:

```json
{
  "audioRoot": "data/audio",
  "datasets": [
    {
      "name": "Final cleaned dataset",
      "path": "data/final_dataset.jsonl",
      "final": true
    },
    {
      "name": "Raw review split",
      "path": "data/raw_review.csv"
    }
  ]
}
```

The manifest is optional. Folder selection is usually easiest while cleaning.

## Parquet or Hugging Face Datasets

Browsers cannot read Parquet, Arrow, or Hugging Face `save_to_disk` folders directly without a conversion layer. On the cloud machine, use the helper to convert those into JSONL and create a manifest:

```bash
python3 tools/prepare_for_viewer.py /path/to/your/datasets --out viewer_data --manifest viewer-manifest.json
```

For Parquet, install `pandas` and `pyarrow` on the cloud machine. For Hugging Face datasets, install `datasets`.

Add `--copy-audio` if you also want the helper to copy audio files into `viewer_data/audio`.

## Synthetic Darija Code-Switch ASR Pipeline

This repo also includes `synthetic_cs_dataset/`, a two-stage pipeline for building a synthetic Moroccan phone-call ASR dataset for Whisper fine-tuning.

It generates transcripts in the target style:

```text
دارجة بالعربية + French/English in Latin script
```

Then it uses a modular TTS interface with an included OmniVoice backend to create clean WAV files, train-only phone-call augmentations, metadata, validation reports, and a Hugging Face `DatasetDict`.

Run it from the pipeline folder:

```bash
cd synthetic_cs_dataset
export LIGHTNING_API_KEY="..."
python scripts/generate_texts.py --config configs/generation.yaml
python scripts/generate_audio.py --config configs/generation.yaml
python scripts/validate_dataset.py --data_dir data/
python scripts/make_hf_dataset.py --data_dir data/
```

See `synthetic_cs_dataset/README.md` for the config fields, speaker reference format, quality checks, and output layout.

# Darija Dataset Viewer

A portable local website for inspecting cleaned speech datasets in tidy tables with inline audio controls.

## Use It

Open `index.html` in a browser, then choose **Open Folder** and select the folder that contains your dataset files and audio files. The viewer supports:

- Dataset files: `.csv`, `.tsv`, `.json`, `.jsonl`, `.ndjson`, `.txt`
- Audio files: `.wav`, `.mp3`, `.m4a`, `.ogg`, `.oga`, `.aac`, `.flac`, `.webm`
- Audio matching by full path, relative path, or filename
- Final dataset highlighting when a file path contains names like `final`, `cleaned`, or `gold`

For cloud usage, move this whole folder to the cloud machine. You can either open `index.html` from the browser and pick a folder, or serve it:

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

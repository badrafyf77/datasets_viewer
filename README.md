# Moroccan ASR Dataset Studio

A local web tool for two workflows:

- **Datasets Viewer**: load a server-side dataset path, choose a row preview limit, inspect rows, and play audio.
- **Darija Code-Switch Generator**: run a one-sample smoke test or launch the synthetic Darija/French/English dataset pipeline.

## Run The Studio

Use `viewer_server.py`. The generator and server-side dataset loader do not work with plain `python3 -m http.server`.

```bash
cd ~/datasets_viewer
python3 viewer_server.py --host 0.0.0.0 --port 8000
```

Then open your Lightning forwarded port URL.

Confirm the backend is active:

```text
http://localhost:8000/api/server-info
```

It should return JSON with `synthetic-test` and `generation-jobs` in `features`.

## Datasets Viewer

Open **Datasets Viewer**, set:

- `Dataset path`: for example `/teamspace/studios/this_studio/darija_clean`
- `Max rows per split`: `0` for all rows, or a preview limit like `2000`

Then click **Load Dataset**.

The viewer supports Hugging Face `save_to_disk` datasets and DatasetDict folders through the Python server. It shows split tables, row stats, exact-value filters for columns such as `language_mix`, searchable value lists, distribution bars, total/visible hours when a duration column is present, column controls, pagination, export, and inline audio when audio paths are available.

## Darija Code-Switch Generator

Open **Darija Code-Switch Generator**.

Use **Run 1-Sample Test** first. It creates:

```text
synthetic_cs_dataset/data/smoke_test/
  texts.jsonl
  metadata.csv
  dataset.jsonl
  audio/
```

For a full run, set the generation parameters and click **Generate Dataset**. The page polls the backend and shows a progress bar for text generation and audio generation. The default config now favors faster 5k-style runs: transcript batches run in parallel, and extra phone augmentation is off unless you raise its probability.

After creating multiple `hf_dataset/` folders, use **Merge HF Datasets** on the same page. Put one `save_to_disk` folder path per line, choose the merged output folder, then click **Merge HF Datasets**. If you open the site from your laptop through a cloud forwarded port, these paths are still paths on the cloud machine running `viewer_server.py`.

The **Push To Hugging Face** form can push any server-side `hf_dataset` path. Click **Load Columns** to choose which columns to upload; leaving the columns field empty pushes all columns.

Before pushing, put your token in the cloud/server environment. The easiest project-local option is:

```bash
cp synthetic_cs_dataset/.env.example synthetic_cs_dataset/.env
```

Then edit `synthetic_cs_dataset/.env`:

```bash
HF_TOKEN=hf_...
```

You can also export it before starting `viewer_server.py`:

```bash
export HF_TOKEN=...
```

Or authenticate the same cloud environment with:

```bash
huggingface-cli login
```

Required before generation:

```bash
cp synthetic_cs_dataset/.env.example synthetic_cs_dataset/.env
```

Then edit `synthetic_cs_dataset/.env`:

```bash
LIGHTNING_API_KEY=...
```

The generator loads `synthetic_cs_dataset/.env` automatically. If you change the key while the web UI is running, restart `viewer_server.py`.

Also make sure OmniVoice, torch, and the Python packages in `synthetic_cs_dataset/requirements.txt` are installed in the environment running `viewer_server.py`.

## CLI Pipeline

You can still run the synthetic pipeline directly:

```bash
cd synthetic_cs_dataset
python scripts/generate_texts.py --config configs/generation.yaml
python scripts/generate_audio.py --config configs/generation.yaml
python scripts/validate_dataset.py --data_dir data/
python scripts/make_hf_dataset.py --data_dir data/
```

One-sample CLI smoke test:

```bash
python scripts/run_smoke_test.py
```

See `synthetic_cs_dataset/README.md` for the config fields, speaker reference format, quality checks, and output layout.

## Dataset Conversion Helper

Browsers cannot read Parquet, Arrow, or Hugging Face `save_to_disk` folders directly without a conversion layer. If you need JSONL files for separate review or sharing, use:

```bash
python3 tools/prepare_for_viewer.py /path/to/your/datasets --out viewer_data --manifest viewer-manifest.json
```

For Parquet, install `pandas` and `pyarrow`. For Hugging Face datasets, install `datasets`.

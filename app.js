const DATA_EXTENSIONS = new Set(["csv", "tsv", "json", "jsonl", "ndjson", "txt"]);
const AUDIO_EXTENSIONS = new Set([
  "wav",
  "mp3",
  "m4a",
  "ogg",
  "oga",
  "aac",
  "flac",
  "webm",
]);

const AUDIO_NAME_RE =
  /(^|[._-])(audio|audios|sound|sounds|recording|recordings|voice|clip|clips|wav|mp3|flac|m4a|ogg|path|file|filename|filepath)($|[._-])/i;
const AUDIO_PATH_RE = /\.(wav|mp3|m4a|ogg|oga|aac|flac|webm)(\?.*)?$/i;
const LONG_TEXT_RE =
  /(text|sentence|transcript|transcription|translation|normalized|clean|raw|comment|notes|prompt|darija|arabic|english|francais|french)/i;
const DURATION_NAME_RE =
  /(^|[._-])(duration|duration_seconds|duration_s|duration_ms|seconds|secs|length_seconds|audio_duration|audio_length)([._-]|$)/i;
const MULTI_VALUE_RE = /\s*(?:\||;|,)\s*/;
const EMPTY_FACET_KEY = "__empty__";

const state = {
  datasets: [],
  audioFiles: new Map(),
  objectUrls: new Map(),
  activeId: null,
  search: "",
  audioFilter: "all",
  focusedColumn: "",
  facetColumn: "",
  facetMode: "exact",
  facetSearch: "",
  exactFilters: new Map(),
  distributionColumn: "",
  distributionMetric: "count",
  pageSize: 50,
  page: 1,
  sort: { column: null, direction: "asc" },
  activePage: "viewer",
  serverAvailable: false,
  syntheticTestAvailable: false,
  hfImportAvailable: false,
  hfDatasetToolsAvailable: false,
  datasetCleanerAvailable: false,
  generationJobId: null,
  generationPollTimer: null,
  hfImportJobId: null,
  hfImportPollTimer: null,
  hfMergeJobId: null,
  hfMergePollTimer: null,
  hfPushJobId: null,
  hfPushPollTimer: null,
  cleanerJobId: null,
  cleanerPollTimer: null,
  mergedHfDatasetPath: "",
  hfPushColumns: [],
};

const els = {
  navViewer: document.getElementById("navViewer"),
  navGenerator: document.getElementById("navGenerator"),
  navCleaner: document.getElementById("navCleaner"),
  datasetPage: document.getElementById("datasetPage"),
  generatorPage: document.getElementById("generatorPage"),
  cleanerPage: document.getElementById("cleanerPage"),
  messagePanel: document.getElementById("messagePanel"),
  messageType: document.getElementById("messageType"),
  messageTitle: document.getElementById("messageTitle"),
  messageText: document.getElementById("messageText"),
  messageDetails: document.getElementById("messageDetails"),
  messageClose: document.getElementById("messageClose"),
  serverStatusDot: document.getElementById("serverStatusDot"),
  serverInfoText: document.getElementById("serverInfoText"),
  datasetLoadForm: document.getElementById("datasetLoadForm"),
  datasetPathInput: document.getElementById("datasetPathInput"),
  maxRowsInput: document.getElementById("maxRowsInput"),
  loadDatasetButton: document.getElementById("loadDatasetButton"),
  hfImportForm: document.getElementById("hfImportForm"),
  hfImportRepoInput: document.getElementById("hfImportRepoInput"),
  hfImportConfigInput: document.getElementById("hfImportConfigInput"),
  hfImportSplitInput: document.getElementById("hfImportSplitInput"),
  hfImportRevisionInput: document.getElementById("hfImportRevisionInput"),
  hfImportOutputInput: document.getElementById("hfImportOutputInput"),
  hfImportOverwriteInput: document.getElementById("hfImportOverwriteInput"),
  hfImportTrustRemoteCodeInput: document.getElementById("hfImportTrustRemoteCodeInput"),
  importHfDatasetButton: document.getElementById("importHfDatasetButton"),
  hfImportProgressCard: document.getElementById("hfImportProgressCard"),
  hfImportProgressLabel: document.getElementById("hfImportProgressLabel"),
  hfImportProgressPercent: document.getElementById("hfImportProgressPercent"),
  hfImportProgressFill: document.getElementById("hfImportProgressFill"),
  hfImportProgressDetails: document.getElementById("hfImportProgressDetails"),
  fileInput: document.getElementById("fileInput"),
  filePickerInput: document.getElementById("filePickerInput"),
  folderInput: document.getElementById("folderInput"),
  dropzone: document.getElementById("dropzone"),
  manifestButton: document.getElementById("manifestButton"),
  syntheticTestButton: document.getElementById("syntheticTestButton"),
  datasetList: document.getElementById("datasetList"),
  datasetCount: document.getElementById("datasetCount"),
  audioCount: document.getElementById("audioCount"),
  audioHint: document.getElementById("audioHint"),
  emptyState: document.getElementById("emptyState"),
  viewer: document.getElementById("viewer"),
  datasetKind: document.getElementById("datasetKind"),
  datasetTitle: document.getElementById("datasetTitle"),
  datasetSubtitle: document.getElementById("datasetSubtitle"),
  statsGrid: document.getElementById("statsGrid"),
  searchInput: document.getElementById("searchInput"),
  pageSizeSelect: document.getElementById("pageSizeSelect"),
  audioFilter: document.getElementById("audioFilter"),
  columnFocus: document.getElementById("columnFocus"),
  facetColumn: document.getElementById("facetColumn"),
  facetMode: document.getElementById("facetMode"),
  facetSearch: document.getElementById("facetSearch"),
  activeFilters: document.getElementById("activeFilters"),
  facetValueList: document.getElementById("facetValueList"),
  clearExactFilters: document.getElementById("clearExactFilters"),
  distributionColumn: document.getElementById("distributionColumn"),
  distributionMetric: document.getElementById("distributionMetric"),
  distributionChart: document.getElementById("distributionChart"),
  columnToggles: document.getElementById("columnToggles"),
  showAllColumns: document.getElementById("showAllColumns"),
  hideLongColumns: document.getElementById("hideLongColumns"),
  tableHead: document.getElementById("tableHead"),
  tableBody: document.getElementById("tableBody"),
  prevPage: document.getElementById("prevPage"),
  nextPage: document.getElementById("nextPage"),
  pageStatus: document.getElementById("pageStatus"),
  exportButton: document.getElementById("exportButton"),
  smokeReferenceAudioInput: document.getElementById("smokeReferenceAudioInput"),
  smokeReferenceTextInput: document.getElementById("smokeReferenceTextInput"),
  smokeResult: document.getElementById("smokeResult"),
  generatorForm: document.getElementById("generatorForm"),
  generatorConfigInput: document.getElementById("generatorConfigInput"),
  generatorNumTextsInput: document.getElementById("generatorNumTextsInput"),
  generatorBatchSizeInput: document.getElementById("generatorBatchSizeInput"),
  generatorTargetHoursInput: document.getElementById("generatorTargetHoursInput"),
  generatorAugProbInput: document.getElementById("generatorAugProbInput"),
  generatorOutputDirInput: document.getElementById("generatorOutputDirInput"),
  generatorReferenceDirInput: document.getElementById("generatorReferenceDirInput"),
  generateDatasetButton: document.getElementById("generateDatasetButton"),
  generationProgressCard: document.getElementById("generationProgressCard"),
  generationProgressLabel: document.getElementById("generationProgressLabel"),
  generationProgressPercent: document.getElementById("generationProgressPercent"),
  generationProgressFill: document.getElementById("generationProgressFill"),
  generationProgressDetails: document.getElementById("generationProgressDetails"),
  hfMergeForm: document.getElementById("hfMergeForm"),
  hfDatasetPathsInput: document.getElementById("hfDatasetPathsInput"),
  hfMergeOutputInput: document.getElementById("hfMergeOutputInput"),
  hfOverwriteInput: document.getElementById("hfOverwriteInput"),
  mergeHfDatasetsButton: document.getElementById("mergeHfDatasetsButton"),
  hfMergeProgressCard: document.getElementById("hfMergeProgressCard"),
  hfMergeProgressLabel: document.getElementById("hfMergeProgressLabel"),
  hfMergeProgressPercent: document.getElementById("hfMergeProgressPercent"),
  hfMergeProgressFill: document.getElementById("hfMergeProgressFill"),
  hfMergeProgressDetails: document.getElementById("hfMergeProgressDetails"),
  hfPushPanel: document.getElementById("hfPushPanel"),
  hfPushForm: document.getElementById("hfPushForm"),
  hfPushDatasetPathInput: document.getElementById("hfPushDatasetPathInput"),
  hfRepoIdInput: document.getElementById("hfRepoIdInput"),
  hfPushColumnsInput: document.getElementById("hfPushColumnsInput"),
  loadHfColumnsButton: document.getElementById("loadHfColumnsButton"),
  hfColumnToggles: document.getElementById("hfColumnToggles"),
  hfPrivateRepoInput: document.getElementById("hfPrivateRepoInput"),
  pushHfDatasetButton: document.getElementById("pushHfDatasetButton"),
  hfPushProgressCard: document.getElementById("hfPushProgressCard"),
  hfPushProgressLabel: document.getElementById("hfPushProgressLabel"),
  hfPushProgressPercent: document.getElementById("hfPushProgressPercent"),
  hfPushProgressFill: document.getElementById("hfPushProgressFill"),
  hfPushProgressDetails: document.getElementById("hfPushProgressDetails"),
  cleanerForm: document.getElementById("cleanerForm"),
  cleanerDatasetPathInput: document.getElementById("cleanerDatasetPathInput"),
  cleanerTranscriptColumnInput: document.getElementById("cleanerTranscriptColumnInput"),
  cleanerAudioColumnInput: document.getElementById("cleanerAudioColumnInput"),
  cleanerWhisperModelInput: document.getElementById("cleanerWhisperModelInput"),
  cleanerLanguageInput: document.getElementById("cleanerLanguageInput"),
  cleanerCerThresholdInput: document.getElementById("cleanerCerThresholdInput"),
  cleanerMinCpsInput: document.getElementById("cleanerMinCpsInput"),
  cleanerMaxCpsInput: document.getElementById("cleanerMaxCpsInput"),
  cleanerUseWhisperInput: document.getElementById("cleanerUseWhisperInput"),
  cleanerUseCpsInput: document.getElementById("cleanerUseCpsInput"),
  cleanerSaveModeInput: document.getElementById("cleanerSaveModeInput"),
  cleanerOutputPathField: document.getElementById("cleanerOutputPathField"),
  cleanerOutputPathInput: document.getElementById("cleanerOutputPathInput"),
  cleanerOverwriteOutputField: document.getElementById("cleanerOverwriteOutputField"),
  cleanerOverwriteOutputInput: document.getElementById("cleanerOverwriteOutputInput"),
  runCleanerButton: document.getElementById("runCleanerButton"),
  cleanerProgressCard: document.getElementById("cleanerProgressCard"),
  cleanerProgressLabel: document.getElementById("cleanerProgressLabel"),
  cleanerProgressPercent: document.getElementById("cleanerProgressPercent"),
  cleanerProgressFill: document.getElementById("cleanerProgressFill"),
  cleanerProgressDetails: document.getElementById("cleanerProgressDetails"),
  cleanerResult: document.getElementById("cleanerResult"),
  toast: document.getElementById("toast"),
};

function getExtension(name = "") {
  const clean = String(name).split("?")[0].split("#")[0];
  const dot = clean.lastIndexOf(".");
  return dot >= 0 ? clean.slice(dot + 1).toLowerCase() : "";
}

function normalizePath(value = "") {
  return safeDecodeURIComponent(String(value))
    .trim()
    .replace(/^["']|["']$/g, "")
    .replace(/\\/g, "/")
    .replace(/^\.\//, "")
    .replace(/\/+/g, "/");
}

function safeDecodeURIComponent(value) {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}

function basename(value = "") {
  return normalizePath(value).split("/").filter(Boolean).pop() || normalizePath(value);
}

function dirname(value = "") {
  const parts = normalizePath(value).split("/").filter(Boolean);
  parts.pop();
  return parts.join("/");
}

function joinUrl(...parts) {
  const cleaned = parts
    .filter((part) => part !== undefined && part !== null && String(part).trim() !== "")
    .map((part) => String(part).replace(/\\/g, "/"));

  if (!cleaned.length) return "";
  const first = cleaned.shift();
  const prefix = /^https?:\/\//i.test(first) ? first.replace(/\/+$/, "") : first.replace(/\/+$/, "");
  const tail = cleaned.map((part) => part.replace(/^\/+|\/+$/g, "")).filter(Boolean);
  return [prefix, ...tail].join("/");
}

function getFilePath(file) {
  return normalizePath(file.webkitRelativePath || file.name || "");
}

function formatNumber(value) {
  return new Intl.NumberFormat().format(value || 0);
}

function showToast(message) {
  if (!els.toast) return;
  els.toast.textContent = message;
  els.toast.classList.add("is-visible");
  window.clearTimeout(showToast.timer);
  showToast.timer = window.setTimeout(() => {
    els.toast.classList.remove("is-visible");
  }, 3600);
}

function showMessage(type, title, text, details = "") {
  if (!els.messagePanel) return;
  els.messagePanel.className = `message-panel ${type || ""}`.trim();
  els.messagePanel.hidden = false;
  els.messageType.textContent = type === "success" ? "Success" : type === "info" ? "Info" : "Error";
  els.messageTitle.textContent = title;
  els.messageText.textContent = text;
  if (details) {
    els.messageDetails.hidden = false;
    els.messageDetails.textContent = details;
  } else {
    els.messageDetails.hidden = true;
    els.messageDetails.textContent = "";
  }
}

function showError(title, error, details = "") {
  const message = error instanceof Error ? error.message : String(error || "Something went wrong.");
  showMessage("error", title, message, details);
}

function clearMessage() {
  if (els.messagePanel) els.messagePanel.hidden = true;
}

function switchPage(page) {
  state.activePage = page;
  els.datasetPage.hidden = page !== "viewer";
  els.generatorPage.hidden = page !== "generator";
  if (els.cleanerPage) els.cleanerPage.hidden = page !== "cleaner";
  els.navViewer.classList.toggle("is-active", page === "viewer");
  els.navGenerator.classList.toggle("is-active", page === "generator");
  if (els.navCleaner) els.navCleaner.classList.toggle("is-active", page === "cleaner");
}

async function loadServerDataset({ silent = false, path = "", maxRows = 0 } = {}) {
  try {
    const params = new URLSearchParams();
    if (path) params.set("path", path);
    if (maxRows !== undefined && maxRows !== null) params.set("max_rows", String(maxRows));
    const url = `./api/datasets${params.toString() ? `?${params}` : ""}`;
    const response = await fetch(url, { cache: "no-store" });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
    const entries = Array.isArray(payload.datasets) ? payload.datasets : [];
    if (!entries.length) throw new Error("No datasets returned by server.");

    state.datasets = state.datasets.filter((dataset) => dataset.source !== "server");
    state.activeId = null;

    for (const entry of entries) {
      addDataset({
        name: entry.name || readableName(entry.path || "dataset"),
        path: entry.path || payload.path || "server dataset",
        source: "server",
        baseDir: "",
        records: entry.records || [],
        totalRows: Number(entry.rows || 0),
        shownRows: Number(entry.shownRows || (entry.records || []).length),
        isFinal: Boolean(entry.final) || isFinalDataset(entry.path || payload.path || ""),
      });
    }

    chooseDefaultDataset();
    render();
    if (!silent) showMessage("success", "Dataset loaded", `Loaded ${formatNumber(entries.length)} split(s) from ${payload.path}.`);
    return true;
  } catch (error) {
    console.info("Dataset API unavailable:", error);
    if (!silent) {
      showError(
        "Could not load dataset",
        error,
        "Make sure you are running `python3 viewer_server.py --host 0.0.0.0 --port 8000`, then check the dataset path.",
      );
    }
    return false;
  }
}

async function detectServerFeatures() {
  try {
    const response = await fetch("./api/server-info", { cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const payload = await response.json();
    const features = Array.isArray(payload.features) ? payload.features : [];
    state.serverAvailable = true;
    state.syntheticTestAvailable = features.includes("synthetic-test");
    state.hfImportAvailable = features.includes("hf-dataset-import");
    state.hfDatasetToolsAvailable = features.includes("hf-dataset-merge") && features.includes("hf-dataset-push");
    state.datasetCleanerAvailable = features.includes("dataset-cleaner");
    if (payload.default_dataset_path && els.datasetPathInput) {
      els.datasetPathInput.value = payload.default_dataset_path;
    }
    if (payload.default_dataset_path && els.cleanerDatasetPathInput) {
      els.cleanerDatasetPathInput.value = payload.default_dataset_path;
    }
    if (payload.default_max_rows !== undefined && els.maxRowsInput) {
      els.maxRowsInput.value = String(payload.default_max_rows);
    }
    if (els.serverInfoText) {
      els.serverInfoText.textContent = "Backend ready. Dataset loading, generation, and cleaning actions are available.";
    }
  } catch {
    state.serverAvailable = false;
    state.syntheticTestAvailable = false;
    state.hfImportAvailable = false;
    state.hfDatasetToolsAvailable = false;
    state.datasetCleanerAvailable = false;
    if (els.serverInfoText) {
      els.serverInfoText.textContent = "Static mode. Use viewer_server.py to load server paths, generate audio, or clean datasets.";
    }
  }

  if (els.serverStatusDot) els.serverStatusDot.classList.toggle("is-online", state.serverAvailable);
  if (els.syntheticTestButton) {
    els.syntheticTestButton.disabled = !state.syntheticTestAvailable;
    els.syntheticTestButton.title = state.syntheticTestAvailable
      ? "Generate one transcript and one audio file."
      : "Requires python3 viewer_server.py; static http.server cannot run LLM/TTS generation.";
  }
  if (els.loadDatasetButton) {
    els.loadDatasetButton.disabled = !state.serverAvailable;
    els.loadDatasetButton.title = state.serverAvailable
      ? "Load this server-side dataset path."
      : "Requires python3 viewer_server.py.";
  }
  if (els.importHfDatasetButton) {
    els.importHfDatasetButton.disabled = !state.hfImportAvailable;
    els.importHfDatasetButton.title = state.hfImportAvailable
      ? "Import this Hugging Face dataset to a local save_to_disk folder."
      : "Requires python3 viewer_server.py.";
  }
  if (els.generateDatasetButton) {
    els.generateDatasetButton.disabled = !state.syntheticTestAvailable;
    els.generateDatasetButton.title = state.syntheticTestAvailable
      ? "Start full generation."
      : "Requires python3 viewer_server.py.";
  }
  if (els.mergeHfDatasetsButton) {
    els.mergeHfDatasetsButton.disabled = !state.hfDatasetToolsAvailable;
    els.mergeHfDatasetsButton.title = state.hfDatasetToolsAvailable
      ? "Merge saved Hugging Face dataset folders."
      : "Requires python3 viewer_server.py.";
  }
  if (els.pushHfDatasetButton) {
    els.pushHfDatasetButton.disabled = !state.hfDatasetToolsAvailable;
    els.pushHfDatasetButton.title = state.hfDatasetToolsAvailable
      ? "Push the merged dataset to Hugging Face."
      : "Requires python3 viewer_server.py.";
  }
  if (els.loadHfColumnsButton) {
    els.loadHfColumnsButton.disabled = !state.hfDatasetToolsAvailable;
    els.loadHfColumnsButton.title = state.hfDatasetToolsAvailable
      ? "Load columns from this Hugging Face dataset path."
      : "Requires python3 viewer_server.py.";
  }
  if (els.runCleanerButton) {
    els.runCleanerButton.disabled = !state.datasetCleanerAvailable;
    els.runCleanerButton.title = state.datasetCleanerAvailable
      ? "Run bad-sample removal."
      : "Requires python3 viewer_server.py.";
  }
}

async function loadDatasetFromForm(event) {
  event.preventDefault();
  clearMessage();
  if (!state.serverAvailable) {
    showError(
      "Dataset loader unavailable",
      "This page is running in static mode.",
      "Start it with `python3 viewer_server.py --host 0.0.0.0 --port 8000`.",
    );
    return;
  }

  const path = els.datasetPathInput.value.trim();
  const maxRows = Number(els.maxRowsInput.value || 0);
  if (!path) {
    showError("Missing dataset path", "Enter the dataset folder path before loading.");
    return;
  }

  els.loadDatasetButton.disabled = true;
  els.loadDatasetButton.textContent = "Loading...";
  setBusy(true, "Loading dataset...");
  try {
    await loadServerDataset({ path, maxRows, silent: false });
  } finally {
    els.loadDatasetButton.disabled = false;
    els.loadDatasetButton.textContent = "Load Dataset";
    setBusy(false);
  }
}

function hfImportParamsFromForm() {
  return {
    repo_id: els.hfImportRepoInput?.value.trim() || "",
    config_name: els.hfImportConfigInput?.value.trim() || "",
    split: els.hfImportSplitInput?.value.trim() || "",
    revision: els.hfImportRevisionInput?.value.trim() || "",
    output_path: els.hfImportOutputInput?.value.trim() || "",
    overwrite: Boolean(els.hfImportOverwriteInput?.checked),
    trust_remote_code: Boolean(els.hfImportTrustRemoteCodeInput?.checked),
  };
}

function setHfImportProgress(status = {}) {
  setProgressElements(
    {
      card: els.hfImportProgressCard,
      fill: els.hfImportProgressFill,
      percent: els.hfImportProgressPercent,
      label: els.hfImportProgressLabel,
      details: els.hfImportProgressDetails,
    },
    status,
  );
}

async function startHfDatasetImport(event) {
  event.preventDefault();
  clearMessage();
  if (!state.hfImportAvailable) {
    showError(
      "Hugging Face import unavailable",
      "Importing from Hugging Face needs the Python backend.",
      "Start the app with `python3 viewer_server.py --host 0.0.0.0 --port 8000`.",
    );
    return;
  }

  const params = hfImportParamsFromForm();
  if (!params.repo_id || /\s/.test(params.repo_id)) {
    showError("Invalid dataset repo", "Enter a Hugging Face dataset repo id like `owner/name`.");
    return;
  }

  window.clearInterval(state.hfImportPollTimer);
  state.hfImportJobId = null;
  els.importHfDatasetButton.disabled = true;
  els.importHfDatasetButton.textContent = "Importing...";
  setHfImportProgress({ percent: 1, stage: "Starting", message: "Preparing the Hugging Face import..." });

  try {
    const response = await fetch("./api/hf/import/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(params),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || !payload.ok) throw new Error(payload.error || `HTTP ${response.status}`);
    state.hfImportJobId = payload.job_id;
    pollHfImportStatus();
    state.hfImportPollTimer = window.setInterval(pollHfImportStatus, 2000);
  } catch (error) {
    els.importHfDatasetButton.disabled = false;
    els.importHfDatasetButton.textContent = "Import Dataset";
    showError("Could not start Hugging Face import", error);
  }
}

async function pollHfImportStatus() {
  if (!state.hfImportJobId) return;
  try {
    const response = await fetch(`./api/hf/status?id=${encodeURIComponent(state.hfImportJobId)}`, {
      cache: "no-store",
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || !payload.ok) throw new Error(payload.error || `HTTP ${response.status}`);
    setHfImportProgress(payload.job);

    if (payload.job.status === "completed") {
      window.clearInterval(state.hfImportPollTimer);
      els.importHfDatasetButton.disabled = false;
      els.importHfDatasetButton.textContent = "Import Dataset";

      const result = payload.job.result || {};
      const outputPath = result.output_path || "";
      if (outputPath) {
        if (els.datasetPathInput) els.datasetPathInput.value = outputPath;
        if (els.hfImportOutputInput && !els.hfImportOutputInput.value.trim()) {
          els.hfImportOutputInput.value = outputPath;
        }
      }

      const maxRows = Number(els.maxRowsInput?.value || 0);
      const loaded = outputPath ? await loadServerDataset({ path: outputPath, maxRows, silent: true }) : false;
      const splitSummary = formatRowsBySplit(result.rows_by_split);
      if (loaded) {
        switchPage("viewer");
        showMessage(
          "success",
          "Hugging Face dataset imported",
          splitSummary
            ? `Saved locally and loaded into the viewer. Splits: ${splitSummary}.`
            : "Saved locally and loaded into the viewer.",
        );
      } else {
        showError("Imported but could not visualize", "The dataset was saved locally, but the viewer could not load it.", outputPath);
      }
    } else if (payload.job.status === "failed") {
      window.clearInterval(state.hfImportPollTimer);
      els.importHfDatasetButton.disabled = false;
      els.importHfDatasetButton.textContent = "Import Dataset";
      showError("Hugging Face import failed", payload.job.error || "The import job failed.", payload.job.log_tail || "");
    }
  } catch (error) {
    window.clearInterval(state.hfImportPollTimer);
    els.importHfDatasetButton.disabled = false;
    els.importHfDatasetButton.textContent = "Import Dataset";
    showError("Could not read Hugging Face import status", error);
  }
}

async function runSyntheticTest() {
  if (!els.syntheticTestButton) return;
  if (!state.syntheticTestAvailable) {
    showError(
      "Generator unavailable",
      "Run the viewer with python3 viewer_server.py to enable the 1-sample test.",
    );
    return;
  }
  const previousText = els.syntheticTestButton.textContent.trim() || "Run 1-Sample Test";
  els.syntheticTestButton.disabled = true;
  els.syntheticTestButton.textContent = "Testing...";
  setBusy(true, "Generating one transcript and one TTS audio file...");

  try {
    const referenceAudio = els.smokeReferenceAudioInput?.value.trim() || "";
    const referenceText = els.smokeReferenceTextInput?.value.trim() || "";
    if (!referenceAudio || !referenceText) {
      throw new Error("Reference audio path and reference text are required for OmniVoice.");
    }

    const response = await fetch("./api/synthetic-test", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      cache: "no-store",
      body: JSON.stringify({
        reference_audio: referenceAudio,
        reference_text: referenceText,
      }),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || !payload.ok) {
      if (response.status === 404 || response.status === 501) {
        throw new Error(
          "Synthetic test endpoint is not available. Restart with python3 viewer_server.py, not python3 -m http.server.",
        );
      }
      throw new Error(payload.error || `HTTP ${response.status}`);
    }

    const dataset = payload.dataset;
    if (!dataset || !Array.isArray(dataset.records) || !dataset.records.length) {
      throw new Error("Smoke test finished without returning a preview row.");
    }

    state.datasets = state.datasets.filter((item) => item.source !== "synthetic-test");
    addDataset({
      name: dataset.name || "Synthetic smoke test",
      path: dataset.path || payload.data_dir || "synthetic smoke test",
      source: "synthetic-test",
      baseDir: "",
      records: dataset.records,
      isFinal: false,
    });
    state.activeId = state.datasets[state.datasets.length - 1].id;
    state.page = 1;
    state.sort = { column: null, direction: "asc" };
    switchPage("viewer");
    render();
    if (els.smokeResult) {
      els.smokeResult.hidden = false;
      els.smokeResult.textContent = `Generated: ${payload.text || "one sample"}\nAudio: ${payload.audio_path || ""}`;
    }
    showMessage("success", "1-sample test completed", "Generated one synthetic text and audio sample. It is loaded in the dataset viewer.");
  } catch (error) {
    console.error(error);
    showError("Synthetic test failed", error);
  } finally {
    els.syntheticTestButton.disabled = false;
    els.syntheticTestButton.textContent = previousText;
    setBusy(false);
  }
}

function generationParamsFromForm() {
  return {
    config_path: els.generatorConfigInput.value.trim(),
    num_texts: Number(els.generatorNumTextsInput.value || 0),
    batch_size: Number(els.generatorBatchSizeInput.value || 0),
    target_hours: Number(els.generatorTargetHoursInput.value || 0),
    augmentation_probability: Number(els.generatorAugProbInput.value || 0),
    output_dir: els.generatorOutputDirInput.value.trim(),
    reference_speakers_dir: els.generatorReferenceDirInput.value.trim(),
  };
}

function setGenerationProgress(status = {}) {
  if (!els.generationProgressCard) return;
  const percent = Math.max(0, Math.min(100, Number(status.percent || 0)));
  els.generationProgressCard.hidden = false;
  els.generationProgressFill.style.width = `${percent}%`;
  els.generationProgressPercent.textContent = `${Math.round(percent)}%`;
  els.generationProgressLabel.textContent = status.stage || status.status || "Generation";
  els.generationProgressDetails.textContent = status.message || "Waiting for job status...";
}

async function startGeneration(event) {
  event.preventDefault();
  clearMessage();
  if (!state.syntheticTestAvailable) {
    showError(
      "Generator unavailable",
      "Full generation needs the Python backend.",
      "Start the app with `python3 viewer_server.py --host 0.0.0.0 --port 8000`.",
    );
    return;
  }

  const params = generationParamsFromForm();
  if (!params.config_path || !params.output_dir || !params.reference_speakers_dir) {
    showError("Missing generator parameter", "Config path, output folder, and reference speaker folder are required.");
    return;
  }
  if (params.num_texts < 1 || params.num_texts > 30000) {
    showError("Invalid text count", "Number of texts must be between 1 and 30000.");
    return;
  }

  els.generateDatasetButton.disabled = true;
  els.generateDatasetButton.textContent = "Starting...";
  setGenerationProgress({ percent: 1, stage: "Starting", message: "Preparing the generation job..." });

  try {
    const response = await fetch("./api/generation/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(params),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || !payload.ok) throw new Error(payload.error || `HTTP ${response.status}`);
    state.generationJobId = payload.job_id;
    pollGenerationStatus();
    window.clearInterval(state.generationPollTimer);
    state.generationPollTimer = window.setInterval(pollGenerationStatus, 2000);
  } catch (error) {
    els.generateDatasetButton.disabled = false;
    els.generateDatasetButton.textContent = "Generate Dataset";
    showError("Could not start generation", error);
  }
}

async function pollGenerationStatus() {
  if (!state.generationJobId) return;
  try {
    const response = await fetch(`./api/generation/status?id=${encodeURIComponent(state.generationJobId)}`, {
      cache: "no-store",
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || !payload.ok) throw new Error(payload.error || `HTTP ${response.status}`);
    setGenerationProgress(payload.job);

    if (payload.job.status === "completed") {
      window.clearInterval(state.generationPollTimer);
      els.generateDatasetButton.disabled = false;
      els.generateDatasetButton.textContent = "Generate Dataset";
      showMessage("success", "Generation completed", payload.job.message || "Dataset generation finished.");
    } else if (payload.job.status === "failed") {
      window.clearInterval(state.generationPollTimer);
      els.generateDatasetButton.disabled = false;
      els.generateDatasetButton.textContent = "Generate Dataset";
      showError("Generation failed", payload.job.error || "The generation job failed.", payload.job.log_tail || "");
    }
  } catch (error) {
    window.clearInterval(state.generationPollTimer);
    els.generateDatasetButton.disabled = false;
    els.generateDatasetButton.textContent = "Generate Dataset";
    showError("Could not read generation status", error);
  }
}

function datasetPathsFromText(value = "") {
  return String(value)
    .split(/\r?\n/)
    .map((path) => path.trim().replace(/^["']|["']$/g, ""))
    .filter(Boolean);
}

function hfMergeParamsFromForm() {
  return {
    dataset_paths: datasetPathsFromText(els.hfDatasetPathsInput?.value || ""),
    output_path: els.hfMergeOutputInput?.value.trim() || "",
    overwrite: Boolean(els.hfOverwriteInput?.checked),
  };
}

function setProgressElements(elements, status = {}) {
  if (!elements.card) return;
  const percent = Math.max(0, Math.min(100, Number(status.percent || 0)));
  elements.card.hidden = false;
  elements.fill.style.width = `${percent}%`;
  elements.percent.textContent = `${Math.round(percent)}%`;
  elements.label.textContent = status.stage || status.status || "Working";
  elements.details.textContent = status.message || "Waiting for job status...";
}

function setHfMergeProgress(status = {}) {
  setProgressElements(
    {
      card: els.hfMergeProgressCard,
      fill: els.hfMergeProgressFill,
      percent: els.hfMergeProgressPercent,
      label: els.hfMergeProgressLabel,
      details: els.hfMergeProgressDetails,
    },
    status,
  );
}

function setHfPushProgress(status = {}) {
  setProgressElements(
    {
      card: els.hfPushProgressCard,
      fill: els.hfPushProgressFill,
      percent: els.hfPushProgressPercent,
      label: els.hfPushProgressLabel,
      details: els.hfPushProgressDetails,
    },
    status,
  );
}

function formatRowsBySplit(rowsBySplit = {}) {
  const entries = Object.entries(rowsBySplit);
  if (!entries.length) return "";
  return entries.map(([split, rows]) => `${split}: ${formatNumber(rows)}`).join(", ");
}

function columnValuesFromText(value = "") {
  return String(value)
    .split(/\r?\n|,/)
    .map((column) => column.trim().replace(/^["']|["']$/g, ""))
    .filter(Boolean);
}

function setColumnsInput(columns) {
  if (!els.hfPushColumnsInput) return;
  els.hfPushColumnsInput.value = columns.join("\n");
}

function renderHfColumnToggles(columns = []) {
  if (!els.hfColumnToggles) return;
  els.hfColumnToggles.innerHTML = "";

  if (!columns.length) {
    const empty = document.createElement("p");
    empty.className = "dataset-empty";
    empty.textContent = "No columns loaded.";
    els.hfColumnToggles.append(empty);
    return;
  }

  const selected = new Set(columnValuesFromText(els.hfPushColumnsInput?.value || ""));
  const selectedOrAll = selected.size ? selected : new Set(columns);

  for (const column of columns) {
    const label = document.createElement("label");
    label.className = "column-toggle";

    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.checked = selectedOrAll.has(column);
    checkbox.addEventListener("change", () => {
      const checkedColumns = Array.from(els.hfColumnToggles.querySelectorAll("input:checked")).map(
        (input) => input.value,
      );
      if (!checkedColumns.length) {
        checkbox.checked = true;
        setColumnsInput([column]);
        showToast("Keep at least one column selected, or clear the columns field to push all columns.");
        return;
      }
      setColumnsInput(checkedColumns);
    });

    const text = document.createElement("span");
    text.textContent = column;

    checkbox.value = column;
    label.append(checkbox, text);
    els.hfColumnToggles.append(label);
  }

  setColumnsInput(Array.from(selectedOrAll).filter((column) => columns.includes(column)));
}

async function loadHfColumns({ silent = false } = {}) {
  if (!state.hfDatasetToolsAvailable) {
    if (!silent) {
      showError(
        "Column loader unavailable",
        "Column loading needs the Python backend.",
        "Start the app with `python3 viewer_server.py --host 0.0.0.0 --port 8000`.",
      );
    }
    return false;
  }

  const path = els.hfPushDatasetPathInput?.value.trim() || "";
  if (!path) {
    if (!silent) showError("Missing HF dataset path", "Enter the dataset path before loading columns.");
    return false;
  }

  if (els.loadHfColumnsButton) {
    els.loadHfColumnsButton.disabled = true;
    els.loadHfColumnsButton.textContent = "Loading...";
  }

  try {
    const response = await fetch(`./api/hf/columns?path=${encodeURIComponent(path)}`, { cache: "no-store" });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || !payload.ok) throw new Error(payload.error || `HTTP ${response.status}`);
    state.hfPushColumns = Array.isArray(payload.columns) ? payload.columns : [];
    renderHfColumnToggles(state.hfPushColumns);
    if (!silent) {
      const splitSummary = formatRowsBySplit(payload.rows_by_split);
      showMessage(
        "success",
        "Columns loaded",
        splitSummary
          ? `Loaded ${formatNumber(state.hfPushColumns.length)} common column(s). Splits: ${splitSummary}.`
          : `Loaded ${formatNumber(state.hfPushColumns.length)} common column(s).`,
      );
    }
    return true;
  } catch (error) {
    if (!silent) showError("Could not load columns", error);
    return false;
  } finally {
    if (els.loadHfColumnsButton) {
      els.loadHfColumnsButton.disabled = !state.hfDatasetToolsAvailable;
      els.loadHfColumnsButton.textContent = "Load Columns";
    }
  }
}

async function startHfDatasetMerge(event) {
  event.preventDefault();
  clearMessage();
  if (!state.hfDatasetToolsAvailable) {
    showError(
      "HF merge unavailable",
      "Merging Hugging Face datasets needs the Python backend.",
      "Start the app with `python3 viewer_server.py --host 0.0.0.0 --port 8000`.",
    );
    return;
  }

  const params = hfMergeParamsFromForm();
  if (params.dataset_paths.length < 2) {
    showError("Missing HF dataset paths", "Enter at least two `hf_dataset` folders, one per line.");
    return;
  }
  if (!params.output_path) {
    showError("Missing output folder", "Choose where the merged Hugging Face dataset should be saved.");
    return;
  }

  window.clearInterval(state.hfMergePollTimer);
  window.clearInterval(state.hfPushPollTimer);
  state.hfMergeJobId = null;
  state.hfPushJobId = null;
  state.mergedHfDatasetPath = "";
  if (els.pushHfDatasetButton) els.pushHfDatasetButton.disabled = !state.hfDatasetToolsAvailable;
  if (els.hfPushProgressCard) els.hfPushProgressCard.hidden = true;

  els.mergeHfDatasetsButton.disabled = true;
  els.mergeHfDatasetsButton.textContent = "Merging...";
  setHfMergeProgress({ percent: 1, stage: "Starting", message: "Preparing the merge job..." });

  try {
    const response = await fetch("./api/hf/merge/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(params),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || !payload.ok) throw new Error(payload.error || `HTTP ${response.status}`);
    state.hfMergeJobId = payload.job_id;
    pollHfMergeStatus();
    state.hfMergePollTimer = window.setInterval(pollHfMergeStatus, 1500);
  } catch (error) {
    els.mergeHfDatasetsButton.disabled = false;
    els.mergeHfDatasetsButton.textContent = "Merge HF Datasets";
    showError("Could not start HF merge", error);
  }
}

async function pollHfMergeStatus() {
  if (!state.hfMergeJobId) return;
  try {
    const response = await fetch(`./api/hf/status?id=${encodeURIComponent(state.hfMergeJobId)}`, {
      cache: "no-store",
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || !payload.ok) throw new Error(payload.error || `HTTP ${response.status}`);
    setHfMergeProgress(payload.job);

    if (payload.job.status === "completed") {
      window.clearInterval(state.hfMergePollTimer);
      els.mergeHfDatasetsButton.disabled = false;
      els.mergeHfDatasetsButton.textContent = "Merge HF Datasets";
      const result = payload.job.result || {};
      const outputPath = result.output_path || els.hfMergeOutputInput.value.trim();
      state.mergedHfDatasetPath = outputPath;
      if (els.hfPushDatasetPathInput) els.hfPushDatasetPathInput.value = outputPath;
      if (els.pushHfDatasetButton) els.pushHfDatasetButton.disabled = !state.hfDatasetToolsAvailable;
      await loadHfColumns({ silent: true });
      const splitSummary = formatRowsBySplit(result.rows_by_split);
      showMessage(
        "success",
        "HF datasets merged",
        splitSummary ? `${payload.job.message}. Splits: ${splitSummary}.` : payload.job.message,
      );
    } else if (payload.job.status === "failed") {
      window.clearInterval(state.hfMergePollTimer);
      els.mergeHfDatasetsButton.disabled = false;
      els.mergeHfDatasetsButton.textContent = "Merge HF Datasets";
      showError("HF dataset merge failed", payload.job.error || "The merge job failed.", payload.job.log_tail || "");
    }
  } catch (error) {
    window.clearInterval(state.hfMergePollTimer);
    els.mergeHfDatasetsButton.disabled = false;
    els.mergeHfDatasetsButton.textContent = "Merge HF Datasets";
    showError("Could not read HF merge status", error);
  }
}

function hfPushParamsFromForm() {
  return {
    dataset_path: els.hfPushDatasetPathInput?.value.trim() || state.mergedHfDatasetPath,
    repo_id: els.hfRepoIdInput?.value.trim() || "",
    columns: columnValuesFromText(els.hfPushColumnsInput?.value || ""),
    private: Boolean(els.hfPrivateRepoInput?.checked),
  };
}

async function startHfDatasetPush(event) {
  event.preventDefault();
  clearMessage();
  if (!state.hfDatasetToolsAvailable) {
    showError(
      "HF push unavailable",
      "Pushing to Hugging Face needs the Python backend.",
      "Start the app with `python3 viewer_server.py --host 0.0.0.0 --port 8000`.",
    );
    return;
  }

  const params = hfPushParamsFromForm();
  if (!params.dataset_path) {
    showError("Missing HF dataset path", "Enter a Hugging Face `save_to_disk` folder path.");
    return;
  }
  if (!params.repo_id || /\s/.test(params.repo_id)) {
    showError("Invalid repo id", "Enter a Hugging Face dataset repo id like `username/darija-code-switch-asr`.");
    return;
  }

  window.clearInterval(state.hfPushPollTimer);
  els.pushHfDatasetButton.disabled = true;
  els.pushHfDatasetButton.textContent = "Pushing...";
  setHfPushProgress({ percent: 1, stage: "Starting", message: "Preparing the Hugging Face push..." });

  try {
    const response = await fetch("./api/hf/push/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(params),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || !payload.ok) throw new Error(payload.error || `HTTP ${response.status}`);
    state.hfPushJobId = payload.job_id;
    pollHfPushStatus();
    state.hfPushPollTimer = window.setInterval(pollHfPushStatus, 2500);
  } catch (error) {
    els.pushHfDatasetButton.disabled = false;
    els.pushHfDatasetButton.textContent = "Push To Hugging Face";
    showError("Could not start Hugging Face push", error);
  }
}

async function pollHfPushStatus() {
  if (!state.hfPushJobId) return;
  try {
    const response = await fetch(`./api/hf/status?id=${encodeURIComponent(state.hfPushJobId)}`, {
      cache: "no-store",
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || !payload.ok) throw new Error(payload.error || `HTTP ${response.status}`);
    setHfPushProgress(payload.job);

    if (payload.job.status === "completed") {
      window.clearInterval(state.hfPushPollTimer);
      els.pushHfDatasetButton.disabled = false;
      els.pushHfDatasetButton.textContent = "Push To Hugging Face";
      const result = payload.job.result || {};
      showMessage("success", "Pushed to Hugging Face", payload.job.message || "Dataset pushed.", result.repo_url || "");
    } else if (payload.job.status === "failed") {
      window.clearInterval(state.hfPushPollTimer);
      els.pushHfDatasetButton.disabled = false;
      els.pushHfDatasetButton.textContent = "Push To Hugging Face";
      showError("Hugging Face push failed", payload.job.error || "The push job failed.", payload.job.log_tail || "");
    }
  } catch (error) {
    window.clearInterval(state.hfPushPollTimer);
    els.pushHfDatasetButton.disabled = false;
    els.pushHfDatasetButton.textContent = "Push To Hugging Face";
    showError("Could not read Hugging Face push status", error);
  }
}

function setCleanerSaveModeState() {
  const isOverwrite = els.cleanerSaveModeInput?.value === "overwrite";
  if (els.cleanerOutputPathField) els.cleanerOutputPathField.hidden = isOverwrite;
  if (els.cleanerOverwriteOutputField) els.cleanerOverwriteOutputField.hidden = isOverwrite;
}

function setCleanerProgress(status = {}) {
  setProgressElements(
    {
      card: els.cleanerProgressCard,
      fill: els.cleanerProgressFill,
      percent: els.cleanerProgressPercent,
      label: els.cleanerProgressLabel,
      details: els.cleanerProgressDetails,
    },
    status,
  );
}

function cleanerParamsFromForm() {
  return {
    dataset_path: els.cleanerDatasetPathInput?.value.trim() || "",
    transcript_column: els.cleanerTranscriptColumnInput?.value.trim() || "",
    audio_column: els.cleanerAudioColumnInput?.value.trim() || "",
    whisper_model: els.cleanerWhisperModelInput?.value.trim() || "large-v3-turbo",
    language: els.cleanerLanguageInput?.value.trim() || "ar",
    cer_threshold: Number(els.cleanerCerThresholdInput?.value || 0.6),
    min_cps: Number(els.cleanerMinCpsInput?.value || 5),
    max_cps: Number(els.cleanerMaxCpsInput?.value || 22),
    use_whisper: Boolean(els.cleanerUseWhisperInput?.checked),
    use_cps: Boolean(els.cleanerUseCpsInput?.checked),
    output_mode: els.cleanerSaveModeInput?.value || "copy",
    output_path: els.cleanerOutputPathInput?.value.trim() || "",
    overwrite_output: Boolean(els.cleanerOverwriteOutputInput?.checked),
  };
}

function formatSplitCounts(counts = {}) {
  const entries = Object.entries(counts);
  if (!entries.length) return "";
  return entries.map(([split, count]) => `${split}: ${formatNumber(count)}`).join(", ");
}

function renderCleanerResult(result = {}) {
  if (!els.cleanerResult) return;
  const removed = Number(result.removed_rows || 0);
  const total = Number(result.total_rows || 0);
  const kept = Number(result.kept_rows || 0);
  const outputPath = result.output_path || "";
  const reportPath = result.report_path || "";
  const removedBySplit = formatSplitCounts(result.removed_by_split);
  const preview = Array.isArray(result.bad_samples_preview) ? result.bad_samples_preview.slice(0, 8) : [];
  const previewLines = preview.map((sample) => {
    const row = sample.row !== undefined ? `row ${formatNumber(Number(sample.row) + 1)}` : "row ?";
    const split = sample.split || "split";
    const reason = sample.reason || "failed";
    const cer = sample.cer !== undefined ? `, CER ${sample.cer}` : "";
    const cps = sample.chars_per_second !== undefined ? `, ${sample.chars_per_second} chars/sec` : "";
    const id = sample.id ? `, ${sample.id}` : "";
    return `- ${split} ${row}${id}: ${reason}${cer}${cps}`;
  });

  els.cleanerResult.hidden = false;
  els.cleanerResult.textContent = [
    `Removed ${formatNumber(removed)} of ${formatNumber(total)} sample(s).`,
    `Kept ${formatNumber(kept)} sample(s).`,
    removedBySplit ? `Removed by split: ${removedBySplit}.` : "",
    outputPath ? `Saved dataset: ${outputPath}` : "",
    reportPath ? `Report: ${reportPath}` : "",
    previewLines.length ? `Preview:\n${previewLines.join("\n")}` : "",
  ]
    .filter(Boolean)
    .join("\n");
}

async function startDatasetCleaner(event) {
  event.preventDefault();
  clearMessage();
  if (!state.datasetCleanerAvailable) {
    showError(
      "Dataset cleaner unavailable",
      "Cleaning needs the Python backend.",
      "Start the app with `python3 viewer_server.py --host 0.0.0.0 --port 8000`.",
    );
    return;
  }

  const params = cleanerParamsFromForm();
  if (!params.dataset_path) {
    showError("Missing dataset path", "Enter the Hugging Face dataset folder path before cleaning.");
    return;
  }
  if (!params.use_whisper && !params.use_cps) {
    showError("No cleaner checks selected", "Enable Whisper CER, chars/sec, or both.");
    return;
  }
  if (params.min_cps > params.max_cps) {
    showError("Invalid chars/sec range", "Min chars/sec must be less than or equal to max chars/sec.");
    return;
  }
  if (params.output_mode === "copy" && !params.output_path) {
    showError("Missing output path", "Enter a destination for the cleaned dataset copy.");
    return;
  }
  if (params.output_mode === "overwrite") {
    const confirmed = window.confirm(`Override the original dataset at ${params.dataset_path}?`);
    if (!confirmed) return;
  }

  window.clearInterval(state.cleanerPollTimer);
  state.cleanerJobId = null;
  if (els.cleanerResult) els.cleanerResult.hidden = true;
  els.runCleanerButton.disabled = true;
  els.runCleanerButton.textContent = "Cleaning...";
  setCleanerProgress({ percent: 1, stage: "Starting", message: "Preparing the bad-sample removal job..." });

  try {
    const response = await fetch("./api/cleaner/bad-samples/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(params),
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || !payload.ok) throw new Error(payload.error || `HTTP ${response.status}`);
    state.cleanerJobId = payload.job_id;
    pollDatasetCleanerStatus();
    state.cleanerPollTimer = window.setInterval(pollDatasetCleanerStatus, 2000);
  } catch (error) {
    els.runCleanerButton.disabled = false;
    els.runCleanerButton.textContent = "Run Cleaner";
    showError("Could not start dataset cleaner", error);
  }
}

async function pollDatasetCleanerStatus() {
  if (!state.cleanerJobId) return;
  try {
    const response = await fetch(`./api/cleaner/status?id=${encodeURIComponent(state.cleanerJobId)}`, {
      cache: "no-store",
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || !payload.ok) throw new Error(payload.error || `HTTP ${response.status}`);
    setCleanerProgress(payload.job);

    if (payload.job.status === "completed") {
      window.clearInterval(state.cleanerPollTimer);
      els.runCleanerButton.disabled = false;
      els.runCleanerButton.textContent = "Run Cleaner";
      const result = payload.job.result || {};
      const outputPath = result.output_path || "";
      renderCleanerResult(result);
      if (outputPath) {
        if (els.datasetPathInput) els.datasetPathInput.value = outputPath;
        if (els.cleanerDatasetPathInput) els.cleanerDatasetPathInput.value = outputPath;
        const maxRows = Number(els.maxRowsInput?.value || 0);
        await loadServerDataset({ path: outputPath, maxRows, silent: true });
      }
      showMessage(
        "success",
        "Dataset cleaner completed",
        `Removed ${formatNumber(result.removed_rows || 0)} of ${formatNumber(result.total_rows || 0)} sample(s).`,
        outputPath,
      );
    } else if (payload.job.status === "failed") {
      window.clearInterval(state.cleanerPollTimer);
      els.runCleanerButton.disabled = false;
      els.runCleanerButton.textContent = "Run Cleaner";
      showError("Dataset cleaner failed", payload.job.error || "The cleaner job failed.", payload.job.log_tail || "");
    }
  } catch (error) {
    window.clearInterval(state.cleanerPollTimer);
    els.runCleanerButton.disabled = false;
    els.runCleanerButton.textContent = "Run Cleaner";
    showError("Could not read dataset cleaner status", error);
  }
}

function updateAudioSummary() {
  if (!els.audioCount || !els.audioHint) return;
  const unique = new Set();
  for (const entry of state.audioFiles.values()) {
    unique.add(entry.url || entry.src || entry.path);
  }
  els.audioCount.textContent = formatNumber(unique.size);
  els.audioHint.textContent = unique.size
    ? `${formatNumber(unique.size)} audio files ready for matching.`
    : "No local audio files indexed yet.";
}

function audioLookupKeys(pathLike) {
  const normalized = normalizePath(pathLike).toLowerCase();
  const fileName = basename(normalized);
  const noQuery = normalized.split("?")[0].split("#")[0];
  const keys = new Set([normalized, noQuery, fileName]);

  const audioIndex = normalized.search(/(^|\/)(audio|audios|clips|recordings|wavs)(\/|$)/i);
  if (audioIndex >= 0) {
    keys.add(normalized.slice(audioIndex).replace(/^\/+/, ""));
  }

  return Array.from(keys).filter(Boolean);
}

function indexAudioFiles(files) {
  let added = 0;
  for (const file of files) {
    const path = getFilePath(file);
    const extension = getExtension(path);
    if (!AUDIO_EXTENSIONS.has(extension)) continue;

    let url = state.objectUrls.get(path);
    if (!url) {
      url = URL.createObjectURL(file);
      state.objectUrls.set(path, url);
    }

    const entry = { file, url, path, name: file.name };
    for (const key of audioLookupKeys(path)) {
      if (!state.audioFiles.has(key)) {
        state.audioFiles.set(key, entry);
      }
    }
    added += 1;
  }

  updateAudioSummary();
  return added;
}

async function handleFiles(fileList) {
  const files = Array.from(fileList || []);
  if (!files.length) return;

  const audioFiles = files.filter((file) => AUDIO_EXTENSIONS.has(getExtension(getFilePath(file))));
  const dataFiles = files.filter((file) => DATA_EXTENSIONS.has(getExtension(getFilePath(file))));

  const audioCount = indexAudioFiles(audioFiles);

  if (!dataFiles.length) {
    showToast(
      audioCount
        ? `Indexed ${formatNumber(audioCount)} audio files. Open dataset files next.`
        : "No supported dataset files found.",
    );
    refreshActiveAudio();
    render();
    return;
  }

  setBusy(true, `Reading ${formatNumber(dataFiles.length)} dataset file(s)...`);
  let loaded = 0;
  let failed = 0;

  for (const file of dataFiles) {
    try {
      const path = getFilePath(file);
      const text = await file.text();
      const parsed = parseDatasetText(text, path);

      for (const item of parsed) {
        addDataset({
          name: item.name || readableName(path),
          path,
          source: "local",
          baseDir: dirname(path),
          records: item.records,
          file,
          isFinal: item.isFinal || isFinalDataset(path),
        });
        loaded += 1;
      }
    } catch (error) {
      failed += 1;
      console.error(error);
      showToast(`Could not read ${file.name}: ${error.message}`);
    }
  }

  chooseDefaultDataset();
  setBusy(false);
  render();
  showToast(
    `Loaded ${formatNumber(loaded)} dataset(s)${
      audioCount ? ` and indexed ${formatNumber(audioCount)} audio file(s)` : ""
    }${failed ? `. ${failed} failed.` : "."}`,
  );
}

function setBusy(isBusy, message = "") {
  document.body.classList.toggle("is-busy", isBusy);
  if (message) showToast(message);
}

function readableName(path) {
  const file = basename(path).replace(/\.(csv|tsv|json|jsonl|ndjson|txt)$/i, "");
  return file
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function isFinalDataset(path = "") {
  return /(^|[\/_.-])(final|clean-final|cleaned-final|final-clean|final_dataset|gold|cleaned)([\/_.-]|$)/i.test(
    path,
  );
}

function parseDatasetText(text, path) {
  const extension = getExtension(path);
  const cleanText = text.replace(/^\uFEFF/, "");

  if (extension === "csv" || extension === "tsv") {
    return [
      {
        name: readableName(path),
        records: parseDelimited(cleanText, extension === "tsv" ? "\t" : null),
      },
    ];
  }

  if (extension === "jsonl" || extension === "ndjson") {
    return [{ name: readableName(path), records: parseJsonLines(cleanText) }];
  }

  if (extension === "json") {
    return parseJsonDataset(cleanText, path);
  }

  if (extension === "txt") {
    return [
      {
        name: readableName(path),
        records: cleanText
          .split(/\r?\n/)
          .map((line, index) => ({ line_number: index + 1, text: line }))
          .filter((row) => row.text.trim() !== ""),
      },
    ];
  }

  throw new Error(`Unsupported file type: ${extension || "unknown"}`);
}

function parseDelimited(text, fixedDelimiter) {
  const delimiter = fixedDelimiter || guessDelimiter(text);
  const rows = [];
  let row = [];
  let field = "";
  let inQuotes = false;

  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    const next = text[index + 1];

    if (inQuotes) {
      if (char === '"' && next === '"') {
        field += '"';
        index += 1;
      } else if (char === '"') {
        inQuotes = false;
      } else {
        field += char;
      }
      continue;
    }

    if (char === '"') {
      inQuotes = true;
    } else if (char === delimiter) {
      row.push(field);
      field = "";
    } else if (char === "\n") {
      row.push(field.replace(/\r$/, ""));
      rows.push(row);
      row = [];
      field = "";
    } else {
      field += char;
    }
  }

  if (field.length || row.length) {
    row.push(field.replace(/\r$/, ""));
    rows.push(row);
  }

  const nonEmptyRows = rows.filter((line) => line.some((cell) => String(cell).trim() !== ""));
  if (!nonEmptyRows.length) return [];

  const headers = normalizeHeaders(nonEmptyRows[0]);
  return nonEmptyRows.slice(1).map((line, index) => {
    const record = {};
    for (let col = 0; col < Math.max(headers.length, line.length); col += 1) {
      const key = headers[col] || `extra_${col - headers.length + 1}`;
      record[key] = line[col] ?? "";
    }
    if (!("row_number" in record)) record.row_number = index + 1;
    return record;
  });
}

function guessDelimiter(text) {
  const sampleLines = text.split(/\r?\n/).filter(Boolean).slice(0, 10);
  const candidates = [",", "\t", ";", "|"];
  let best = ",";
  let bestScore = -1;

  for (const delimiter of candidates) {
    const counts = sampleLines.map((line) => countDelimiterOutsideQuotes(line, delimiter));
    const total = counts.reduce((sum, count) => sum + count, 0);
    const variation = Math.max(...counts, 0) - Math.min(...counts, 0);
    const score = total - variation * 2;
    if (score > bestScore) {
      bestScore = score;
      best = delimiter;
    }
  }

  return best;
}

function countDelimiterOutsideQuotes(line, delimiter) {
  let count = 0;
  let inQuotes = false;
  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    const next = line[index + 1];
    if (char === '"' && next === '"') {
      index += 1;
      continue;
    }
    if (char === '"') {
      inQuotes = !inQuotes;
      continue;
    }
    if (!inQuotes && char === delimiter) count += 1;
  }
  return count;
}

function normalizeHeaders(headers) {
  const seen = new Map();
  return headers.map((header, index) => {
    const base =
      String(header || "")
        .trim()
        .replace(/^\uFEFF/, "")
        .replace(/\s+/g, "_") || `column_${index + 1}`;
    const count = seen.get(base) || 0;
    seen.set(base, count + 1);
    return count ? `${base}_${count + 1}` : base;
  });
}

function parseJsonLines(text) {
  const records = [];
  const lines = text.split(/\r?\n/);

  for (const [index, line] of lines.entries()) {
    if (!line.trim()) continue;
    try {
      const value = JSON.parse(line);
      records.push(toRecord(value, index));
    } catch {
      records.push({ line_number: index + 1, text: line });
    }
  }

  return records;
}

function parseJsonDataset(text, path) {
  const parsed = JSON.parse(text);

  if (Array.isArray(parsed)) {
    return [{ name: readableName(path), records: parsed.map(toRecord) }];
  }

  if (parsed && typeof parsed === "object") {
    const preferredKeys = ["data", "rows", "records", "items", "examples", "samples"];
    for (const key of preferredKeys) {
      if (Array.isArray(parsed[key])) {
        return [{ name: readableName(path), records: parsed[key].map(toRecord) }];
      }
    }

    const splitDatasets = Object.entries(parsed)
      .filter(([, value]) => Array.isArray(value))
      .map(([key, value]) => ({
        name: `${readableName(path)} - ${key}`,
        records: value.map(toRecord),
        isFinal: isFinalDataset(`${path}/${key}`),
      }));

    if (splitDatasets.length) return splitDatasets;
  }

  return [{ name: readableName(path), records: [toRecord(parsed, 0)] }];
}

function toRecord(value, index = 0) {
  if (value && typeof value === "object" && !Array.isArray(value)) return flattenRecord(value);
  if (Array.isArray(value)) {
    return Object.fromEntries(value.map((item, itemIndex) => [`value_${itemIndex + 1}`, item]));
  }
  return { row_number: index + 1, value };
}

function flattenRecord(record, prefix = "", output = {}) {
  for (const [key, value] of Object.entries(record || {})) {
    const nextKey = prefix ? `${prefix}.${key}` : key;
    if (value && typeof value === "object" && !Array.isArray(value)) {
      flattenRecord(value, nextKey, output);
    } else {
      output[nextKey] = value;
    }
  }
  return output;
}

function addDataset(dataset) {
  const preparedRecords = dataset.records.map((record, index) => toRecord(record, index));
  const columns = collectColumns(preparedRecords);
  const orderedColumns = orderColumns(columns);
  const id = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const visibleColumns = new Set(orderedColumns);

  const nextDataset = {
    id,
    ...dataset,
    records: preparedRecords.map((record, index) => ({
      ...record,
      __rowIndex: index + 1,
    })),
    columns: orderedColumns,
    visibleColumns,
    totalRows: Number(dataset.totalRows || preparedRecords.length),
    shownRows: Number(dataset.shownRows || preparedRecords.length),
  };

  annotateAudio(nextDataset);
  state.datasets.push(nextDataset);
}

function collectColumns(records) {
  const columns = [];
  const seen = new Set();
  for (const record of records) {
    for (const key of Object.keys(record)) {
      if (key.startsWith("__") || key.startsWith("_play_")) continue;
      if (!seen.has(key)) {
        seen.add(key);
        columns.push(key);
      }
    }
  }
  return columns;
}

function orderColumns(columns) {
  const score = (column) => {
    const lower = column.toLowerCase();
    if (/^(id|row_number|index)$/.test(lower)) return 0;
    if (AUDIO_NAME_RE.test(column) || AUDIO_PATH_RE.test(column)) return 1;
    if (LONG_TEXT_RE.test(column)) return 2;
    return 3;
  };
  return [...columns].sort((a, b) => score(a) - score(b));
}

function chooseDefaultDataset() {
  if (!state.datasets.length) {
    state.activeId = null;
    return;
  }

  const currentExists = state.datasets.some((dataset) => dataset.id === state.activeId);
  if (currentExists) return;

  const finalDataset = state.datasets.find((dataset) => dataset.isFinal);
  state.activeId = (finalDataset || state.datasets[0]).id;
}

function activeDataset() {
  return state.datasets.find((dataset) => dataset.id === state.activeId) || null;
}

function annotateAudio(dataset) {
  let linked = 0;
  let candidates = 0;
  for (const row of dataset.records) {
    const info = resolveAudio(row, dataset);
    row.__audioInfo = info;
    if (info.path) candidates += 1;
    if (info.src) linked += 1;
  }
  dataset.audioLinked = linked;
  dataset.audioCandidates = candidates;
}

function refreshActiveAudio() {
  for (const dataset of state.datasets) annotateAudio(dataset);
}

function findAudioPath(row, dataset) {
  const serverPlayerColumn = Object.keys(row).find((column) => column.startsWith("_play_") && row[column]);
  if (serverPlayerColumn) {
    return { path: String(row[serverPlayerColumn]), column: serverPlayerColumn };
  }

  const preferred = dataset.columns
    .filter((column) => AUDIO_NAME_RE.test(column) || AUDIO_PATH_RE.test(String(row[column] || "")))
    .concat(dataset.columns.filter((column) => /audio\.path|path$/i.test(column)));

  const seen = new Set();
  for (const column of preferred) {
    if (seen.has(column)) continue;
    seen.add(column);
    const value = row[column];
    if (value === undefined || value === null || value === "") continue;

    const stringValue = String(value).trim();
    const columnStronglyAudio = /(audio|sound|recording|voice|clip|wav|mp3|flac|m4a|ogg)/i.test(
      column,
    );
    if (
      AUDIO_PATH_RE.test(stringValue) ||
      /[\\/]/.test(stringValue) ||
      (columnStronglyAudio && stringValue)
    ) {
      return { path: stringValue, column };
    }
  }

  return { path: "", column: "" };
}

function resolveAudio(row, dataset) {
  const candidate = findAudioPath(row, dataset);
  if (!candidate.path) return { path: "", column: "", src: "", sources: [], matched: false };

  const path = normalizePath(candidate.path);
  const localMatch = findLocalAudio(path, dataset);
  if (localMatch) {
    return {
      path,
      column: candidate.column,
      src: localMatch.url,
      sources: [localMatch.url],
      matched: true,
    };
  }

  const remoteSources = buildRemoteAudioSources(path, dataset);
  return {
    path,
    column: candidate.column,
    src: remoteSources[0] || "",
    sources: remoteSources,
    matched: Boolean(remoteSources.length && dataset.source === "manifest"),
  };
}

function findLocalAudio(path, dataset) {
  const candidates = new Set(audioLookupKeys(path));
  if (dataset.baseDir) {
    for (const key of audioLookupKeys(joinUrl(dataset.baseDir, path))) candidates.add(key);
  }
  for (const key of audioLookupKeys(basename(path))) candidates.add(key);

  for (const key of candidates) {
    const match = state.audioFiles.get(key.toLowerCase());
    if (match) return match;
  }
  return null;
}

function buildRemoteAudioSources(path, dataset) {
  if (/^(https?:|blob:|data:)/i.test(path)) return [path];

  if (/^(api\/|\/api\/)/i.test(path)) {
    return [path.startsWith("/") ? path : `./${path}`];
  }

  if (dataset.source === "server" && path) {
    return [`./api/audio?path=${encodeURIComponent(path)}`];
  }

  const candidates = new Set();
  const globalAudioRoot = dataset.manifestAudioRoot || "";
  const datasetAudioRoot = dataset.audioRoot || "";
  const baseDir = dataset.baseDir || "";
  const fileName = basename(path);

  if (datasetAudioRoot) {
    candidates.add(joinUrl(datasetAudioRoot, path));
    candidates.add(joinUrl(datasetAudioRoot, fileName));
  }
  if (globalAudioRoot) {
    candidates.add(joinUrl(globalAudioRoot, path));
    candidates.add(joinUrl(globalAudioRoot, fileName));
  }
  if (baseDir) candidates.add(joinUrl(baseDir, path));
  candidates.add(path);

  return Array.from(candidates).filter(Boolean);
}

function render() {
  chooseDefaultDataset();
  renderDatasetList();
  updateAudioSummary();

  const dataset = activeDataset();
  if (els.viewer) els.viewer.hidden = !dataset;
  if (!dataset) return;

  annotateAudio(dataset);
  renderViewer(dataset);
}

function renderDatasetList() {
  if (!els.datasetList || !els.datasetCount) return;
  els.datasetCount.textContent = formatNumber(state.datasets.length);
  els.datasetList.innerHTML = "";

  if (!state.datasets.length) {
    const empty = document.createElement("p");
    empty.className = "dataset-empty";
    empty.textContent = "No datasets loaded.";
    els.datasetList.append(empty);
    return;
  }

  for (const dataset of state.datasets) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `dataset-button${dataset.id === state.activeId ? " is-active" : ""}`;
    button.addEventListener("click", () => {
      state.activeId = dataset.id;
      state.page = 1;
      state.sort = { column: null, direction: "asc" };
      render();
    });

    const name = document.createElement("span");
    name.className = "dataset-name";
    name.textContent = dataset.name;
    button.append(name);

    if (dataset.isFinal) {
      const badge = document.createElement("span");
      badge.className = "badge final";
      badge.textContent = "Final";
      button.append(badge);
    }

    const meta = document.createElement("span");
    meta.className = "dataset-meta";
    const rowLabel =
      dataset.totalRows && dataset.totalRows > dataset.records.length
        ? `${formatNumber(dataset.records.length)} of ${formatNumber(dataset.totalRows)} rows`
        : `${formatNumber(dataset.records.length)} rows`;
    meta.textContent = `${rowLabel} - ${dataset.path}`;
    button.append(meta);
    els.datasetList.append(button);
  }
}

function renderViewer(dataset) {
  ensureViewerDefaults(dataset);
  els.datasetKind.textContent = dataset.isFinal ? "Final Dataset" : "Dataset";
  els.datasetTitle.textContent = dataset.name;
  els.datasetSubtitle.textContent = dataset.path;

  const filteredRows = getFilteredRows(dataset);
  const totalPages = Math.max(1, Math.ceil(filteredRows.length / state.pageSize));
  if (state.page > totalPages) state.page = totalPages;
  const start = (state.page - 1) * state.pageSize;
  const pageRows = filteredRows.slice(start, start + state.pageSize);

  renderStats(dataset, filteredRows.length);
  renderInsights(dataset, filteredRows);
  renderColumnControls(dataset);
  renderTable(dataset, pageRows);
  renderPagination(filteredRows.length, start, pageRows.length, totalPages);
}

function ensureViewerDefaults(dataset) {
  pruneUnavailableFilters(dataset);

  if (!dataset.columns.includes(state.focusedColumn)) state.focusedColumn = "";

  const preferredFacet = preferredCategoricalColumn(dataset);
  if (!dataset.columns.includes(state.facetColumn)) {
    state.facetColumn = preferredFacet || dataset.columns[0] || "";
  }
  if (!dataset.columns.includes(state.distributionColumn)) {
    state.distributionColumn = state.facetColumn || preferredFacet || dataset.columns[0] || "";
  }
}

function pruneUnavailableFilters(dataset) {
  for (const column of Array.from(state.exactFilters.keys())) {
    if (!dataset.columns.includes(column)) state.exactFilters.delete(column);
  }
}

function preferredCategoricalColumn(dataset) {
  const preferredNames = [
    "language_mix",
    "split",
    "_split",
    "source",
    "domain",
    "speaker_id",
    "gender",
    "accent",
    "is_synthetic",
  ];
  for (const name of preferredNames) {
    const match = dataset.columns.find((column) => column.toLowerCase() === name);
    if (match) return match;
  }
  return categoricalColumns(dataset)[0] || dataset.columns[0] || "";
}

function categoricalColumns(dataset) {
  const sampleRows = dataset.records.slice(0, 500);
  return dataset.columns.filter((column) => {
    if (LONG_TEXT_RE.test(column) && !/language|mix|source|domain|split/i.test(column)) return false;
    const values = new Set();
    for (const row of sampleRows) {
      values.add(facetKeyForValue(row[column], "exact"));
      if (values.size > 80) return false;
    }
    return values.size > 0;
  });
}

function renderStats(dataset, filteredCount) {
  const durationColumn = findDurationColumn(dataset);
  const totalSeconds = durationColumn ? sumDurationSeconds(dataset.records, durationColumn) : 0;
  const filteredRows = filteredCount === dataset.records.length ? dataset.records : getFilteredRows(dataset, { includeSort: false });
  const filteredSeconds = durationColumn ? sumDurationSeconds(filteredRows, durationColumn) : 0;
  const missing = Math.max(dataset.audioCandidates - dataset.audioLinked, 0);
  const diskRows = Number(dataset.totalRows || dataset.records.length);
  const stats = [];

  stats.push({ label: diskRows > dataset.records.length ? "Rows Loaded" : "Rows", value: formatNumber(dataset.records.length) });
  if (diskRows > dataset.records.length) stats.push({ label: "Rows In Split", value: formatNumber(diskRows) });
  stats.push({ label: "Visible", value: formatNumber(filteredCount) });
  stats.push({ label: "Columns", value: formatNumber(dataset.columns.length) });
  stats.push({ label: "Playable Audio", value: formatNumber(dataset.audioLinked) });
  if (dataset.audioCandidates) stats.push({ label: "Missing Audio", value: formatNumber(missing) });
  if (durationColumn) {
    stats.push({ label: "Total Hours", value: formatHours(totalSeconds) });
    if (filteredCount !== dataset.records.length) stats.push({ label: "Visible Hours", value: formatHours(filteredSeconds) });
    if (dataset.records.length) stats.push({ label: "Avg Duration", value: formatDuration(totalSeconds / dataset.records.length) });
  }

  els.statsGrid.innerHTML = "";
  for (const { label, value } of stats.slice(0, 8)) {
    const stat = document.createElement("div");
    stat.className = "stat";
    stat.innerHTML = `
      <span class="stat-value">${value}</span>
      <span class="stat-label">${label}</span>
    `;
    els.statsGrid.append(stat);
  }
}

function renderInsights(dataset, filteredRows) {
  renderFacetControls(dataset);
  renderActiveFilters(dataset);
  renderFacetValues(dataset);
  renderDistributionControls(dataset);
  renderDistributionChart(dataset, filteredRows);
}

function renderFacetControls(dataset) {
  if (!els.facetColumn || !els.facetMode || !els.facetSearch) return;
  fillColumnSelect(els.facetColumn, dataset.columns, state.facetColumn);
  els.facetMode.value = state.facetMode;
  els.facetSearch.value = state.facetSearch;
}

function renderActiveFilters(dataset) {
  if (!els.activeFilters || !els.clearExactFilters) return;
  els.activeFilters.innerHTML = "";
  const active = activeColumnFilters(dataset);
  els.clearExactFilters.disabled = active.length === 0;

  if (!active.length) {
    const empty = document.createElement("span");
    empty.className = "filter-empty";
    empty.textContent = "No filters";
    els.activeFilters.append(empty);
    return;
  }

  for (const [column, filter] of active) {
    for (const key of filter.values) {
      const chip = document.createElement("button");
      chip.className = "filter-chip";
      chip.type = "button";
      chip.title = `Remove ${column}`;
      chip.textContent = `${column}: ${displayFacetKey(key)}`;
      chip.addEventListener("click", () => {
        removeExactFilter(column, key);
        state.page = 1;
        render();
      });
      els.activeFilters.append(chip);
    }
  }
}

function renderFacetValues(dataset) {
  if (!els.facetValueList) return;
  els.facetValueList.innerHTML = "";
  const column = state.facetColumn;
  if (!column) return;

  const current = getColumnFilter(column);
  const rows = getFilteredRows(dataset, {
    includeSort: false,
    skipColumnFilter: column,
  });
  const buckets = valueBuckets(rows, column, state.facetMode, findDurationColumn(dataset));
  const query = state.facetSearch.trim().toLowerCase();
  const visibleBuckets = buckets
    .filter((bucket) => !query || bucket.label.toLowerCase().includes(query))
    .slice(0, 80);

  if (!visibleBuckets.length) {
    const empty = document.createElement("p");
    empty.className = "dataset-empty";
    empty.textContent = "No values";
    els.facetValueList.append(empty);
    return;
  }

  for (const bucket of visibleBuckets) {
    const label = document.createElement("label");
    label.className = "facet-value";

    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.checked = current.values.has(bucket.key);
    checkbox.addEventListener("change", () => {
      setExactFilterValue(column, bucket.key, checkbox.checked, state.facetMode);
      state.page = 1;
      render();
    });

    const value = document.createElement("span");
    value.className = "facet-label";
    value.textContent = bucket.label;
    value.title = bucket.label;

    const count = document.createElement("span");
    count.className = "facet-count";
    count.textContent = formatNumber(bucket.count);

    label.append(checkbox, value, count);
    els.facetValueList.append(label);
  }
}

function renderDistributionControls(dataset) {
  if (!els.distributionColumn || !els.distributionMetric) return;
  fillColumnSelect(els.distributionColumn, dataset.columns, state.distributionColumn);
  els.distributionMetric.value = state.distributionMetric;
  els.distributionMetric.disabled = !findDurationColumn(dataset);
  if (els.distributionMetric.disabled && state.distributionMetric === "hours") {
    state.distributionMetric = "count";
    els.distributionMetric.value = "count";
  }
}

function renderDistributionChart(dataset, filteredRows) {
  if (!els.distributionChart) return;
  els.distributionChart.innerHTML = "";
  const column = state.distributionColumn;
  if (!column) return;

  const durationColumn = findDurationColumn(dataset);
  const metric = durationColumn ? state.distributionMetric : "count";
  const buckets = valueBuckets(filteredRows, column, "exact", durationColumn);
  const ranked = buckets
    .map((bucket) => ({
      ...bucket,
      metricValue: metric === "hours" ? bucket.seconds / 3600 : bucket.count,
    }))
    .filter((bucket) => bucket.metricValue > 0)
    .sort((a, b) => b.metricValue - a.metricValue || a.label.localeCompare(b.label))
    .slice(0, 20);

  if (!ranked.length) {
    const empty = document.createElement("p");
    empty.className = "dataset-empty";
    empty.textContent = "No distribution";
    els.distributionChart.append(empty);
    return;
  }

  const maxValue = Math.max(...ranked.map((bucket) => bucket.metricValue), 1);
  for (const bucket of ranked) {
    const row = document.createElement("div");
    row.className = "bar-row";

    const label = document.createElement("span");
    label.className = "bar-label";
    label.title = bucket.label;
    label.textContent = bucket.label;

    const track = document.createElement("span");
    track.className = "bar-track";
    const fill = document.createElement("span");
    fill.className = "bar-fill";
    fill.style.width = `${Math.max(3, (bucket.metricValue / maxValue) * 100)}%`;
    track.append(fill);

    const value = document.createElement("span");
    value.className = "bar-value";
    value.textContent = metric === "hours" ? formatHours(bucket.seconds) : formatNumber(bucket.count);

    row.append(label, track, value);
    els.distributionChart.append(row);
  }
}

function fillColumnSelect(select, columns, selected) {
  const previous = selected || select.value;
  select.innerHTML = "";
  for (const column of columns) {
    const option = document.createElement("option");
    option.value = column;
    option.textContent = column;
    select.append(option);
  }
  select.value = columns.includes(previous) ? previous : columns[0] || "";
}

function activeColumnFilters(dataset) {
  return Array.from(state.exactFilters.entries()).filter(
    ([column, filter]) => dataset.columns.includes(column) && filter.values.size,
  );
}

function getColumnFilter(column) {
  return state.exactFilters.get(column) || { mode: state.facetMode, values: new Set() };
}

function setExactFilterValue(column, key, checked, mode) {
  const existing = state.exactFilters.get(column);
  const filter =
    existing && existing.mode === mode
      ? existing
      : {
          mode,
          values: new Set(),
        };

  if (checked) filter.values.add(key);
  else filter.values.delete(key);

  if (filter.values.size) state.exactFilters.set(column, filter);
  else state.exactFilters.delete(column);
}

function removeExactFilter(column, key) {
  const filter = state.exactFilters.get(column);
  if (!filter) return;
  filter.values.delete(key);
  if (!filter.values.size) state.exactFilters.delete(column);
}

function clearExactFilters() {
  state.exactFilters.clear();
  state.facetSearch = "";
  state.page = 1;
  render();
}

function valueBuckets(rows, column, mode, durationColumn = "") {
  const buckets = new Map();
  for (const row of rows) {
    const keys = facetKeysForValue(row[column], mode);
    const duration = durationColumn ? durationSeconds(row[durationColumn], durationColumn) : 0;
    for (const key of keys) {
      if (!buckets.has(key)) {
        buckets.set(key, {
          key,
          label: displayFacetKey(key),
          count: 0,
          seconds: 0,
        });
      }
      const bucket = buckets.get(key);
      bucket.count += 1;
      bucket.seconds += duration;
    }
  }
  return Array.from(buckets.values()).sort((a, b) => b.count - a.count || a.label.localeCompare(b.label));
}

function facetKeysForValue(value, mode) {
  if (mode === "token") {
    const tokens = splitFacetTokens(value).map(facetKeyForScalar).filter(Boolean);
    return Array.from(new Set(tokens.length ? tokens : [EMPTY_FACET_KEY]));
  }
  return [facetKeyForValue(value, "exact")];
}

function facetKeyForValue(value, mode = "exact") {
  if (mode === "token") return facetKeysForValue(value, mode)[0] || EMPTY_FACET_KEY;
  if (value === undefined || value === null || value === "") return EMPTY_FACET_KEY;
  if (Array.isArray(value)) {
    const parts = value.map(valueToString).map((item) => item.trim()).filter(Boolean);
    return parts.length ? parts.join(" | ") : EMPTY_FACET_KEY;
  }
  if (typeof value === "object") return valueToString(value) || EMPTY_FACET_KEY;
  const stringValue = String(value).trim();
  return stringValue || EMPTY_FACET_KEY;
}

function facetKeyForScalar(value) {
  const stringValue = valueToString(value).trim();
  return stringValue || EMPTY_FACET_KEY;
}

function splitFacetTokens(value) {
  if (Array.isArray(value)) {
    return value.flatMap((item) => splitFacetTokens(item));
  }
  if (value && typeof value === "object") return [valueToString(value)];
  return String(value ?? "")
    .split(MULTI_VALUE_RE)
    .map((item) => item.trim())
    .filter(Boolean);
}

function displayFacetKey(key) {
  return key === EMPTY_FACET_KEY ? "(blank)" : key;
}

function rowMatchesExactFilters(row, dataset, skipColumn = "") {
  for (const [column, filter] of activeColumnFilters(dataset)) {
    if (column === skipColumn) continue;
    const rowKeys = new Set(facetKeysForValue(row[column], filter.mode));
    let matched = false;
    for (const key of filter.values) {
      if (rowKeys.has(key)) {
        matched = true;
        break;
      }
    }
    if (!matched) return false;
  }
  return true;
}

function findDurationColumn(dataset) {
  const exact = dataset.columns.find((column) => /^duration_seconds$/i.test(column));
  if (exact) return exact;

  const named = dataset.columns.find((column) => DURATION_NAME_RE.test(column) && columnHasNumericDuration(dataset, column));
  if (named) return named;

  return dataset.columns.find((column) => columnHasNumericDuration(dataset, column) && /duration|seconds|length/i.test(column)) || "";
}

function columnHasNumericDuration(dataset, column) {
  let numeric = 0;
  let checked = 0;
  for (const row of dataset.records.slice(0, 200)) {
    const value = row[column];
    if (value === undefined || value === null || value === "") continue;
    checked += 1;
    if (durationSeconds(value, column) > 0) numeric += 1;
  }
  return checked > 0 && numeric / checked >= 0.75;
}

function sumDurationSeconds(rows, column) {
  return rows.reduce((sum, row) => sum + durationSeconds(row[column], column), 0);
}

function durationSeconds(value, column = "") {
  if (value === undefined || value === null || value === "") return 0;
  if (typeof value === "number") return durationUnitSeconds(value, column);
  const text = String(value).trim();
  if (!text) return 0;

  const colon = text.match(/^(\d+):([0-5]?\d)(?::([0-5]?\d(?:\.\d+)?))?$/);
  if (colon) {
    const first = Number(colon[1]);
    const second = Number(colon[2]);
    const third = colon[3] === undefined ? null : Number(colon[3]);
    return third === null ? first * 60 + second : first * 3600 + second * 60 + third;
  }

  const number = Number(text.replace(/,/g, ""));
  if (Number.isNaN(number)) return 0;
  return durationUnitSeconds(number, column);
}

function durationUnitSeconds(value, column = "") {
  const lower = column.toLowerCase();
  if (/(^|[._-])(ms|millis|milliseconds)([._-]|$)/.test(lower)) return value / 1000;
  if (/(^|[._-])(min|mins|minutes)([._-]|$)/.test(lower)) return value * 60;
  if (/(^|[._-])(hour|hours|hrs)([._-]|$)/.test(lower)) return value * 3600;
  return value;
}

function formatHours(seconds) {
  const hours = seconds / 3600;
  return hours >= 10 ? hours.toFixed(1) : hours.toFixed(2);
}

function formatDuration(seconds) {
  if (!Number.isFinite(seconds) || seconds <= 0) return "0s";
  if (seconds >= 3600) return `${formatHours(seconds)}h`;
  if (seconds >= 60) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  return `${seconds.toFixed(seconds >= 10 ? 1 : 2)}s`;
}

function renderColumnControls(dataset) {
  const currentFocus = els.columnFocus.value;
  els.columnFocus.innerHTML = '<option value="">All columns</option>';
  for (const column of dataset.columns) {
    const option = document.createElement("option");
    option.value = column;
    option.textContent = column;
    els.columnFocus.append(option);
  }
  els.columnFocus.value = dataset.columns.includes(currentFocus) ? currentFocus : state.focusedColumn;

  els.columnToggles.innerHTML = "";
  for (const column of dataset.columns) {
    const label = document.createElement("label");
    label.className = "column-toggle";

    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.checked = dataset.visibleColumns.has(column);
    checkbox.addEventListener("change", () => {
      if (checkbox.checked) dataset.visibleColumns.add(column);
      else dataset.visibleColumns.delete(column);
      state.page = 1;
      render();
    });

    const text = document.createElement("span");
    text.textContent = column;

    label.append(checkbox, text);
    els.columnToggles.append(label);
  }
}

function visibleColumns(dataset) {
  return dataset.columns.filter((column) => dataset.visibleColumns.has(column));
}

function getFilteredRows(dataset, options = {}) {
  const { includeSort = true, skipColumnFilter = "" } = options;
  const query = state.search.trim().toLowerCase();
  const focusedColumn = state.focusedColumn;
  const columns = focusedColumn ? [focusedColumn] : dataset.columns;

  let rows = dataset.records.filter((row) => {
    if (state.audioFilter === "linked" && !row.__audioInfo?.src) return false;
    if (state.audioFilter === "missing" && (!row.__audioInfo?.path || row.__audioInfo?.src)) {
      return false;
    }
    if (!rowMatchesExactFilters(row, dataset, skipColumnFilter)) return false;
    if (!query) return true;
    return columns.some((column) => valueToString(row[column]).toLowerCase().includes(query));
  });

  if (includeSort && state.sort.column) {
    const direction = state.sort.direction === "desc" ? -1 : 1;
    rows = [...rows].sort((a, b) => compareValues(a[state.sort.column], b[state.sort.column]) * direction);
  }

  return rows;
}

function compareValues(left, right) {
  const leftString = valueToString(left);
  const rightString = valueToString(right);
  const leftNumber = Number(leftString);
  const rightNumber = Number(rightString);
  const bothNumeric = leftString.trim() !== "" && rightString.trim() !== "" && !Number.isNaN(leftNumber) && !Number.isNaN(rightNumber);

  if (bothNumeric) return leftNumber - rightNumber;
  return leftString.localeCompare(rightString, undefined, { numeric: true, sensitivity: "base" });
}

function renderTable(dataset, rows) {
  const columns = visibleColumns(dataset);
  els.tableHead.innerHTML = "";
  els.tableBody.innerHTML = "";

  const headerRow = document.createElement("tr");
  headerRow.append(createHeaderCell("#", "row-number"));
  headerRow.append(createHeaderCell("Audio", "audio-cell"));

  for (const column of columns) {
    const th = createHeaderCell(column);
    th.addEventListener("click", () => {
      if (state.sort.column === column) {
        state.sort.direction = state.sort.direction === "asc" ? "desc" : "asc";
      } else {
        state.sort.column = column;
        state.sort.direction = "asc";
      }
      render();
    });
    headerRow.append(th);
  }
  els.tableHead.append(headerRow);

  if (!rows.length) {
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = columns.length + 2;
    cell.textContent = "No rows match the current filters.";
    row.append(cell);
    els.tableBody.append(row);
    return;
  }

  for (const rowData of rows) {
    const row = document.createElement("tr");
    const rowNumber = document.createElement("td");
    rowNumber.className = "row-number";
    rowNumber.textContent = rowData.__rowIndex;
    row.append(rowNumber);

    const audioCell = document.createElement("td");
    audioCell.className = "audio-cell";
    audioCell.append(renderAudio(rowData.__audioInfo));
    row.append(audioCell);

    for (const column of columns) {
      const cell = document.createElement("td");
      const content = document.createElement("span");
      const value = rowData[column];
      content.className = `cell-content${isNumeric(value) ? " numeric" : ""}`;
      content.textContent = valueToString(value);
      cell.append(content);
      row.append(cell);
    }

    els.tableBody.append(row);
  }
}

function createHeaderCell(text, className = "") {
  const th = document.createElement("th");
  if (className) th.className = className;
  th.scope = "col";
  th.textContent = text;

  if (state.sort.column === text) {
    th.textContent = `${text} ${state.sort.direction === "asc" ? "^" : "v"}`;
  }

  return th;
}

function renderAudio(info = {}) {
  const wrap = document.createElement("div");
  wrap.className = "audio-wrap";

  if (info.src) {
    const audio = document.createElement("audio");
    audio.controls = true;
    audio.preload = "none";
    audio.src = info.src;
    audio.dataset.sources = JSON.stringify(info.sources || [info.src]);
    audio.dataset.sourceIndex = "0";
    audio.addEventListener("error", tryNextAudioSource);
    wrap.append(audio);
  } else if (info.path) {
    const missing = document.createElement("span");
    missing.className = "missing-audio";
    missing.textContent = "Missing audio";
    wrap.append(missing);
  } else {
    const empty = document.createElement("span");
    empty.className = "audio-path";
    empty.textContent = "No audio";
    wrap.append(empty);
  }

  if (info.path) {
    const path = document.createElement("span");
    path.className = "audio-path";
    path.textContent = info.path;
    wrap.append(path);
  }

  return wrap;
}

function tryNextAudioSource(event) {
  const audio = event.currentTarget;
  const sources = JSON.parse(audio.dataset.sources || "[]");
  const currentIndex = Number(audio.dataset.sourceIndex || "0");
  const nextIndex = currentIndex + 1;
  const next = sources[nextIndex];
  if (next) {
    audio.dataset.sourceIndex = String(nextIndex);
    audio.src = next;
    audio.load();
  } else {
    audio.removeEventListener("error", tryNextAudioSource);
  }
}

function renderPagination(totalRows, start, pageRows, totalPages) {
  const end = totalRows ? start + pageRows : 0;
  els.pageStatus.textContent = totalRows
    ? `${formatNumber(start + 1)}-${formatNumber(end)} of ${formatNumber(totalRows)} - page ${formatNumber(
        state.page,
      )}/${formatNumber(totalPages)}`
    : "0 rows";
  els.prevPage.disabled = state.page <= 1;
  els.nextPage.disabled = state.page >= totalPages;
}

function valueToString(value) {
  if (value === undefined || value === null) return "";
  if (Array.isArray(value)) {
    if (value.length > 12) return `[array: ${formatNumber(value.length)} items]`;
    return value.map(valueToString).join(", ");
  }
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function isNumeric(value) {
  if (value === "" || value === null || value === undefined) return false;
  return !Number.isNaN(Number(value));
}

function exportFilteredRows() {
  const dataset = activeDataset();
  if (!dataset) return;

  const rows = getFilteredRows(dataset).map((row) => {
    const clean = {};
    for (const column of dataset.columns) clean[column] = row[column];
    return clean;
  });

  const body = rows.map((row) => JSON.stringify(row)).join("\n");
  const blob = new Blob([body], { type: "application/x-ndjson" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `${dataset.name.replace(/\W+/g, "_").replace(/^_+|_+$/g, "") || "dataset"}_filtered.jsonl`;
  document.body.append(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
}

async function loadManifest({ silent = false } = {}) {
  if (!silent) setBusy(true, "Looking for viewer-manifest.json...");
  try {
    const response = await fetch("./viewer-manifest.json", { cache: "no-store" });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const manifest = await response.json();
    state.datasets = state.datasets.filter((dataset) => dataset.source !== "manifest");
    state.activeId = null;
    await loadManifestObject(manifest);
    chooseDefaultDataset();
    render();
    if (!silent) showToast("Loaded viewer-manifest.json.");
  } catch (error) {
    console.info("Manifest unavailable:", error);
    if (!silent) showError("No manifest found", "Create viewer-manifest.json or use the dataset path loader.");
  } finally {
    if (!silent) setBusy(false);
  }
}

async function loadManifestObject(manifest) {
  const entries = Array.isArray(manifest.datasets) ? manifest.datasets : [];
  let loaded = 0;

  for (const entry of entries) {
    if (!entry.path) continue;
    const response = await fetch(entry.path, { cache: "no-store" });
    if (!response.ok) throw new Error(`Could not fetch ${entry.path}`);
    const text = await response.text();
    const parsed = parseDatasetText(text, entry.path);
    for (const item of parsed) {
      addDataset({
        name: entry.name || item.name || readableName(entry.path),
        path: entry.path,
        source: "manifest",
        baseDir: dirname(entry.path),
        audioRoot: entry.audioRoot || "",
        manifestAudioRoot: manifest.audioRoot || "",
        records: item.records,
        isFinal: Boolean(entry.final) || item.isFinal || isFinalDataset(entry.path),
      });
      loaded += 1;
    }
  }

  if (!loaded) throw new Error("Manifest did not contain supported datasets.");
}

function bindEvents() {
  const on = (element, eventName, handler) => {
    if (element) element.addEventListener(eventName, handler);
  };

  on(els.navViewer, "click", () => switchPage("viewer"));
  on(els.navGenerator, "click", () => switchPage("generator"));
  on(els.navCleaner, "click", () => switchPage("cleaner"));
  on(els.messageClose, "click", clearMessage);
  on(els.datasetLoadForm, "submit", loadDatasetFromForm);
  on(els.hfImportForm, "submit", startHfDatasetImport);
  on(els.generatorForm, "submit", startGeneration);
  on(els.hfMergeForm, "submit", startHfDatasetMerge);
  on(els.hfPushForm, "submit", startHfDatasetPush);
  on(els.cleanerForm, "submit", startDatasetCleaner);
  on(els.cleanerSaveModeInput, "change", setCleanerSaveModeState);
  on(els.loadHfColumnsButton, "click", () => loadHfColumns());
  on(els.hfPushDatasetPathInput, "input", () => {
    state.hfPushColumns = [];
    if (els.hfColumnToggles) els.hfColumnToggles.innerHTML = "";
  });
  on(els.syntheticTestButton, "click", runSyntheticTest);

  on(els.fileInput, "change", (event) => handleFiles(event.target.files));
  on(els.filePickerInput, "change", (event) => handleFiles(event.target.files));
  on(els.folderInput, "change", (event) => handleFiles(event.target.files));
  on(els.manifestButton, "click", loadManifest);

  on(els.dropzone, "dragover", (event) => {
    event.preventDefault();
    els.dropzone.classList.add("is-dragging");
  });
  on(els.dropzone, "dragleave", () => {
    els.dropzone.classList.remove("is-dragging");
  });
  on(els.dropzone, "drop", (event) => {
    event.preventDefault();
    els.dropzone.classList.remove("is-dragging");
    handleFiles(event.dataTransfer.files);
  });

  on(els.searchInput, "input", (event) => {
    state.search = event.target.value;
    state.page = 1;
    render();
  });

  on(els.facetColumn, "change", (event) => {
    state.facetColumn = event.target.value;
    const filter = state.exactFilters.get(state.facetColumn);
    if (filter) state.facetMode = filter.mode;
    state.facetSearch = "";
    state.page = 1;
    render();
  });

  on(els.facetMode, "change", (event) => {
    state.facetMode = event.target.value;
    if (state.facetColumn) state.exactFilters.delete(state.facetColumn);
    state.page = 1;
    render();
  });

  on(els.facetSearch, "input", (event) => {
    state.facetSearch = event.target.value;
    render();
  });

  on(els.clearExactFilters, "click", clearExactFilters);

  on(els.distributionColumn, "change", (event) => {
    state.distributionColumn = event.target.value;
    render();
  });

  on(els.distributionMetric, "change", (event) => {
    state.distributionMetric = event.target.value;
    render();
  });

  on(els.pageSizeSelect, "change", (event) => {
    state.pageSize = Number(event.target.value);
    state.page = 1;
    render();
  });

  on(els.audioFilter, "change", (event) => {
    state.audioFilter = event.target.value;
    state.page = 1;
    render();
  });

  on(els.columnFocus, "change", (event) => {
    state.focusedColumn = event.target.value;
    state.page = 1;
    render();
  });

  on(els.prevPage, "click", () => {
    state.page = Math.max(1, state.page - 1);
    render();
  });

  on(els.nextPage, "click", () => {
    state.page += 1;
    render();
  });

  on(els.showAllColumns, "click", () => {
    const dataset = activeDataset();
    if (!dataset) return;
    dataset.visibleColumns = new Set(dataset.columns);
    render();
  });

  on(els.hideLongColumns, "click", () => {
    const dataset = activeDataset();
    if (!dataset) return;
    dataset.visibleColumns = new Set(
      dataset.columns.filter((column) => !LONG_TEXT_RE.test(column) || /text|sentence|transcript/i.test(column)),
    );
    render();
  });

  on(els.exportButton, "click", exportFilteredRows);
}

bindEvents();
setCleanerSaveModeState();
render();
switchPage("viewer");
detectServerFeatures();

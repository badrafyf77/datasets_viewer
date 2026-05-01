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

const state = {
  datasets: [],
  audioFiles: new Map(),
  objectUrls: new Map(),
  activeId: null,
  search: "",
  audioFilter: "all",
  focusedColumn: "",
  pageSize: 50,
  page: 1,
  sort: { column: null, direction: "asc" },
};

const els = {
  fileInput: document.getElementById("fileInput"),
  filePickerInput: document.getElementById("filePickerInput"),
  folderInput: document.getElementById("folderInput"),
  dropzone: document.getElementById("dropzone"),
  manifestButton: document.getElementById("manifestButton"),
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
  columnToggles: document.getElementById("columnToggles"),
  showAllColumns: document.getElementById("showAllColumns"),
  hideLongColumns: document.getElementById("hideLongColumns"),
  tableHead: document.getElementById("tableHead"),
  tableBody: document.getElementById("tableBody"),
  prevPage: document.getElementById("prevPage"),
  nextPage: document.getElementById("nextPage"),
  pageStatus: document.getElementById("pageStatus"),
  exportButton: document.getElementById("exportButton"),
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
  els.toast.textContent = message;
  els.toast.classList.add("is-visible");
  window.clearTimeout(showToast.timer);
  showToast.timer = window.setTimeout(() => {
    els.toast.classList.remove("is-visible");
  }, 3600);
}

function updateAudioSummary() {
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
  const columns = collectColumns(dataset.records);
  const orderedColumns = orderColumns(columns);
  const id = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const visibleColumns = new Set(orderedColumns);

  const nextDataset = {
    id,
    ...dataset,
    records: dataset.records.map((record, index) => ({
      ...record,
      __rowIndex: index + 1,
    })),
    columns: orderedColumns,
    visibleColumns,
  };

  annotateAudio(nextDataset);
  state.datasets.push(nextDataset);
}

function collectColumns(records) {
  const columns = [];
  const seen = new Set();
  for (const record of records) {
    for (const key of Object.keys(record)) {
      if (key.startsWith("__")) continue;
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
  els.emptyState.hidden = Boolean(dataset);
  els.viewer.hidden = !dataset;
  if (!dataset) return;

  annotateAudio(dataset);
  renderViewer(dataset);
}

function renderDatasetList() {
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
    meta.textContent = `${formatNumber(dataset.records.length)} rows - ${dataset.path}`;
    button.append(meta);
    els.datasetList.append(button);
  }
}

function renderViewer(dataset) {
  els.datasetKind.textContent = dataset.isFinal ? "Final Dataset" : "Dataset";
  els.datasetTitle.textContent = dataset.name;
  els.datasetSubtitle.textContent = dataset.path;

  const filteredRows = getFilteredRows(dataset);
  const totalPages = Math.max(1, Math.ceil(filteredRows.length / state.pageSize));
  if (state.page > totalPages) state.page = totalPages;
  const start = (state.page - 1) * state.pageSize;
  const pageRows = filteredRows.slice(start, start + state.pageSize);

  renderStats(dataset, filteredRows.length);
  renderColumnControls(dataset);
  renderTable(dataset, pageRows);
  renderPagination(filteredRows.length, start, pageRows.length, totalPages);
}

function renderStats(dataset, filteredCount) {
  const missing = Math.max(dataset.audioCandidates - dataset.audioLinked, 0);
  const stats = [
    ["Rows", dataset.records.length],
    ["Visible", filteredCount],
    ["Columns", dataset.columns.length],
    ["Playable Audio", dataset.audioLinked],
  ];

  if (dataset.audioCandidates) stats.push(["Missing Audio", missing]);

  els.statsGrid.innerHTML = "";
  for (const [label, value] of stats.slice(0, 5)) {
    const stat = document.createElement("div");
    stat.className = "stat";
    stat.innerHTML = `
      <span class="stat-value">${formatNumber(value)}</span>
      <span class="stat-label">${label}</span>
    `;
    els.statsGrid.append(stat);
  }
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

function getFilteredRows(dataset) {
  const query = state.search.trim().toLowerCase();
  const focusedColumn = state.focusedColumn;
  const columns = focusedColumn ? [focusedColumn] : dataset.columns;

  let rows = dataset.records.filter((row) => {
    if (state.audioFilter === "linked" && !row.__audioInfo?.src) return false;
    if (state.audioFilter === "missing" && (!row.__audioInfo?.path || row.__audioInfo?.src)) {
      return false;
    }
    if (!query) return true;
    return columns.some((column) => valueToString(row[column]).toLowerCase().includes(query));
  });

  if (state.sort.column) {
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
    if (!silent) showToast("No manifest found. Use Open Folder or create viewer-manifest.json.");
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
  els.fileInput.addEventListener("change", (event) => handleFiles(event.target.files));
  els.filePickerInput.addEventListener("change", (event) => handleFiles(event.target.files));
  els.folderInput.addEventListener("change", (event) => handleFiles(event.target.files));
  els.manifestButton.addEventListener("click", loadManifest);

  els.dropzone.addEventListener("dragover", (event) => {
    event.preventDefault();
    els.dropzone.classList.add("is-dragging");
  });
  els.dropzone.addEventListener("dragleave", () => {
    els.dropzone.classList.remove("is-dragging");
  });
  els.dropzone.addEventListener("drop", (event) => {
    event.preventDefault();
    els.dropzone.classList.remove("is-dragging");
    handleFiles(event.dataTransfer.files);
  });

  els.searchInput.addEventListener("input", (event) => {
    state.search = event.target.value;
    state.page = 1;
    render();
  });

  els.pageSizeSelect.addEventListener("change", (event) => {
    state.pageSize = Number(event.target.value);
    state.page = 1;
    render();
  });

  els.audioFilter.addEventListener("change", (event) => {
    state.audioFilter = event.target.value;
    state.page = 1;
    render();
  });

  els.columnFocus.addEventListener("change", (event) => {
    state.focusedColumn = event.target.value;
    state.page = 1;
    render();
  });

  els.prevPage.addEventListener("click", () => {
    state.page = Math.max(1, state.page - 1);
    render();
  });

  els.nextPage.addEventListener("click", () => {
    state.page += 1;
    render();
  });

  els.showAllColumns.addEventListener("click", () => {
    const dataset = activeDataset();
    if (!dataset) return;
    dataset.visibleColumns = new Set(dataset.columns);
    render();
  });

  els.hideLongColumns.addEventListener("click", () => {
    const dataset = activeDataset();
    if (!dataset) return;
    dataset.visibleColumns = new Set(
      dataset.columns.filter((column) => !LONG_TEXT_RE.test(column) || /text|sentence|transcript/i.test(column)),
    );
    render();
  });

  els.exportButton.addEventListener("click", exportFilteredRows);
}

bindEvents();
render();

loadManifest({ silent: true }).catch(() => {});

"use strict";

const INDEX_TYPES = [
  ["none", "None"],
  ["scalar", "Scalar"],
  ["b_tree", "B-tree"],
  ["bitmap", "Bitmap"],
  ["label_list", "Label list"],
  ["inverted", "Inverted"],
  ["n_gram", "N-gram"],
  ["zone_map", "Zone map"],
  ["bloom_filter", "Bloom filter"],
  ["r_tree", "R-tree"],
  ["fm", "FM"],
  ["vector", "Vector"],
  ["ivf_flat", "IVF Flat"],
  ["ivf_sq", "IVF SQ"],
  ["ivf_pq", "IVF PQ"],
  ["ivf_hnsw_sq", "IVF HNSW SQ"],
  ["ivf_hnsw_pq", "IVF HNSW PQ"],
  ["ivf_hnsw_flat", "IVF HNSW Flat"],
  ["ivf_rq", "IVF RQ"],
];

const state = {
  inspectedSource: "",
  fields: [],
  jobsLoading: false,
  inspectController: null,
};

const elements = {};

// Caches the DOM elements used throughout the interface.
function cacheElements() {
  elements.form = document.querySelector("#job-form");
  elements.creator = document.querySelector("#creator");
  elements.source = document.querySelector("#source-uri");
  elements.destination = document.querySelector("#destination-uri");
  elements.move = document.querySelector("#kind-move");
  elements.moveHelp = document.querySelector("#move-help");
  elements.inspectButton = document.querySelector("#inspect-button");
  elements.submitButton = document.querySelector("#submit-button");
  elements.schemaSection = document.querySelector("#schema-section");
  elements.schemaState = document.querySelector("#schema-state");
  elements.schemaTableWrap = document.querySelector("#schema-table-wrap");
  elements.schemaBody = document.querySelector("#schema-body");
  elements.schemaCount = document.querySelector("#schema-count");
  elements.selectionSummary = document.querySelector("#selection-summary");
  elements.formMessage = document.querySelector("#form-message");
  elements.refreshButton = document.querySelector("#refresh-button");
  elements.jobsState = document.querySelector("#jobs-state");
  elements.jobsTableWrap = document.querySelector("#jobs-table-wrap");
  elements.jobsBody = document.querySelector("#jobs-body");
  elements.lastUpdated = document.querySelector("#last-updated");
}

// Creates an element with an optional class name and text value.
function makeElement(tagName, className, text) {
  const element = document.createElement(tagName);
  if (className) {
    element.className = className;
  }
  if (text !== undefined) {
    element.textContent = text;
  }
  return element;
}

// Extracts a useful error message from an API response.
async function responseError(response) {
  const fallback = `${response.status} ${response.statusText || "Request failed"}`;
  try {
    const body = await response.json();
    return body.error || body.message || fallback;
  } catch (_error) {
    return fallback;
  }
}

// Updates the copy/move controls when the source URI changes.
function updateMoveAvailability() {
  const isHuggingFace = elements.source.value.trim().toLowerCase().startsWith("hf://");
  elements.move.disabled = isHuggingFace;
  elements.moveHelp.hidden = !isHuggingFace;
  if (isHuggingFace && elements.move.checked) {
    document.querySelector('input[name="kind"][value="copy"]').checked = true;
  }
  if (state.inspectedSource && state.inspectedSource !== elements.source.value.trim()) {
    state.inspectedSource = "";
    state.fields = [];
    elements.schemaBody.replaceChildren();
    elements.schemaSection.hidden = true;
    elements.selectionSummary.textContent = "Source changed — inspect again to reconfigure columns.";
  }
}

// Converts a backend data type value into readable text.
function formatDataType(value) {
  if (typeof value === "string") {
    return value;
  }
  if (value === null || value === undefined) {
    return "unknown";
  }
  try {
    return JSON.stringify(value);
  } catch (_error) {
    return String(value);
  }
}

// Determines whether a schema field can be ingested as a URL-backed blob.
function isBlobEligible(field) {
  if (typeof field.blob_eligible === "boolean") {
    return field.blob_eligible;
  }
  if (typeof field.is_blob_eligible === "boolean") {
    return field.is_blob_eligible;
  }
  const type = formatDataType(field.data_type ?? field.type).toLowerCase().replace(/[^a-z0-9]/g, "");
  return type === "utf8" || type === "largeutf8" || type === "utf8view" || type === "string";
}

// Normalizes supported schema response shapes into a field array.
function normalizeFields(payload) {
  let fields = [];
  if (Array.isArray(payload)) {
    fields = payload;
  } else if (payload && Array.isArray(payload.fields)) {
    fields = payload.fields;
  } else if (payload && Array.isArray(payload.columns)) {
    fields = payload.columns;
  } else if (payload && payload.schema && Array.isArray(payload.schema.fields)) {
    fields = payload.schema.fields;
  }

  const normalized = [];
  for (let index = 0; index < fields.length; index += 1) {
    const field = fields[index] || {};
    normalized.push({
      name: field.name ?? field.column ?? `column_${index + 1}`,
      dataType: formatDataType(field.data_type ?? field.dataType ?? field.type),
      nullable: field.nullable ?? field.is_nullable ?? true,
      blobEligible: isBlobEligible(field),
    });
  }
  return normalized;
}

// Builds an accessible index type selector for a schema field.
function createIndexSelect(field, index) {
  const select = makeElement("select", "index-select");
  select.dataset.column = field.name;
  select.setAttribute("aria-label", `Index type for ${field.name}`);
  for (let optionIndex = 0; optionIndex < INDEX_TYPES.length; optionIndex += 1) {
    const definition = INDEX_TYPES[optionIndex];
    const option = makeElement("option", "", definition[1]);
    option.value = definition[0];
    select.append(option);
  }
  select.id = `index-${index}`;
  select.addEventListener("change", updateSelectionSummary);
  return select;
}

// Builds the blob control for an eligible or ineligible schema field.
function createBlobControl(field, index) {
  if (!field.blobEligible) {
    return makeElement("span", "not-eligible", "—");
  }
  const checkbox = makeElement("input", "blob-toggle");
  checkbox.type = "checkbox";
  checkbox.id = `blob-${index}`;
  checkbox.dataset.column = field.name;
  checkbox.setAttribute("aria-label", `Treat ${field.name} as a URL-backed blob`);
  checkbox.addEventListener("change", updateSelectionSummary);
  return checkbox;
}

// Renders the inspected schema and its conversion controls.
function renderSchema(fields) {
  elements.schemaBody.replaceChildren();
  elements.schemaCount.textContent = `${fields.length} ${fields.length === 1 ? "column" : "columns"}`;

  if (fields.length === 0) {
    showSchemaState("empty", "The source schema contains no columns.");
    return;
  }

  for (let index = 0; index < fields.length; index += 1) {
    const field = fields[index];
    const row = document.createElement("tr");

    row.append(makeElement("td", "row-index", String(index + 1).padStart(2, "0")));
    row.append(makeElement("td", "column-name", field.name));
    row.append(makeElement("td", "type-name", field.dataType));
    row.append(makeElement("td", field.nullable ? "nullable" : "required", field.nullable ? "nullable" : "required"));

    const blobCell = document.createElement("td");
    blobCell.append(createBlobControl(field, index));
    row.append(blobCell);

    const indexCell = document.createElement("td");
    indexCell.append(createIndexSelect(field, index));
    row.append(indexCell);
    elements.schemaBody.append(row);
  }

  elements.schemaState.hidden = true;
  elements.schemaTableWrap.hidden = false;
  updateSelectionSummary();
}

// Shows a loading, empty, or error message in the schema panel.
function showSchemaState(kind, message) {
  elements.schemaTableWrap.hidden = true;
  elements.schemaState.hidden = false;
  elements.schemaState.className = `state-box ${kind}`;
  elements.schemaState.replaceChildren();
  if (kind === "loading") {
    elements.schemaState.append(makeElement("span", "spinner"));
  }
  elements.schemaState.append(document.createTextNode(message));
}

// Counts selected column options and updates the form summary.
function updateSelectionSummary() {
  const blobs = elements.schemaBody.querySelectorAll(".blob-toggle:checked").length;
  const selects = elements.schemaBody.querySelectorAll(".index-select");
  let indices = 0;
  for (let index = 0; index < selects.length; index += 1) {
    if (selects[index].value !== "none") {
      indices += 1;
    }
  }
  elements.selectionSummary.textContent = `${blobs} blob ${blobs === 1 ? "column" : "columns"} · ${indices} ${indices === 1 ? "index" : "indices"}`;
}

// Inspects the current source and displays its schema.
async function inspectSchema() {
  const sourceUri = elements.source.value.trim();
  if (!sourceUri) {
    elements.source.reportValidity();
    return;
  }

  if (state.inspectController) {
    state.inspectController.abort();
  }
  const controller = new AbortController();
  state.inspectController = controller;
  elements.schemaSection.hidden = false;
  elements.inspectButton.disabled = true;
  elements.inspectButton.textContent = "Inspecting…";
  showSchemaState("loading", "Inspecting source schema…");
  hideFormMessage();

  try {
    const response = await fetch("/v1/sources/inspect", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ source_uri: sourceUri }),
      signal: controller.signal,
    });
    if (!response.ok) {
      throw new Error(await responseError(response));
    }
    const payload = await response.json();
    state.fields = normalizeFields(payload);
    state.inspectedSource = sourceUri;
    renderSchema(state.fields);
  } catch (error) {
    if (error.name !== "AbortError") {
      showSchemaState("error", `Could not inspect schema: ${error.message}`);
    }
  } finally {
    if (state.inspectController === controller) {
      state.inspectController = null;
      elements.inspectButton.disabled = false;
      elements.inspectButton.textContent = "Inspect schema";
    }
  }
}

// Collects selected blob columns from the schema table.
function collectBlobColumns() {
  const selected = elements.schemaBody.querySelectorAll(".blob-toggle:checked");
  const blobColumns = [];
  for (let index = 0; index < selected.length; index += 1) {
    blobColumns.push({ column: selected[index].dataset.column });
  }
  return blobColumns;
}

// Collects selected index definitions from the schema table.
function collectIndices() {
  const selects = elements.schemaBody.querySelectorAll(".index-select");
  const indices = [];
  for (let index = 0; index < selects.length; index += 1) {
    if (selects[index].value !== "none") {
      indices.push({
        columns: [selects[index].dataset.column],
        index_type: selects[index].value,
      });
    }
  }
  return indices;
}

// Displays a success or error message below the conversion form.
function showFormMessage(message, isError) {
  elements.formMessage.hidden = false;
  elements.formMessage.className = isError ? "form-message error" : "form-message";
  elements.formMessage.textContent = message;
}

// Hides and clears the conversion form message.
function hideFormMessage() {
  elements.formMessage.hidden = true;
  elements.formMessage.textContent = "";
}

// Submits a conversion job using the form and selected schema options.
async function submitJob(event) {
  event.preventDefault();
  if (!elements.form.reportValidity()) {
    return;
  }
  if (state.inspectedSource !== elements.source.value.trim()) {
    showFormMessage("Inspect the current source schema before creating the job.", true);
    return;
  }

  const selectedKind = document.querySelector('input[name="kind"]:checked');
  const payload = {
    creator: elements.creator.value.trim(),
    source_uri: elements.source.value.trim(),
    destination_uri: elements.destination.value.trim(),
    kind: selectedKind.value,
    blob_columns: collectBlobColumns(),
    indices: collectIndices(),
  };

  elements.submitButton.disabled = true;
  elements.submitButton.textContent = "Creating job…";
  hideFormMessage();

  try {
    const response = await fetch("/v1/jobs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!response.ok) {
      throw new Error(await responseError(response));
    }
    window.location.assign("/jobs");
  } catch (error) {
    showFormMessage(`Could not create job: ${error.message}`, true);
  } finally {
    elements.submitButton.disabled = false;
    elements.submitButton.textContent = "Create conversion job";
  }
}

// Formats a millisecond timestamp for local display.
function formatTimestamp(timestamp) {
  const value = Number(timestamp);
  if (!Number.isFinite(value)) {
    return "—";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "—";
  }
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "medium",
  }).format(date);
}

// Computes a safe progress percentage for a job.
function progressPercent(job) {
  const progress = job.progress || {};
  const total = Number(progress.rows_total) || 0;
  const completed = Math.max(Number(progress.rows_written) || 0, Number(progress.rows_read) || 0);
  if (job.status === "succeeded") {
    return 100;
  }
  if (total <= 0) {
    return 0;
  }
  return Math.min(100, Math.max(0, (completed / total) * 100));
}

// Builds the progress visualization for one job.
function createProgressCell(job) {
  const cell = makeElement("td", "progress-cell");
  const progress = job.progress || {};
  const read = Number(progress.rows_read) || 0;
  const written = Number(progress.rows_written) || 0;
  const total = Number(progress.rows_total) || 0;
  const track = makeElement("div", "progress-track");
  const bar = makeElement("div", "progress-bar");
  const percent = progressPercent(job);
  bar.style.width = `${percent}%`;
  track.setAttribute("role", "progressbar");
  track.setAttribute("aria-label", "Job row progress");
  track.setAttribute("aria-valuemin", "0");
  track.setAttribute("aria-valuemax", String(total || 100));
  track.setAttribute("aria-valuenow", String(total ? Math.max(read, written) : percent));
  track.append(bar);
  cell.append(track);
  cell.append(makeElement("span", "progress-label", `${read.toLocaleString()} read · ${written.toLocaleString()} written · ${total.toLocaleString()} total`));
  return cell;
}

// Builds the source and destination route cell for one job.
function createRouteCell(job) {
  const cell = makeElement("td", "route-cell");
  const source = makeElement("span", "uri", job.source_uri || "—");
  source.title = job.source_uri || "";
  const destination = makeElement("span", "uri", job.destination_uri || "—");
  destination.title = job.destination_uri || "";
  cell.append(source, makeElement("span", "route-arrow", `↓ ${String(job.kind || "copy").toUpperCase()}`), destination);
  return cell;
}

// Builds the expandable error summary for one job.
function createErrorCell(job) {
  const cell = makeElement("td", "error-list");
  const errors = Array.isArray(job.error_reasons) ? job.error_reasons : [];
  if (errors.length === 0) {
    cell.append(makeElement("span", "no-errors", "None"));
    return cell;
  }

  const details = document.createElement("details");
  const summary = makeElement("summary", "", `${errors.length} ${errors.length === 1 ? "error" : "errors"}`);
  details.append(summary);
  for (let index = errors.length - 1; index >= 0; index -= 1) {
    const error = errors[index] || {};
    const reason = error.reason || error.error || String(error);
    details.append(makeElement("p", "", `Attempt ${error.attempt ?? "—"} · ${reason}`));
  }
  cell.append(details);
  return cell;
}

// Renders all jobs into the dashboard table.
function renderJobs(jobs) {
  elements.jobsBody.replaceChildren();
  if (jobs.length === 0) {
    elements.jobsTableWrap.hidden = true;
    elements.jobsState.hidden = false;
    elements.jobsState.className = "state-box empty";
    elements.jobsState.textContent = "No conversion jobs yet. Create one above to get started.";
    return;
  }

  for (let index = 0; index < jobs.length; index += 1) {
    const job = jobs[index];
    const row = document.createElement("tr");
    const statusCell = document.createElement("td");
    statusCell.append(makeElement("span", `status-pill ${job.status || ""}`, job.status || "unknown"));
    row.append(statusCell);
    row.append(createProgressCell(job));
    row.append(createRouteCell(job));
    row.append(makeElement("td", "time-cell", formatTimestamp(job.creation_timestamp_ms)));
    row.append(makeElement("td", "time-cell", formatTimestamp(job.update_timestamp_ms)));
    row.append(makeElement("td", "attempt-cell", String(job.attempt ?? 0)));
    row.append(createErrorCell(job));
    elements.jobsBody.append(row);
  }

  elements.jobsState.hidden = true;
  elements.jobsTableWrap.hidden = false;
}

// Loads the latest jobs while preventing overlapping poll requests.
async function loadJobs() {
  if (state.jobsLoading) {
    return;
  }
  state.jobsLoading = true;
  elements.refreshButton.disabled = true;
  if (elements.jobsBody.children.length === 0) {
    elements.jobsState.hidden = false;
    elements.jobsState.className = "state-box";
    elements.jobsState.replaceChildren(makeElement("span", "spinner"), document.createTextNode("Loading jobs…"));
  }

  try {
    const response = await fetch("/v1/jobs", {
      headers: { Accept: "application/json" },
      cache: "no-store",
    });
    if (!response.ok) {
      throw new Error(await responseError(response));
    }
    const payload = await response.json();
    if (!Array.isArray(payload)) {
      throw new Error("The jobs response was not a list.");
    }
    renderJobs(payload);
    elements.lastUpdated.textContent = `Updated ${new Date().toLocaleTimeString()}`;
  } catch (error) {
    elements.jobsTableWrap.hidden = true;
    elements.jobsState.hidden = false;
    elements.jobsState.className = "state-box error";
    elements.jobsState.textContent = `Could not load jobs: ${error.message}`;
    elements.lastUpdated.textContent = "Refresh failed";
  } finally {
    state.jobsLoading = false;
    elements.refreshButton.disabled = false;
  }
}

// Initializes event handlers, the first job load, and polling.
function initialize() {
  cacheElements();
  if (elements.form) {
    elements.source.addEventListener("input", updateMoveAvailability);
    elements.inspectButton.addEventListener("click", inspectSchema);
    elements.form.addEventListener("submit", submitJob);
    updateMoveAvailability();
  }
  if (elements.jobsBody) {
    elements.refreshButton.addEventListener("click", loadJobs);
    loadJobs();
    window.setInterval(loadJobs, 3000);
  }
}

document.addEventListener("DOMContentLoaded", initialize);

"use strict";

const INDEX_TYPES = [
  ["none", "None"],
  ["scalar", "Scalar — B-tree for exact and range filters"],
  ["text", "Text — inverted index for full-text search"],
  ["vector", "Vector — IVF-PQ for similarity search"],
];

const state = {
  inspectedSource: "",
  fields: [],
  jobsLoading: false,
  jobsQuery: "",
  expandedJobs: new Set(),
  inspectController: null,
  hfHydratedFrom: "",
};

const elements = {};

// Caches the DOM elements used throughout the interface.
function cacheElements() {
  elements.form = document.querySelector("#job-form");
  elements.creator = document.querySelector("#creator");
  elements.source = document.querySelector("#source-uri");
  elements.sourceHelp = document.querySelector("#source-help");
  elements.hfSourceFields = document.querySelector("#hf-source-fields");
  elements.hfConfig = document.querySelector("#hf-config");
  elements.hfSplit = document.querySelector("#hf-split");
  elements.hfRevision = document.querySelector("#hf-revision");
  elements.destination = document.querySelector("#destination-uri");
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
  elements.jobFilters = document.querySelector("#job-filters");
  elements.filterCreator = document.querySelector("#filter-creator");
  elements.filterStatusInputs = document.querySelectorAll('input[name="job-status"]');
  elements.filterCreatedFrom = document.querySelector("#filter-created-from");
  elements.filterCreatedTo = document.querySelector("#filter-created-to");
  elements.clearFilters = document.querySelector("#clear-filters");
  elements.createdSortHeader = document.querySelector("#created-sort-header");
  elements.createdSort = document.querySelector("#created-sort");
  elements.createdSortDirection = document.querySelector("#created-sort-direction");
  elements.updatedSortHeader = document.querySelector("#updated-sort-header");
  elements.updatedSort = document.querySelector("#updated-sort");
  elements.updatedSortDirection = document.querySelector("#updated-sort-direction");
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

// Returns owner/name for an hf:// or huggingface.co dataset URI.
function huggingfaceDatasetPath(value) {
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  if (trimmed.toLowerCase().startsWith("hf://")) {
    try {
      const url = new URL(trimmed);
      if (url.hostname !== "datasets") {
        return null;
      }
      const path = url.pathname.replace(/^\/+|\/+$/g, "").split("@")[0];
      const parts = path.split("/").filter(Boolean);
      return parts.length >= 2 ? `${parts[0]}/${parts[1]}` : null;
    } catch (_error) {
      return null;
    }
  }
  const hub = trimmed.match(
    /^(?:https?:\/\/)?(?:www\.)?huggingface\.co\/datasets\/([^/?#]+\/[^/?#]+)/i,
  );
  return hub ? hub[1] : null;
}

function isHfSource(value) {
  return huggingfaceDatasetPath(value) !== null;
}

// Reads revision, config, and split from a full hf:// URI.
function parseHfFields(value) {
  const parsed = { revision: "main", config: "", split: "" };
  const trimmed = value.trim();
  if (!trimmed.toLowerCase().startsWith("hf://")) {
    return parsed;
  }
  try {
    const url = new URL(trimmed);
    const path = url.pathname.replace(/^\/+|\/+$/g, "");
    const at = path.lastIndexOf("@");
    if (at !== -1 && path.slice(at + 1)) {
      parsed.revision = path.slice(at + 1);
    }
    parsed.config = url.searchParams.get("config") || "";
    parsed.split = url.searchParams.get("split") || "";
  } catch (_error) {
    return parsed;
  }
  return parsed;
}

// Builds the inspect/submit URI, adding HF query fields when present.
function composeSourceUri() {
  const raw = elements.source.value.trim();
  const dataset = huggingfaceDatasetPath(raw);
  if (!dataset) {
    return raw;
  }
  const revision = (elements.hfRevision && elements.hfRevision.value.trim()) || "main";
  const config = elements.hfConfig ? elements.hfConfig.value.trim() : "";
  const split = elements.hfSplit ? elements.hfSplit.value.trim() : "";
  const params = new URLSearchParams();
  if (config) {
    params.set("config", config);
  }
  if (split) {
    params.set("split", split);
  }
  const query = params.toString();
  return `hf://datasets/${dataset}@${revision}${query ? `?${query}` : ""}`;
}

// Copies @revision / query params from the source URI into the HF fields.
function syncHfFieldsFromSource() {
  if (!elements.hfConfig || !elements.hfSplit || !elements.hfRevision) {
    return;
  }
  const raw = elements.source.value.trim();
  if (!isHfSource(raw) || state.hfHydratedFrom === raw) {
    return;
  }
  state.hfHydratedFrom = raw;
  if (!raw.toLowerCase().startsWith("hf://") || (!raw.includes("@") && !raw.includes("?"))) {
    return;
  }
  const parsed = parseHfFields(raw);
  if (raw.includes("@")) {
    elements.hfRevision.value = parsed.revision;
  }
  if (/(?:[?&])config=/.test(raw)) {
    elements.hfConfig.value = parsed.config;
  }
  if (/(?:[?&])split=/.test(raw)) {
    elements.hfSplit.value = parsed.split;
  }
}

// Shows HF config/split/revision only for Hugging Face sources.
function updateHfFields() {
  const detected = isHfSource(elements.source.value);
  if (elements.hfSourceFields) {
    elements.hfSourceFields.hidden = !detected;
  }
  if (elements.sourceHelp) {
    elements.sourceHelp.textContent = detected
      ? "Hugging Face dataset detected. Set config, split, and revision, then inspect."
      : "Supported location schemes depend on service configuration.";
  }
  syncHfFieldsFromSource();
}

// Invalidates schema-derived controls when the composed source URI changes.
function updateSourceInspection() {
  updateHfFields();
  const composed = composeSourceUri();
  if (state.inspectedSource && state.inspectedSource !== composed) {
    state.inspectedSource = "";
    state.fields = [];
    elements.schemaBody.replaceChildren();
    elements.schemaSection.hidden = true;
    elements.selectionSummary.textContent = "Source changed — inspect again to reconfigure columns.";
  }
}

// Maps the source-inspection API response into the view model.
function normalizeFields(payload) {
  if (!payload || !Array.isArray(payload.columns)) {
    throw new Error("The source inspection response did not contain columns.");
  }

  return payload.columns.map((field) => ({
    name: field.name,
    dataType: field.data_type,
    nullable: field.nullable,
    blobEligible: field.blob_eligible,
  }));
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
  const sourceUri = composeSourceUri();
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
        column: selects[index].dataset.column,
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
  const sourceUri = composeSourceUri();
  if (state.inspectedSource !== sourceUri) {
    showFormMessage("Inspect the current source schema before creating the job.", true);
    return;
  }

  const payload = {
    creator: elements.creator.value.trim(),
    source_uri: sourceUri,
    destination_uri: elements.destination.value.trim(),
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

// Formats elapsed milliseconds as a compact human-readable duration.
function formatDuration(durationMs) {
  const totalSeconds = Math.max(0, Math.floor(durationMs / 1000));
  if (totalSeconds < 1) {
    return "<1s";
  }
  const days = Math.floor(totalSeconds / 86_400);
  const hours = Math.floor((totalSeconds % 86_400) / 3_600);
  const minutes = Math.floor((totalSeconds % 3_600) / 60);
  const seconds = totalSeconds % 60;
  if (days > 0) {
    return `${days}d ${hours}h`;
  }
  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }
  if (minutes > 0) {
    return `${minutes}m ${seconds}s`;
  }
  return `${seconds}s`;
}

// Computes total wall-clock time from job creation through completion or now.
function formatJobDuration(job) {
  const created = Number(job.creation_timestamp_ms);
  const terminal = job.status === "succeeded" || job.status === "failed";
  const ended = terminal ? Number(job.update_timestamp_ms) : Date.now();
  if (!Number.isFinite(created) || !Number.isFinite(ended)) {
    return "—";
  }
  return formatDuration(ended - created);
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
  const missingBlobs = Number(progress.rows_missing_blobs) || 0;
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
  cell.append(makeElement(
    "span",
    "progress-label",
    `${read.toLocaleString()} read · ${written.toLocaleString()} written · ${missingBlobs.toLocaleString()} missing-blob rows · ${total.toLocaleString()} total`,
  ));
  return cell;
}

// Builds the source and destination route cell for one job.
function createRouteCell(job) {
  const cell = makeElement("td", "route-cell");
  const source = makeElement("span", "uri", job.source_uri || "—");
  source.title = job.source_uri || "";
  const destination = makeElement("span", "uri", job.destination_uri || "—");
  destination.title = job.destination_uri || "";
  cell.append(source, makeElement("span", "route-arrow", "↓ COPY"), destination);
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
    const occurredAt = formatTimestamp(error.error_timestamp_ms);
    details.append(
      makeElement("p", "", `Attempt ${error.attempt ?? "—"} · ${occurredAt} · ${reason}`),
    );
  }
  cell.append(details);
  return cell;
}

// Renders one labeled list of job conversion options.
function createSpecGroup(label, values) {
  const group = makeElement("div", "job-spec-group");
  group.append(makeElement("span", "job-spec-label", label));
  const list = makeElement("div", "job-spec-list");
  if (values.length === 0) {
    list.append(makeElement("span", "job-spec-empty", "None"));
  } else {
    for (const value of values) {
      list.append(makeElement("span", "job-spec-pill", value));
    }
  }
  group.append(list);
  return group;
}

// Builds the expandable blob-column and index detail row for one job.
function createJobDetailsRow(job, rowId) {
  const row = makeElement("tr", "job-details-row");
  row.id = rowId;
  row.hidden = !state.expandedJobs.has(job.destination_uri);
  const cell = document.createElement("td");
  cell.colSpan = 10;
  const content = makeElement("div", "job-details-content");
  const blobs = Array.isArray(job.blob_columns)
    ? job.blob_columns.map((spec) => spec.column)
    : [];
  const indices = Array.isArray(job.indices)
    ? job.indices.map((spec) => `${spec.index_type} · ${spec.column}`)
    : [];
  const missingBlobRows = Number(job.progress?.rows_missing_blobs) || 0;
  content.append(
    createSpecGroup("Blob columns", blobs),
    createSpecGroup("Indexes", indices),
    createSpecGroup("Missing blob rows", [String(missingBlobRows)]),
  );
  cell.append(content);
  row.append(cell);
  return row;
}

// Builds a button that toggles one job's conversion details.
function createDetailsCell(job, detailsRow) {
  const cell = makeElement("td", "details-cell");
  const expanded = state.expandedJobs.has(job.destination_uri);
  const button = makeElement("button", "details-button", expanded ? "Hide details" : "View details");
  button.type = "button";
  button.setAttribute("aria-expanded", String(expanded));
  button.setAttribute("aria-controls", detailsRow.id);
  button.addEventListener("click", () => {
    const shouldExpand = !state.expandedJobs.has(job.destination_uri);
    if (shouldExpand) {
      state.expandedJobs.add(job.destination_uri);
    } else {
      state.expandedJobs.delete(job.destination_uri);
    }
    detailsRow.hidden = !shouldExpand;
    button.textContent = shouldExpand ? "Hide details" : "View details";
    button.setAttribute("aria-expanded", String(shouldExpand));
  });
  cell.append(button);
  return cell;
}

// Renders all jobs into the dashboard table.
function renderJobs(jobs) {
  elements.jobsBody.replaceChildren();
  if (jobs.length === 0) {
    elements.jobsTableWrap.hidden = true;
    elements.jobsState.hidden = false;
    elements.jobsState.className = "state-box empty";
    elements.jobsState.textContent = state.jobsQuery
      ? "No conversion jobs match the selected filters."
      : "No conversion jobs yet. Schedule one to get started.";
    return;
  }

  for (let index = 0; index < jobs.length; index += 1) {
    const job = jobs[index];
    const detailsRow = createJobDetailsRow(job, `job-details-${index}`);
    const row = document.createElement("tr");
    const statusCell = document.createElement("td");
    statusCell.append(makeElement("span", `status-pill ${job.status || ""}`, job.status || "unknown"));
    row.append(statusCell);
    row.append(makeElement("td", "creator-cell", job.creator || "—"));
    row.append(createDetailsCell(job, detailsRow));
    row.append(createProgressCell(job));
    row.append(createRouteCell(job));
    row.append(makeElement("td", "time-cell", formatTimestamp(job.creation_timestamp_ms)));
    row.append(makeElement("td", "time-cell", formatTimestamp(job.update_timestamp_ms)));
    row.append(makeElement("td", "duration-cell", formatJobDuration(job)));
    row.append(makeElement("td", "attempt-cell", String(job.attempt ?? 0)));
    row.append(createErrorCell(job));
    elements.jobsBody.append(row, detailsRow);
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
    const url = state.jobsQuery ? `/v1/jobs?${state.jobsQuery}` : "/v1/jobs";
    const response = await fetch(url, {
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

// Formats a timestamp for a datetime-local filter in the browser's timezone.
function timestampToLocalInput(timestamp) {
  if (!timestamp) {
    return "";
  }
  const date = new Date(Number(timestamp));
  if (Number.isNaN(date.getTime())) {
    return "";
  }
  const local = new Date(date.getTime() - date.getTimezoneOffset() * 60_000);
  return local.toISOString().slice(0, 16);
}

// Returns the active server-side job ordering from the current query.
function currentJobSort() {
  const query = new URLSearchParams(state.jobsQuery);
  return {
    field: query.get("order_by") === "update" ? "update" : "creation",
    order: query.get("order") === "asc" ? "asc" : "desc",
  };
}

// Updates both sortable timestamp headers to reflect the active ordering.
function updateSortControls() {
  const sort = currentJobSort();
  const createdActive = sort.field === "creation";
  const updatedActive = sort.field === "update";
  elements.createdSortHeader.setAttribute(
    "aria-sort",
    createdActive ? (sort.order === "desc" ? "descending" : "ascending") : "none",
  );
  elements.updatedSortHeader.setAttribute(
    "aria-sort",
    updatedActive ? (sort.order === "desc" ? "descending" : "ascending") : "none",
  );
  elements.createdSortDirection.textContent = createdActive
    ? sort.order === "desc"
      ? "↓"
      : "↑"
    : "";
  elements.updatedSortDirection.textContent = updatedActive
    ? sort.order === "desc"
      ? "↓"
      : "↑"
    : "";
}

// Selects a timestamp sort or reverses the currently selected direction.
function toggleJobSort(field) {
  const query = new URLSearchParams(state.jobsQuery);
  const current = currentJobSort();
  const order = current.field === field && current.order === "desc" ? "asc" : "desc";
  if (field === "creation") {
    query.delete("order_by");
  } else {
    query.set("order_by", "update");
  }
  if (order === "desc") {
    query.delete("order");
  } else {
    query.set("order", "asc");
  }
  state.jobsQuery = query.toString();
  window.history.replaceState(null, "", state.jobsQuery ? `/jobs?${state.jobsQuery}` : "/jobs");
  updateSortControls();
  loadJobs();
}

// Restores job filters from the current page URL.
function restoreJobFilters() {
  const query = new URLSearchParams(window.location.search);
  elements.filterCreator.value = query.get("creator") || "";
  let status = "all";
  if (query.get("ongoing_only") === "true") {
    status = "ongoing";
  } else if (query.get("failed_only") === "true") {
    status = "failed";
  }
  for (const input of elements.filterStatusInputs) {
    input.checked = input.value === status;
  }
  elements.filterCreatedFrom.value = timestampToLocalInput(
    query.get("creation_timestamp_ms_from"),
  );
  elements.filterCreatedTo.value = timestampToLocalInput(
    query.get("creation_timestamp_ms_to"),
  );
  state.jobsQuery = query.toString();
  updateSortControls();
}

// Converts one datetime-local value to a millisecond timestamp.
function localInputToTimestamp(value) {
  if (!value) {
    return null;
  }
  const timestamp = new Date(value).getTime();
  return Number.isFinite(timestamp) ? timestamp : null;
}

// Applies the current job filters and persists them in the page URL.
function applyJobFilters(event) {
  event.preventDefault();
  const from = localInputToTimestamp(elements.filterCreatedFrom.value);
  const to = localInputToTimestamp(elements.filterCreatedTo.value);
  elements.filterCreatedTo.setCustomValidity(
    from !== null && to !== null && from > to
      ? "Created to must be later than created from."
      : "",
  );
  if (!elements.jobFilters.reportValidity()) {
    return;
  }

  const currentSort = currentJobSort();
  const query = new URLSearchParams();
  const creator = elements.filterCreator.value.trim();
  if (creator) {
    query.set("creator", creator);
  }
  const status = document.querySelector('input[name="job-status"]:checked').value;
  if (status === "failed") {
    query.set("failed_only", "true");
  } else if (status === "ongoing") {
    query.set("ongoing_only", "true");
  }
  if (from !== null) {
    query.set("creation_timestamp_ms_from", String(from));
  }
  if (to !== null) {
    query.set("creation_timestamp_ms_to", String(to));
  }
  if (currentSort.field === "update") {
    query.set("order_by", "update");
  }
  if (currentSort.order === "asc") {
    query.set("order", "asc");
  }
  state.jobsQuery = query.toString();
  window.history.replaceState(null, "", state.jobsQuery ? `/jobs?${state.jobsQuery}` : "/jobs");
  loadJobs();
}

// Clears every job filter and reloads the unfiltered job list.
function clearJobFilters() {
  elements.jobFilters.reset();
  elements.filterCreatedTo.setCustomValidity("");
  state.jobsQuery = "";
  window.history.replaceState(null, "", "/jobs");
  updateSortControls();
  loadJobs();
}

// Initializes event handlers, the first job load, and polling.
function initialize() {
  cacheElements();
  if (elements.form) {
    elements.source.addEventListener("input", updateSourceInspection);
    if (elements.hfConfig) {
      elements.hfConfig.addEventListener("input", updateSourceInspection);
    }
    if (elements.hfSplit) {
      elements.hfSplit.addEventListener("input", updateSourceInspection);
    }
    if (elements.hfRevision) {
      elements.hfRevision.addEventListener("input", updateSourceInspection);
    }
    elements.inspectButton.addEventListener("click", inspectSchema);
    elements.form.addEventListener("submit", submitJob);
    updateSourceInspection();
  }
  if (elements.jobsBody) {
    restoreJobFilters();
    elements.refreshButton.addEventListener("click", loadJobs);
    elements.jobFilters.addEventListener("submit", applyJobFilters);
    elements.clearFilters.addEventListener("click", clearJobFilters);
    elements.createdSort.addEventListener("click", () => toggleJobSort("creation"));
    elements.updatedSort.addEventListener("click", () => toggleJobSort("update"));
    loadJobs();
    window.setInterval(loadJobs, 3000);
  }
}

document.addEventListener("DOMContentLoaded", initialize);

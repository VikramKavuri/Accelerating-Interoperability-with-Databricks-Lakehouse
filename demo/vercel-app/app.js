const dataRoot = "./public/data";

const tableGroups = {
  Bronze: ["bronze_fhir_resources", "bronze_ingestion_audit", "bronze_rejected_records"],
  Silver: [
    "silver_patient",
    "silver_encounter",
    "silver_condition",
    "silver_observation",
    "silver_medication",
    "silver_claim",
    "silver_procedure",
    "silver_provider",
  ],
  Quality: ["quality_check_results", "quality_failed_records", "quality_run_summary"],
  OMOP: [
    "omop_person",
    "omop_visit_occurrence",
    "omop_condition_occurrence",
    "omop_measurement",
    "omop_drug_exposure",
    "omop_procedure_occurrence",
    "omop_payer_plan_period",
  ],
  Gold: [
    "gold_patient_summary",
    "gold_condition_prevalence",
    "gold_encounter_utilization",
    "gold_claim_cost_summary",
    "gold_medication_usage",
    "gold_population_health_cohort",
  ],
};

const state = {
  tables: {},
  manifest: null,
};

function formatTableName(name) {
  return name.replaceAll("_", " ");
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function compactValue(value) {
  if (value === null || value === undefined) return "";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

async function fetchJson(path) {
  const response = await fetch(`${dataRoot}/${path}`);
  if (!response.ok) throw new Error(`Unable to load ${path}`);
  return response.json();
}

async function loadDemoData() {
  state.manifest = await fetchJson("manifest.json");
  await Promise.all(
    state.manifest.tables.map(async (table) => {
      state.tables[table] = await fetchJson(`${table}.json`);
    }),
  );
}

function tableCount(names) {
  return names.reduce((total, name) => total + (state.tables[name]?.length || 0), 0);
}

function renderRunLine() {
  const summary = state.tables.run_summary?.[0];
  const quality = summary?.quality;
  document.getElementById("run-line").textContent = summary
    ? `Batch ${summary.batch_id} from ${summary.source}`
    : "No run summary exported";

  const badge = document.getElementById("quality-badge");
  if (!quality) return;
  const failed = quality.checks_failed || 0;
  badge.textContent = failed ? `${failed} quality gate failed` : "All quality gates passed";
  badge.classList.toggle("pass", failed === 0);
  badge.classList.toggle("fail", failed > 0);
}

function renderMetrics() {
  const summary = state.tables.run_summary?.[0];
  const quality = summary?.quality || {};
  const metrics = [
    ["Resources", summary?.table_counts?.bronze_fhir_resources || 0],
    ["Patients", summary?.table_counts?.silver_patient || 0],
    ["OMOP rows", tableCount(tableGroups.OMOP)],
    ["Quality failures", quality.failed_record_count || 0],
  ];

  document.getElementById("metrics").innerHTML = metrics
    .map(
      ([label, value]) =>
        `<article class="metric"><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong></article>`,
    )
    .join("");

  document.getElementById("bronze-count").textContent = summary?.table_counts?.bronze_fhir_resources || 0;
  document.getElementById("silver-count").textContent = tableCount(tableGroups.Silver);
  document.getElementById("quality-count").textContent = summary?.quality?.checks_run || 0;
  document.getElementById("omop-count").textContent = tableCount(tableGroups.OMOP);
  document.getElementById("gold-count").textContent = tableCount(tableGroups.Gold);
}

function renderTableOptions() {
  const select = document.getElementById("table-select");
  select.innerHTML = Object.entries(tableGroups)
    .map(([group, tables]) => {
      const options = tables
        .filter((table) => state.tables[table])
        .map((table) => `<option value="${escapeHtml(table)}">${escapeHtml(formatTableName(table))}</option>`)
        .join("");
      return `<optgroup label="${escapeHtml(group)}">${options}</optgroup>`;
    })
    .join("");
  select.value = "gold_patient_summary";
  select.addEventListener("change", () => renderTable(select.value));
  renderTable(select.value);
}

function renderTable(tableName) {
  const rows = state.tables[tableName] || [];
  const table = document.getElementById("data-table");
  document.getElementById("table-title").textContent = formatTableName(tableName);
  document.getElementById("row-count").textContent = `${rows.length} rows`;

  if (!rows.length) {
    table.innerHTML = "<tbody><tr><td>No rows exported for this table.</td></tr></tbody>";
    return;
  }

  const columns = Object.keys(rows[0]).slice(0, 8);
  const header = columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("");
  const body = rows
    .slice(0, 25)
    .map(
      (row) =>
        `<tr>${columns.map((column) => `<td>${escapeHtml(compactValue(row[column]))}</td>`).join("")}</tr>`,
    )
    .join("");
  table.innerHTML = `<thead><tr>${header}</tr></thead><tbody>${body}</tbody>`;
}

function renderBarList(elementId, rows, labelKey, valueKey, className = "") {
  const max = Math.max(...rows.map((row) => Number(row[valueKey]) || 0), 1);
  document.getElementById(elementId).innerHTML = rows
    .map((row) => {
      const value = Number(row[valueKey]) || 0;
      const width = Math.max(4, (value / max) * 100);
      return `
        <div class="bar-row">
          <span>${escapeHtml(compactValue(row[labelKey]))}</span>
          <div class="bar-track"><div class="bar-fill ${className}" style="width:${width}%"></div></div>
          <strong>${escapeHtml(value)}</strong>
        </div>
      `;
    })
    .join("");
}

function renderAnalytics() {
  renderBarList(
    "condition-bars",
    state.tables.gold_condition_prevalence || [],
    "condition_display",
    "patient_count",
  );
  renderBarList(
    "claim-bars",
    state.tables.gold_claim_cost_summary || [],
    "condition_display",
    "total_claim_amount",
    "cost",
  );
}

function renderQuality() {
  const rows = state.tables.quality_check_results || [];
  const failed = rows.reduce((total, row) => total + Number(row.failed_count || 0), 0);
  document.getElementById("quality-summary").textContent = `${failed} failed records`;
  document.getElementById("quality-list").innerHTML = rows
    .map(
      (row) => `
        <div class="quality-item">
          <span>${formatTableName(row.check_name)}</span>
          <span class="status ${escapeHtml(row.status)}">${escapeHtml(row.status)} ${escapeHtml(row.failed_count)}</span>
        </div>
      `,
    )
    .join("");
}

async function previewUploadedFiles(files) {
  if (!files.length) {
    document.getElementById("upload-preview").textContent = "No local file selected";
    return;
  }

  let resourceCount = 0;
  for (const file of files) {
    const text = await file.text();
    if (file.name.endsWith(".ndjson")) {
      resourceCount += text.split(/\r?\n/).filter(Boolean).length;
      continue;
    }
    try {
      const document = JSON.parse(text);
      resourceCount += document.resourceType === "Bundle" ? document.entry?.length || 0 : 1;
    } catch {
      resourceCount += 0;
    }
  }
  document.getElementById("upload-preview").textContent =
    `${files.length} local files selected, ${resourceCount} FHIR resources detected`;
}

async function boot() {
  try {
    await loadDemoData();
    renderRunLine();
    renderMetrics();
    renderTableOptions();
    renderAnalytics();
    renderQuality();
    document.getElementById("file-input").addEventListener("change", (event) => {
      previewUploadedFiles([...event.target.files]);
    });
  } catch (error) {
    document.body.innerHTML = `<main class="panel"><h1>Demo data failed to load</h1><p>${escapeHtml(error.message)}</p></main>`;
  }
}

boot();

const API = window.API_BASE_URL;
const charts = {};
const state = { buildingType: "", hvacSystem: "" };

function qs() {
  const p = new URLSearchParams();
  if (state.buildingType) p.set("buildingType", state.buildingType);
  if (state.hvacSystem) p.set("hvacSystem", state.hvacSystem);
  return p.toString() ? `?${p.toString()}` : "";
}

async function getJSON(path) {
  const res = await fetch(`${API}${path}${qs()}`);
  if (!res.ok) throw new Error(`${path} failed (${res.status})`);
  return res.json();
}

function fmtNumber(n, digits = 0) {
  if (n === undefined || n === null || Number.isNaN(n)) return "–";
  return n.toLocaleString(undefined, { maximumFractionDigits: digits });
}

function setStatus(ok, msg) {
  document.getElementById("statusDot").className = `status-dot ${ok ? "ok" : "error"}`;
  document.getElementById("refreshTime").textContent = msg;
}

function showError(msg) {
  const el = document.getElementById("errorBanner");
  el.hidden = false;
  el.textContent = msg;
}
function clearError() {
  document.getElementById("errorBanner").hidden = true;
}

function renderKpis({ totalEnergy, avgEnergyPerArea, buildingTypes, avgHealthScore, atRiskCount }) {
  const cards = [
    { icon: "⚡", color: "#3b82f6", label: "Total Energy Consumption", value: `${fmtNumber(totalEnergy)} kWh` },
    { icon: "📈", color: "#22c55e", label: "Average Energy per Area", value: `${fmtNumber(avgEnergyPerArea, 1)} kWh/m²` },
    { icon: "🏢", color: "#8b5cf6", label: "Building Types Tracked", value: fmtNumber(buildingTypes) },
    { icon: "❤", color: "#ef4444", label: "Average Health Score", value: `${fmtNumber(avgHealthScore, 0)} /100` },
    { icon: "⚠", color: "#f59e0b", label: "Records at Risk", value: fmtNumber(atRiskCount) },
  ];
  document.getElementById("kpiRow").innerHTML = cards
    .map(
      (c) => `
      <div class="kpi-card">
        <div class="kpi-icon" style="background:${c.color}22;color:${c.color}">${c.icon}</div>
        <div>
          <div class="kpi-label">${c.label}</div>
          <div class="kpi-value">${c.value}</div>
        </div>
      </div>`
    )
    .join("");
}

function barChart(id, labels, data, color) {
  charts[id]?.destroy();
  charts[id] = new Chart(document.getElementById(id), {
    type: "bar",
    data: { labels, datasets: [{ data, backgroundColor: color, borderRadius: 4 }] },
    options: {
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: "#8791ab" }, grid: { display: false } },
        y: { ticks: { color: "#8791ab" }, grid: { color: "#1e2740" } },
      },
    },
  });
}

function doughnutChart(id, labels, data, colors) {
  charts[id]?.destroy();
  charts[id] = new Chart(document.getElementById(id), {
    type: "doughnut",
    data: { labels, datasets: [{ data, backgroundColor: colors, borderWidth: 0 }] },
    options: { plugins: { legend: { display: false } }, cutout: "65%" },
  });
}

function scatterChart(id, points) {
  charts[id]?.destroy();
  charts[id] = new Chart(document.getElementById(id), {
    type: "scatter",
    data: { datasets: [{ data: points.map((p) => ({ x: p.area, y: p.energy })), backgroundColor: "#38bdf8" }] },
    options: {
      plugins: { legend: { display: false } },
      scales: {
        x: { title: { display: true, text: "Room Area (m²)", color: "#8791ab" }, ticks: { color: "#8791ab" }, grid: { color: "#1e2740" } },
        y: { title: { display: true, text: "Energy Consumption (kWh)", color: "#8791ab" }, ticks: { color: "#8791ab" }, grid: { color: "#1e2740" } },
      },
    },
  });
}

function lineChart(id, series) {
  charts[id]?.destroy();
  charts[id] = new Chart(document.getElementById(id), {
    type: "line",
    data: {
      labels: series.map((s) => s.index),
      datasets: [
        { label: "Actual", data: series.map((s) => s.actual), borderColor: "#3b82f6", pointRadius: 0, tension: 0.3 },
        { label: "Predicted", data: series.map((s) => s.predicted), borderColor: "#f59e0b", pointRadius: 0, tension: 0.3 },
      ],
    },
    options: {
      plugins: { legend: { labels: { color: "#e7ebf5" } } },
      scales: {
        x: { ticks: { display: false }, grid: { display: false } },
        y: { ticks: { color: "#8791ab" }, grid: { color: "#1e2740" } },
      },
    },
  });
}

function gaugeChart(id, valuePercent) {
  const clamped = Math.max(-50, Math.min(50, valuePercent));
  const pct = (clamped + 50) / 100; // 0..1
  charts[id]?.destroy();
  charts[id] = new Chart(document.getElementById(id), {
    type: "doughnut",
    data: {
      datasets: [
        {
          data: [pct, 1 - pct],
          backgroundColor: ["#22c55e", "#1e2740"],
          borderWidth: 0,
          circumference: 180,
          rotation: 270,
        },
      ],
    },
    options: { plugins: { legend: { display: false } }, cutout: "75%" },
  });
  document.getElementById("gaugeValue").textContent = `${clamped.toFixed(1)}%`;
}

function renderHealthLegend(rows) {
  const colors = { Healthy: "#22c55e", Warning: "#f59e0b", Critical: "#ef4444" };
  const total = rows.reduce((s, r) => s + r.value, 0) || 1;
  document.getElementById("healthLegend").innerHTML = rows
    .map(
      (r) => `<div class="legend-item"><span class="legend-dot" style="background:${colors[r.name] || "#8791ab"}"></span>
        ${r.name} (${Math.round((r.value / total) * 100)}%) — ${r.value} records</div>`
    )
    .join("");
}

function renderTopTable(rows) {
  const riskClass = { Low: "risk-low", Medium: "risk-medium", High: "risk-high" };
  document.querySelector("#topTable tbody").innerHTML = rows
    .map(
      (r) => `<tr>
        <td>${r.buildingType}</td>
        <td>${fmtNumber(r.totalEnergy)}</td>
        <td>${r.energyPerArea}</td>
        <td>${r.healthScore}</td>
        <td><span class="risk-pill ${riskClass[r.riskLevel]}">${r.riskLevel}</span></td>
      </tr>`
    )
    .join("");
}

function renderAccuracy({ mae, rmse, mape, r2 }) {
  const items = [
    { label: "MAE (kWh)", value: fmtNumber(mae, 2) },
    { label: "RMSE (kWh)", value: fmtNumber(rmse, 2) },
    { label: "R² Score", value: fmtNumber(r2, 2) },
    { label: "MAPE", value: `${fmtNumber(mape, 2)}%` },
  ];
  document.getElementById("accuracyGrid").innerHTML = items
    .map((i) => `<div class="accuracy-item"><div class="val">${i.value}</div><div class="lbl">${i.label}</div></div>`)
    .join("");
}

function renderAlerts(rows) {
  const icons = { critical: "⛔", warning: "⚠", info: "ℹ" };
  document.getElementById("alertsList").innerHTML = rows
    .map(
      (a) => `<div class="alert-item">
        <div class="alert-icon ${a.severity}">${icons[a.severity]}</div>
        <div>
          <div class="alert-title">${a.buildingType} · ${a.hvacSystem}</div>
          <div class="alert-msg">${a.message}</div>
        </div>
      </div>`
    )
    .join("");
}

async function populateFilters() {
  const { buildingTypes, hvacSystems } = await getJSON("/api/filters");
  const bSel = document.getElementById("filterBuildingType");
  const hSel = document.getElementById("filterHvac");
  bSel.innerHTML = `<option value="">All</option>` + buildingTypes.map((b) => `<option value="${b}">${b}</option>`).join("");
  hSel.innerHTML = `<option value="">All</option>` + hvacSystems.map((h) => `<option value="${h}">${h}</option>`).join("");
}

async function loadAll() {
  clearError();
  setStatus(false, "Loading from Snowflake…");
  try {
    const [kpis, byType, byHvac, health, scatter, epaByType, predictions, topTypes, alerts] = await Promise.all([
      getJSON("/api/kpis"),
      getJSON("/api/energy-by-buildingtype"),
      getJSON("/api/energy-by-hvac"),
      getJSON("/api/health-distribution"),
      getJSON("/api/energy-vs-area"),
      getJSON("/api/energy-per-area-by-type"),
      getJSON("/api/predictions"),
      getJSON("/api/top-buildingtypes"),
      getJSON("/api/alerts"),
    ]);

    renderKpis(kpis);
    barChart("chartByType", byType.map((r) => r.name), byType.map((r) => r.value), "#3b82f6");
    barChart("chartByHvac", byHvac.map((r) => r.name), byHvac.map((r) => r.value), "#14b8a6");
    doughnutChart(
      "chartHealth",
      health.map((r) => r.name),
      health.map((r) => r.value),
      health.map((r) => ({ Healthy: "#22c55e", Warning: "#f59e0b", Critical: "#ef4444" }[r.name] || "#8791ab"))
    );
    renderHealthLegend(health);
    scatterChart("chartScatter", scatter);
    barChart("chartEpaByType", epaByType.map((r) => r.name), epaByType.map((r) => r.value), "#8b5cf6");
    lineChart("chartActualPredicted", predictions.series);
    renderAccuracy(predictions.accuracy);
    gaugeChart("chartGauge", predictions.accuracy.mape);
    renderTopTable(topTypes);
    renderAlerts(alerts);

    setStatus(true, `Live · updated ${new Date().toLocaleTimeString()}`);
  } catch (err) {
    console.error(err);
    setStatus(false, "Connection failed");
    showError(
      `Could not load data from the API (${err.message}). Check that the backend is running and reachable at ${API}, and that it can connect to Snowflake.`
    );
  }
}

document.getElementById("reloadBtn").addEventListener("click", loadAll);
document.getElementById("filterBuildingType").addEventListener("change", (e) => {
  state.buildingType = e.target.value;
  loadAll();
});
document.getElementById("filterHvac").addEventListener("change", (e) => {
  state.hvacSystem = e.target.value;
  loadAll();
});
document.querySelectorAll(".nav-item").forEach((btn) =>
  btn.addEventListener("click", () => {
    document.querySelector(".nav-item.active")?.classList.remove("active");
    btn.classList.add("active");
    if (btn.dataset.page !== "overview") {
      showError("This section isn't built yet — only Executive Overview is wired up so far.");
    } else {
      clearError();
    }
  })
);

(async function init() {
  try {
    await populateFilters();
  } catch (err) {
    console.error("Could not load filters", err);
  }
  loadAll();
})();

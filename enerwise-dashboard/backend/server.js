require("dotenv").config();
const express = require("express");
const cors = require("cors");
const { runQuery } = require("./db");

const app = express();
app.use(cors({ origin: (process.env.CORS_ORIGIN || "*").split(",") }));
app.use(express.json());

const T_RAW = process.env.TABLE_RAW;
const T_PRED = process.env.TABLE_PREDICTIONS;
const T_EFF = process.env.TABLE_EFFICIENCY;
const T_BY_BUILDING = process.env.TABLE_BY_BUILDING;
const T_BY_HVAC = process.env.TABLE_BY_HVAC;
const T_STATS = process.env.TABLE_STATS;

// Builds a "WHERE BUILDINGTYPE = ? AND HVACSYSTEM = ?" clause from query params,
// always excluding the literal "Nan" rows that show up in the raw export.
function buildFilter(query, { buildingCol = "BUILDINGTYPE", hvacCol = "HVACSYSTEM" } = {}) {
  const clauses = [`${buildingCol} <> 'Nan'`, `${hvacCol} <> 'Nan'`];
  const binds = [];
  if (query.buildingType) {
    clauses.push(`${buildingCol} = ?`);
    binds.push(query.buildingType);
  }
  if (query.hvacSystem) {
    clauses.push(`${hvacCol} = ?`);
    binds.push(query.hvacSystem);
  }
  return { where: "WHERE " + clauses.join(" AND "), binds };
}

const asyncRoute = (fn) => (req, res) => fn(req, res).catch((err) => {
  console.error(err);
  res.status(500).json({ error: err.message });
});

// Distinct values for the sidebar filter dropdowns
app.get("/api/filters", asyncRoute(async (req, res) => {
  const buildingTypes = await runQuery(
    `SELECT DISTINCT BUILDINGTYPE FROM ${T_RAW} WHERE BUILDINGTYPE <> 'Nan' ORDER BY 1`
  );
  const hvacSystems = await runQuery(
    `SELECT DISTINCT HVACSYSTEM FROM ${T_RAW} WHERE HVACSYSTEM <> 'Nan' ORDER BY 1`
  );
  res.json({
    buildingTypes: buildingTypes.map((r) => r.BUILDINGTYPE),
    hvacSystems: hvacSystems.map((r) => r.HVACSYSTEM),
  });
}));

// Top KPI strip
app.get("/api/kpis", asyncRoute(async (req, res) => {
  const { where, binds } = buildFilter(req.query);
  const [totals] = await runQuery(
    `SELECT SUM(ENERGYCONSUMPTION) AS TOTAL_ENERGY,
            AVG(ENERGYPERAREA) AS AVG_ENERGY_PER_AREA,
            COUNT(DISTINCT BUILDINGTYPE) AS BUILDING_TYPES
     FROM ${T_RAW} ${where}`,
    binds
  );

  const { where: effWhere, binds: effBinds } = buildFilter(req.query);
  const [health] = await runQuery(
    `SELECT
        AVG(GREATEST(0, LEAST(100, 100 - GREATEST(DEVIATIONPERCENT, 0)))) AS AVG_HEALTH_SCORE,
        SUM(CASE WHEN EFFICIENCYSTATUS = 'Inefficient (Above Expected)' THEN 1 ELSE 0 END) AS AT_RISK_COUNT,
        COUNT(*) AS TOTAL_RECORDS
     FROM ${T_EFF} ${effWhere}`,
    effBinds
  );

  res.json({
    totalEnergy: Number(totals.TOTAL_ENERGY || 0),
    avgEnergyPerArea: Number(totals.AVG_ENERGY_PER_AREA || 0),
    buildingTypes: Number(totals.BUILDING_TYPES || 0),
    avgHealthScore: Number(health.AVG_HEALTH_SCORE || 0),
    atRiskCount: Number(health.AT_RISK_COUNT || 0),
    totalRecords: Number(health.TOTAL_RECORDS || 0),
  });
}));

// Energy Consumption by Building Type
// Uses the pre-aggregated ENERGY_BY_BUILDING table when no HVAC filter is
// active (it doesn't carry an HVACSYSTEM column); otherwise falls back to
// grouping the raw table so the HVAC filter still applies.
app.get("/api/energy-by-buildingtype", asyncRoute(async (req, res) => {
  if (!req.query.hvacSystem) {
    const clauses = ["BUILDINGTYPE <> 'Nan'"];
    const binds = [];
    if (req.query.buildingType) {
      clauses.push("BUILDINGTYPE = ?");
      binds.push(req.query.buildingType);
    }
    const rows = await runQuery(
      `SELECT BUILDINGTYPE, TOTALENERGY FROM ${T_BY_BUILDING} WHERE ${clauses.join(" AND ")} ORDER BY TOTALENERGY DESC`,
      binds
    );
    return res.json(rows.map((r) => ({ name: r.BUILDINGTYPE, value: Number(r.TOTALENERGY) })));
  }
  const { where, binds } = buildFilter(req.query);
  const rows = await runQuery(
    `SELECT BUILDINGTYPE, SUM(ENERGYCONSUMPTION) AS TOTAL_ENERGY
     FROM ${T_RAW} ${where}
     GROUP BY BUILDINGTYPE ORDER BY TOTAL_ENERGY DESC`,
    binds
  );
  res.json(rows.map((r) => ({ name: r.BUILDINGTYPE, value: Number(r.TOTAL_ENERGY) })));
}));

// Energy Consumption by HVAC System (same pre-aggregated-vs-raw logic, mirrored)
app.get("/api/energy-by-hvac", asyncRoute(async (req, res) => {
  if (!req.query.buildingType) {
    const clauses = ["HVACSYSTEM <> 'Nan'"];
    const binds = [];
    if (req.query.hvacSystem) {
      clauses.push("HVACSYSTEM = ?");
      binds.push(req.query.hvacSystem);
    }
    const rows = await runQuery(
      `SELECT HVACSYSTEM, TOTALENERGY FROM ${T_BY_HVAC} WHERE ${clauses.join(" AND ")} ORDER BY TOTALENERGY DESC`,
      binds
    );
    return res.json(rows.map((r) => ({ name: r.HVACSYSTEM, value: Number(r.TOTALENERGY) })));
  }
  const { where, binds } = buildFilter(req.query);
  const rows = await runQuery(
    `SELECT HVACSYSTEM, SUM(ENERGYCONSUMPTION) AS TOTAL_ENERGY
     FROM ${T_RAW} ${where}
     GROUP BY HVACSYSTEM ORDER BY TOTAL_ENERGY DESC`,
    binds
  );
  res.json(rows.map((r) => ({ name: r.HVACSYSTEM, value: Number(r.TOTAL_ENERGY) })));
}));

// Overall stats strip (used as a fast path when no filters are active)
app.get("/api/stats-summary", asyncRoute(async (req, res) => {
  const [row] = await runQuery(`SELECT * FROM ${T_STATS}`);
  res.json({
    averageEnergy: Number(row.AVERAGEENERGY),
    maximumEnergy: Number(row.MAXIMUMENERGY),
    minimumEnergy: Number(row.MINIMUMENERGY),
    totalEnergy: Number(row.TOTALENERGY),
    totalRecords: Number(row.TOTALRECORDS),
  });
}));

// Building Health Distribution (Healthy / Warning / Critical)
app.get("/api/health-distribution", asyncRoute(async (req, res) => {
  const { where, binds } = buildFilter(req.query);
  const rows = await runQuery(
    `SELECT
        CASE
          WHEN EFFICIENCYSTATUS = 'Inefficient (Above Expected)' AND DEVIATIONPERCENT > 50 THEN 'Critical'
          WHEN EFFICIENCYSTATUS = 'Inefficient (Above Expected)' THEN 'Warning'
          ELSE 'Healthy'
        END AS RISK_LEVEL,
        COUNT(*) AS CNT
     FROM ${T_EFF} ${where}
     GROUP BY RISK_LEVEL`,
    binds
  );
  res.json(rows.map((r) => ({ name: r.RISK_LEVEL, value: Number(r.CNT) })));
}));

// Energy vs Room Area scatter (sampled)
app.get("/api/energy-vs-area", asyncRoute(async (req, res) => {
  const { where, binds } = buildFilter(req.query);
  const rows = await runQuery(
    `SELECT ROOMAREA, ENERGYCONSUMPTION
     FROM ${T_RAW} ${where}
     ORDER BY ROOMAREA
     LIMIT 200`,
    binds
  );
  res.json(rows.map((r) => ({ area: Number(r.ROOMAREA), energy: Number(r.ENERGYCONSUMPTION) })));
}));

// Energy per Area by Building Type
app.get("/api/energy-per-area-by-type", asyncRoute(async (req, res) => {
  const { where, binds } = buildFilter(req.query);
  const rows = await runQuery(
    `SELECT BUILDINGTYPE, AVG(ENERGYPERAREA) AS AVG_EPA
     FROM ${T_RAW} ${where}
     GROUP BY BUILDINGTYPE ORDER BY AVG_EPA DESC`,
    binds
  );
  res.json(rows.map((r) => ({ name: r.BUILDINGTYPE, value: Number(r.AVG_EPA) })));
}));

// Actual vs Predicted energy (sampled) + accuracy metrics computed in JS
app.get("/api/predictions", asyncRoute(async (req, res) => {
  const { where, binds } = buildFilter(req.query);
  const rows = await runQuery(
    `SELECT ENERGYCONSUMPTION AS ACTUAL, PREDICTION AS PREDICTED
     FROM ${T_PRED} ${where}
     LIMIT 400`,
    binds
  );

  const pairs = rows.map((r) => ({ actual: Number(r.ACTUAL), predicted: Number(r.PREDICTED) }));
  const n = pairs.length || 1;
  const meanActual = pairs.reduce((s, p) => s + p.actual, 0) / n;
  const mae = pairs.reduce((s, p) => s + Math.abs(p.actual - p.predicted), 0) / n;
  const rmse = Math.sqrt(pairs.reduce((s, p) => s + (p.actual - p.predicted) ** 2, 0) / n);
  const mape = (pairs.reduce((s, p) => s + Math.abs((p.actual - p.predicted) / (p.actual || 1)), 0) / n) * 100;
  const ssRes = pairs.reduce((s, p) => s + (p.actual - p.predicted) ** 2, 0);
  const ssTot = pairs.reduce((s, p) => s + (p.actual - meanActual) ** 2, 0);
  const r2 = ssTot === 0 ? 0 : 1 - ssRes / ssTot;

  res.json({
    series: pairs.slice(0, 100).map((p, i) => ({ index: i + 1, actual: p.actual, predicted: p.predicted })),
    accuracy: { mae, rmse, mape, r2 },
  });
}));

// Top 5 building types by energy consumption, blended with efficiency/risk info
app.get("/api/top-buildingtypes", asyncRoute(async (req, res) => {
  const { where, binds } = buildFilter(req.query);
  const energyRows = await runQuery(
    `SELECT BUILDINGTYPE,
            SUM(ENERGYCONSUMPTION) AS TOTAL_ENERGY,
            AVG(ENERGYPERAREA) AS AVG_EPA
     FROM ${T_RAW} ${where}
     GROUP BY BUILDINGTYPE ORDER BY TOTAL_ENERGY DESC LIMIT 5`,
    binds
  );

  const { where: effWhere, binds: effBinds } = buildFilter(req.query);
  const healthRows = await runQuery(
    `SELECT BUILDINGTYPE,
            AVG(GREATEST(0, LEAST(100, 100 - GREATEST(DEVIATIONPERCENT, 0)))) AS HEALTH_SCORE,
            SUM(CASE WHEN EFFICIENCYSTATUS = 'Inefficient (Above Expected)' AND DEVIATIONPERCENT > 50 THEN 1 ELSE 0 END) AS CRITICAL_CNT,
            SUM(CASE WHEN EFFICIENCYSTATUS = 'Inefficient (Above Expected)' AND DEVIATIONPERCENT <= 50 THEN 1 ELSE 0 END) AS WARNING_CNT
     FROM ${T_EFF} ${effWhere}
     GROUP BY BUILDINGTYPE`,
    effBinds
  );
  const healthByType = Object.fromEntries(healthRows.map((r) => [r.BUILDINGTYPE, r]));

  res.json(
    energyRows.map((r) => {
      const h = healthByType[r.BUILDINGTYPE] || {};
      const healthScore = Math.round(Number(h.HEALTH_SCORE || 75));
      const riskLevel = Number(h.CRITICAL_CNT || 0) > 0 ? "High" : Number(h.WARNING_CNT || 0) > 0 ? "Medium" : "Low";
      return {
        buildingType: r.BUILDINGTYPE,
        totalEnergy: Math.round(Number(r.TOTAL_ENERGY)),
        energyPerArea: Number(Number(r.AVG_EPA).toFixed(1)),
        healthScore,
        riskLevel,
      };
    })
  );
}));

// Recent alerts, generated from the largest efficiency deviations
app.get("/api/alerts", asyncRoute(async (req, res) => {
  const { where, binds } = buildFilter(req.query);
  const rows = await runQuery(
    `SELECT BUILDINGTYPE, HVACSYSTEM, DEVIATIONPERCENT, EFFICIENCYSTATUS
     FROM ${T_EFF} ${where}
     ORDER BY ABS(DEVIATIONPERCENT) DESC
     LIMIT 5`,
    binds
  );
  res.json(
    rows.map((r) => ({
      buildingType: r.BUILDINGTYPE,
      hvacSystem: r.HVACSYSTEM,
      severity: Math.abs(r.DEVIATIONPERCENT) > 50 ? "critical" : Math.abs(r.DEVIATIONPERCENT) > 20 ? "warning" : "info",
      message:
        r.DEVIATIONPERCENT > 0
          ? `Energy consumption is ${Math.abs(r.DEVIATIONPERCENT).toFixed(1)}% higher than predicted`
          : `Energy consumption is ${Math.abs(r.DEVIATIONPERCENT).toFixed(1)}% lower than predicted`,
    }))
  );
}));

app.get("/api/health", (req, res) => res.json({ ok: true }));

const port = process.env.PORT || 4000;
app.listen(port, () => console.log(`EnerWise API listening on port ${port}`));

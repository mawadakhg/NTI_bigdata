# EnerWise Executive Overview — live from Snowflake

⚠️ **Important — about the password you shared:** please rotate your Snowflake
password now that it's been typed into a chat, and never hardcode Snowflake
credentials inside frontend/browser code. A browser can't speak Snowflake's
protocol directly anyway, and any credentials placed in frontend JS are
visible to anyone who opens dev tools. That's why this project is split in two:

- **backend/** — a small Node/Express API that holds the Snowflake
  credentials (in a local `.env` file, never committed) and runs the SQL.
- **frontend/** — a plain HTML/CSS/JS dashboard (no build step) that only
  ever talks to your backend over HTTP, and renders the charts.

## 1. Configure and run the backend

```bash
cd backend
cp .env.example .env
# edit .env: fill in SF_PASSWORD, and set TABLE_RAW / TABLE_PREDICTIONS /
# TABLE_EFFICIENCY to the real table or view names in ENERWISE_NEW.ANALYTICS
npm install
npm start
```

The API listens on `http://localhost:4000` by default.

### Table names (confirmed against ENERWISE_NEW.ANALYTICS)

| Env var             | Real object          | Columns |
|----------------------|----------------------|---------|
| `TABLE_RAW`          | `PROCESSED_ENERGY`   | BUILDINGTYPE, HVACSYSTEM, AVERAGETEMPERATURE, ENERGYCONSUMPTION, ENERGYPERAREA, INSULATIONTHICKNESS, NUMBEROFAPPLIANCES, OUTSIDETEMPERATURE, ROOMAREA, TEMPERATUREDIFFERENCE |
| `TABLE_PREDICTIONS`  | `ML_PREDICTIONS`     | BUILDINGTYPE, HVACSYSTEM, ENERGYCONSUMPTION, ENERGYPERAREA, INSULATIONTHICKNESS, NUMBEROFAPPLIANCES, PREDICTION, ROOMAREA |
| `TABLE_EFFICIENCY`   | `BUILDING_HEALTH`    | BUILDINGTYPE, EFFICIENCYSTATUS, HVACSYSTEM, ACTUALENERGY, DEVIATIONPERCENT, ENERGYPERAREA, EXPECTEDENERGY, INSULATIONTHICKNESS, NUMBEROFAPPLIANCES, ROOMAREA |
| `TABLE_BY_BUILDING`  | `ENERGY_BY_BUILDING` | AVERAGEENERGY, AVERAGEENERGYPERAREA, BUILDINGTYPE, RECORDCOUNT, TOTALENERGY |
| `TABLE_BY_HVAC`      | `ENERGY_BY_HVAC`     | AVERAGEENERGY, AVERAGEENERGYPERAREA, HVACSYSTEM, RECORDCOUNT, TOTALENERGY |
| `TABLE_STATS`        | `ENERGY_STATISTICS`  | AVERAGEENERGY, MAXIMUMENERGY, MINIMUMENERGY, TOTALENERGY, TOTALRECORDS (single row) |

These are already set as the defaults in `.env.example`. The "by building" and
"by HVAC" charts use the pre-aggregated tables when no cross-filter is
active (faster), and fall back to grouping `PROCESSED_ENERGY` live when you
filter one chart by the other dimension.

### CORS

If the frontend is served on a different port than the backend expects,
you'll see "Failed to fetch" in the browser even though the backend logs look
fine. Set `CORS_ORIGIN` in `.env` to match whatever URL your static server
actually prints (e.g. `npx serve .` usually prints `http://localhost:3000`),
then restart the backend. The default is already
`http://localhost:3000,http://localhost:5173`.

## 2. Run the frontend

Any static file server works, e.g.:

```bash
cd frontend
npx serve .
```

Then open the printed URL. If your backend isn't on `http://localhost:4000`,
edit `frontend/config.js`.

## What's different from the screenshot, and why

Your Snowflake data doesn't include individual building names (A, B, C…) or a
ready-made health score — it's organized by **Building Type**
(Residential, Commercial, Hospital, Educational, Office) and **HVAC System**.
So the dashboard groups everything by Building Type instead of named
buildings, and the health score / risk level are computed from
`EFFICIENCYSTATUS` and `DEVIATIONPERCENT` in your efficiency table (bigger
positive deviation = lower health score, more at risk). Everything else —
layout, KPI cards, chart types, the alerts panel, prediction accuracy
tile — mirrors the screenshot.

Only **Executive Overview** is fully wired up; the other sidebar items are
placeholders so you can see the intended navigation shape, same as in your
screenshot's layout.

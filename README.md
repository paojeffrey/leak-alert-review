# Leak Alert Review

A [Databricks App](https://docs.databricks.com/en/dev-tools/databricks-apps/index.html) where plant operators review leak alerts from the two-stage leak detection pipeline and confirm or reject each one. Every decision is recorded (who, when, optional comment) in an append-only Delta table.

**Pipeline that produces the alerts**
1. **Stage 1, POI change detector:** Model D, an unsupervised ensemble of POI-relative feature anomaly + DINOv2 PatchCore-lite. It flags images that differ from the same point of interest's normal history (20% false-alarm budget).
2. **Stage 2, vision-LLM verifier:** Gemini 3.8 Flash compares each stage 1 candidate to a known-dry reference image of the same POI. It returns a verdict (`leak` / `no_leak`), a confidence, a change type and its evidence.

The app shows the stage 2 output and collects the operator's final label. Those labels can later feed back into model evaluation.

Built with [FastAPI](https://fastapi.tiangolo.com/), a single-page vanilla JS front end, and the [Databricks SDK for Python](https://docs.databricks.com/en/dev-tools/sdk-python.html).

> [!IMPORTANT]
> **Update these names before deploying in Westlake's environment.**
> The schema, table names, volume path and warehouse in this repo come from the Databricks FE demo workspace (`stable_classic_wg38i9_catalog`). Westlake's workspace will use **different names**, and the app will return SQL errors until you change them.
>
> | What | Where | Current (demo) value | Change to |
> |------|-------|----------------------|-----------|
> | Catalog + schema | `app.yaml` → `LEAK_SCHEMA` (fallback default in `app.py` → `SCHEMA`) | `stable_classic_wg38i9_catalog.westlake_gas_leakage_2` | `<westlake_catalog>.<westlake_schema>` |
> | Image volume folder | `app.yaml` → `IMAGE_VOLUME_PATH` (fallback default in `app.py` → `VOLUME`) | `/Volumes/stable_classic_wg38i9_catalog/westlake_gas_leakage_2/documents/OneDrive_1_10-13-2026` | Westlake volume + folder |
> | Table names | `app.py` → `ALERTS`, `DECISIONS`, `PLANTS`, `POI_MAP` | `leak_alerts`, `leak_alert_decisions`, `plants`, `poi_plant_map` | Westlake table names, if different |
> | SQL warehouse | Databricks Apps UI → *Resources* → `sql-warehouse` (`app.yaml` reads it with `valueFrom`) | `4c06524b26564b9c` | Westlake warehouse |
> | Unity Catalog grants | Run as a Westlake admin (see [Permissions](#permissions)) | grants on demo tables | grants on Westlake tables |
>
> The tables must have the columns listed in [Tables](#tables), or the SQL in `app.py` must be changed to match.

## Features

- **Plant picker:** one card per plant with its number of leak alerts, alerts awaiting review, and confirmed leaks. Deep link to a plant with `#plant=<plant_id>` (e.g. `#plant=westlake-pilot`).
- **Alert cards:** the current image next to a known-dry reference image of the same POI, with the AI verdict and confidence, the LLM's evidence, the change type and the stage 1 change score. Click an image to see it full size.
- **Filters:** AI verdict (flagged as leak / all candidates) and review status (pending / reviewed / all).
- **Confirm / reject:** *Confirm leak* or *Not a leak*, with an optional comment of up to 500 characters. Decisions can be changed later. Every change is a new row, so the history is kept.
- **Decision log:** per-plant audit trail showing when (Central Time), who, which POI, the decision and the comment.
- **User identity:** the signed-in operator is taken from the `X-Forwarded-Email` header that Databricks Apps adds, so decisions are attributed automatically.

## Architecture

```
Browser (static/index.html)
   │  fetch /api/*
   ▼
FastAPI (app.py) ── Statement Execution API ──▶ SQL warehouse ──▶ leak_alerts / plants / poi_plant_map (read)
   │                                                           └─▶ leak_alert_decisions (append)
   └──────────── Files API (download + resize) ──▶ UC Volume with inspection images
```

| Endpoint | Purpose |
|----------|---------|
| `GET /api/me` | Signed-in user |
| `GET /api/plants` | Plants with POI / candidate / alert / pending / confirmed counts |
| `GET /api/alerts?plant_id=` | Alerts for a plant, with each alert's latest decision. Leak verdicts come first, then by stage 1 score |
| `POST /api/alerts/{alert_id}/decision` | Body `{"decision": "leak" \| "no_leak", "comment": "..."}`. Appends a decision row |
| `GET /api/decisions?plant_id=` | Decision log for a plant (latest 500) |
| `GET /api/images/{filename}?w_px=` | Image from the volume, resized to 900px or 1920px wide. Only filenames that appear in `leak_alerts` are served |

## Tables

All four tables live in the schema set by `LEAK_SCHEMA`.

**`leak_alerts`**: one row per stage 1 candidate, with the stage 2 verdict. Written by the pipeline; the app only reads it.

| Column | Type | Notes |
|--------|------|-------|
| `alert_id` | string | Primary key |
| `filename` | string | Current image in the volume |
| `poi` | int | Point of interest |
| `captured_at` | timestamp | UTC; shown in CT |
| `stage1_score` | decimal(10,10) | Model D anomaly score |
| `ai_verdict` | string | `leak` / `no_leak` |
| `change_type` | string | LLM's description of the change |
| `ai_confidence` | decimal(2,2) | 0–1 |
| `ai_evidence` | string | LLM's reasoning |
| `reference_filename` | string | Known-dry image of the same POI (nullable) |
| `model` | string | Verifier model |
| `created_at` | timestamp | |

**`leak_alert_decisions`**: append-only audit log. The latest row per `alert_id` is the current label.

| Column | Type |
|--------|------|
| `decision_id` | string |
| `alert_id` | string |
| `filename` | string |
| `poi` | int |
| `decision` | string (`leak` / `no_leak`) |
| `comment` | string |
| `decided_by` | string |
| `decided_at` | timestamp |
| `plant_id` | string |

**`plants`**: `plant_id` string, `plant_name` string, `description` string

**`poi_plant_map`**: `point_of_interest_id` int, `plant_id` string

Example DDL for the two app-specific tables:

```sql
CREATE TABLE IF NOT EXISTS <catalog>.<schema>.plants (
  plant_id STRING, plant_name STRING, description STRING)
COMMENT 'Plants shown in the Leak Alert Review app';

CREATE TABLE IF NOT EXISTS <catalog>.<schema>.poi_plant_map (
  point_of_interest_id INT, plant_id STRING)
COMMENT 'Maps each robot point of interest to its plant';

CREATE TABLE IF NOT EXISTS <catalog>.<schema>.leak_alert_decisions (
  decision_id STRING, alert_id STRING, filename STRING, poi INT, decision STRING,
  comment STRING, decided_by STRING, decided_at TIMESTAMP, plant_id STRING)
COMMENT 'Append-only audit log of operator decisions on leak alerts. Latest row per alert_id is the current label.';
```

## Permissions

The app's service principal needs:

| Securable | Privilege |
|-----------|-----------|
| SQL warehouse (app resource `sql-warehouse`) | `CAN_USE` |
| Catalog | `USE CATALOG` |
| Schema | `USE SCHEMA` |
| `leak_alerts`, `plants`, `poi_plant_map` | `SELECT` |
| `leak_alert_decisions` | `SELECT`, `MODIFY` |
| Image volume | `READ VOLUME` |

```sql
-- Replace <sp> with the app's service principal application ID (shown on the app's overview page)
GRANT USE CATALOG ON CATALOG <catalog> TO `<sp>`;
GRANT USE SCHEMA  ON SCHEMA  <catalog>.<schema> TO `<sp>`;
GRANT SELECT ON TABLE <catalog>.<schema>.leak_alerts   TO `<sp>`;
GRANT SELECT ON TABLE <catalog>.<schema>.plants        TO `<sp>`;
GRANT SELECT ON TABLE <catalog>.<schema>.poi_plant_map TO `<sp>`;
GRANT SELECT, MODIFY ON TABLE <catalog>.<schema>.leak_alert_decisions TO `<sp>`;
GRANT READ VOLUME ON VOLUME <catalog>.<schema>.<volume> TO `<sp>`;
```

## Project structure

```
.
├── app.py             # FastAPI backend (SQL, image proxy, decision logging)
├── app.yaml           # Databricks Apps run command + environment
├── requirements.txt   # Python dependencies
└── static/
    └── index.html     # Single-page UI
```

## Deploying to Databricks Apps

```bash
# 1. Update the names in app.yaml (see the note at the top)

# 2. Upload source to the workspace
databricks sync . /Workspace/Users/<you>/apps/leak-alert-review

# 3. Create the app (first time only), then add a SQL warehouse resource named "sql-warehouse" in the UI
databricks apps create leak-alert-review

# 4. Grant the service principal access (see Permissions), then deploy
databricks apps deploy leak-alert-review \
  --source-code-path /Workspace/Users/<you>/apps/leak-alert-review
```

## Running locally

```bash
pip install -r requirements.txt

export DATABRICKS_PROFILE=<your-cli-profile>      # defaults to DEFAULT
export DATABRICKS_WAREHOUSE_ID=<warehouse-id>
export LEAK_SCHEMA=<catalog>.<schema>
export IMAGE_VOLUME_PATH=/Volumes/<catalog>/<schema>/<volume>/<folder>
export LOCAL_USER=you@example.com                  # recorded as decided_by (defaults to "local-dev")

uvicorn app:app --reload --port 8000
```

Then open http://localhost:8000.

## Configuration reference

| Env var | Default | Purpose |
|---------|---------|---------|
| `DATABRICKS_WAREHOUSE_ID` | — (required) | SQL warehouse for all queries. Set from the `sql-warehouse` resource in `app.yaml` |
| `LEAK_SCHEMA` | `stable_classic_wg38i9_catalog.westlake_gas_leakage_2` | `catalog.schema` holding the four tables |
| `IMAGE_VOLUME_PATH` | `/Volumes/stable_classic_wg38i9_catalog/westlake_gas_leakage_2/documents/OneDrive_1_10-13-2026` | Folder with the inspection images |
| `DATABRICKS_PROFILE` | `DEFAULT` | CLI profile (local only) |
| `LOCAL_USER` | `local-dev` | User identity (local only) |

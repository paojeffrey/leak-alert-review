"""Leak Alert Review — Databricks App.

Shows leak alerts produced by the two-stage pipeline (POI change detector + vision-LLM verifier),
lets plant operators confirm or reject each alert, and logs who decided and when to a Delta table.
"""
import io
import os
import re
import time
import uuid
from datetime import datetime, timezone
from functools import lru_cache
from zoneinfo import ZoneInfo

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, Response
from PIL import Image
from pydantic import BaseModel

IS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))
SCHEMA = os.environ.get("LEAK_SCHEMA", "stable_classic_wg38i9_catalog.westlake_gas_leakage_2")
ALERTS = f"{SCHEMA}.leak_alerts"
DECISIONS = f"{SCHEMA}.leak_alert_decisions"
PLANTS = f"{SCHEMA}.plants"
POI_MAP = f"{SCHEMA}.poi_plant_map"
VOLUME = os.environ.get(
    "IMAGE_VOLUME_PATH",
    "/Volumes/stable_classic_wg38i9_catalog/westlake_gas_leakage_2/documents/OneDrive_1_10-13-2026",
)
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
CT = ZoneInfo("America/Chicago")
FILENAME_RE = re.compile(r"^[A-Za-z0-9_\-]+\.(jpg|jpeg|png)$", re.I)

w = WorkspaceClient() if IS_APP else WorkspaceClient(profile=os.environ.get("DATABRICKS_PROFILE", "DEFAULT"))
app = FastAPI(title="Leak Alert Review")
STATIC = os.path.join(os.path.dirname(__file__), "static")


# ---------------------------------------------------------------- helpers
def run_sql(statement: str, params: dict | None = None) -> list[dict]:
    """Run a parameterized statement on the SQL warehouse and return rows as dicts."""
    p = [StatementParameterListItem(name=k, value=None if v is None else str(v)) for k, v in (params or {}).items()]
    r = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID, statement=statement, parameters=p, wait_timeout="30s")
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(1)
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != StatementState.SUCCEEDED:
        msg = r.status.error.message if r.status.error else str(r.status.state)
        raise HTTPException(status_code=500, detail=f"SQL error: {msg}")
    cols = [c.name for c in (r.manifest.schema.columns or [])] if r.manifest and r.manifest.schema else []
    rows = r.result.data_array if r.result and r.result.data_array else []
    return [dict(zip(cols, row)) for row in rows]


def to_ct(ts: str | None) -> str | None:
    """'2026-09-08T09:01:43.000Z' -> 'Sep 8, 2026 4:01 AM CT'."""
    if not ts:
        return None
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(CT).strftime("%b %-d, %Y %-I:%M %p CT")


def current_user(request: Request) -> str:
    """Databricks Apps forwards the signed-in user's identity in these headers."""
    user = request.headers.get("x-forwarded-email") or request.headers.get("x-forwarded-preferred-username")
    if not user and not IS_APP:
        user = os.environ.get("LOCAL_USER", "local-dev")
    if not user:
        raise HTTPException(status_code=401, detail="No user identity on request")
    return user


@lru_cache(maxsize=1)
def allowed_images() -> frozenset:
    rows = run_sql(f"SELECT filename, reference_filename FROM {ALERTS}")
    return frozenset(f for r in rows for f in (r["filename"], r["reference_filename"]) if f)


@lru_cache(maxsize=256)
def load_image(filename: str, max_w: int) -> bytes:
    """Download from the UC Volume and downscale (originals are ~5 MB) for fast page loads."""
    raw = w.files.download(f"{VOLUME}/{filename}").contents.read()
    img = Image.open(io.BytesIO(raw)).convert("RGB")
    if img.width > max_w:
        img = img.resize((max_w, round(img.height * max_w / img.width)), Image.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    return buf.getvalue()


# ---------------------------------------------------------------- API
class Decision(BaseModel):
    decision: str
    comment: str | None = None


@app.get("/api/me")
def me(request: Request):
    return {"user": current_user(request)}


LATEST = f"""latest AS (
          SELECT * FROM {DECISIONS}
          QUALIFY row_number() OVER (PARTITION BY alert_id ORDER BY decided_at DESC) = 1)"""


@app.get("/api/plants")
def plants():
    """Plants with their alert counts, for the plant-selection screen."""
    rows = run_sql(f"""
        WITH {LATEST}
        SELECT p.plant_id, p.plant_name, p.description,
               count(DISTINCT m.point_of_interest_id) AS points_of_interest,
               count(a.alert_id) AS candidates,
               count_if(a.ai_verdict = 'leak') AS leak_alerts,
               count_if(a.ai_verdict = 'leak' AND l.decision IS NULL) AS awaiting_review,
               count_if(l.decision = 'leak') AS confirmed_leaks
        FROM {PLANTS} p
        LEFT JOIN {POI_MAP} m ON m.plant_id = p.plant_id
        LEFT JOIN {ALERTS} a ON a.poi = m.point_of_interest_id
        LEFT JOIN latest l ON l.alert_id = a.alert_id
        GROUP BY ALL ORDER BY p.plant_name""")
    for r in rows:
        for k in ("points_of_interest", "candidates", "leak_alerts", "awaiting_review", "confirmed_leaks"):
            r[k] = int(r[k] or 0)
    return rows


@app.get("/api/alerts")
def alerts(plant_id: str):
    rows = run_sql(f"""
        WITH {LATEST}
        SELECT a.alert_id, a.filename, a.poi, a.captured_at, a.stage1_score, a.ai_verdict, a.change_type,
               a.ai_confidence, a.ai_evidence, a.reference_filename,
               l.decision, l.decided_by, l.decided_at, l.comment
        FROM {ALERTS} a
        JOIN {POI_MAP} m ON m.point_of_interest_id = a.poi AND m.plant_id = :plant_id
        LEFT JOIN latest l ON l.alert_id = a.alert_id
        ORDER BY CASE WHEN a.ai_verdict = 'leak' THEN 0 ELSE 1 END, a.stage1_score DESC""", {"plant_id": plant_id})
    for r in rows:
        r["captured_at_ct"] = to_ct(r.pop("captured_at"))
        r["decided_at_ct"] = to_ct(r.pop("decided_at"))
        r["poi"] = int(float(r["poi"])) if r["poi"] is not None else None
        for k in ("stage1_score", "ai_confidence"):
            r[k] = float(r[k]) if r[k] is not None else None
    return rows


@app.post("/api/alerts/{alert_id}/decision")
def decide(alert_id: str, body: Decision, request: Request):
    if body.decision not in ("leak", "no_leak"):
        raise HTTPException(status_code=400, detail="decision must be 'leak' or 'no_leak'")
    user = current_user(request)
    alert = run_sql(f"""SELECT a.alert_id, a.filename, a.poi, m.plant_id FROM {ALERTS} a
                        LEFT JOIN {POI_MAP} m ON m.point_of_interest_id = a.poi
                        WHERE a.alert_id = :alert_id""", {"alert_id": alert_id})
    if not alert:
        raise HTTPException(status_code=404, detail="Unknown alert")
    decision_id = str(uuid.uuid4())
    comment = (body.comment or "").strip()[:500] or None
    run_sql(f"""
        INSERT INTO {DECISIONS} (decision_id, alert_id, filename, poi, decision, comment, decided_by, decided_at, plant_id)
        VALUES (:decision_id, :alert_id, :filename, CAST(:poi AS INT), :decision, :comment, :decided_by,
                current_timestamp(), :plant_id)""",
            {"decision_id": decision_id, "alert_id": alert_id, "filename": alert[0]["filename"],
             "poi": int(float(alert[0]["poi"])), "decision": body.decision, "comment": comment, "decided_by": user,
             "plant_id": alert[0]["plant_id"]})
    row = run_sql(f"SELECT decided_by, decided_at FROM {DECISIONS} WHERE decision_id = :id", {"id": decision_id})[0]
    return {"decision_id": decision_id, "alert_id": alert_id, "decision": body.decision, "comment": comment,
            "decided_by": row["decided_by"], "decided_at_ct": to_ct(row["decided_at"])}


@app.get("/api/decisions")
def decisions(plant_id: str):
    rows = run_sql(f"""SELECT decision_id, alert_id, filename, poi, decision, comment, decided_by, decided_at
                       FROM {DECISIONS} WHERE plant_id = :plant_id ORDER BY decided_at DESC LIMIT 500""",
                   {"plant_id": plant_id})
    for r in rows:
        r["decided_at_ct"] = to_ct(r.pop("decided_at"))
    return rows


@app.get("/api/images/{filename}")
def image(filename: str, w_px: int = 900):
    if not FILENAME_RE.match(filename) or filename not in allowed_images():
        raise HTTPException(status_code=404, detail="Image not found")
    return Response(content=load_image(filename, 1920 if w_px > 900 else 900), media_type="image/jpeg",
                    headers={"Cache-Control": "private, max-age=3600"})


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC, "index.html"))

"""
GEE Web App – FastAPI backend

Replaces the Streamlit server layer from main.py while keeping all pipeline
logic (Snakemake, DuckDB, worker scripts) identical.

DB schema is intentionally compatible with main.py so both can share the same
run_state.duckdb and resume runs started by either interface.
"""
from __future__ import annotations

import calendar
import io
import json
import logging
import os
import re
import secrets
import signal
import string
import subprocess
import sys
import tempfile
import time
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import duckdb
import geopandas as gpd
import pandas as pd
import yaml
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ─── Paths ────────────────────────────────────────────────────────────────────

_docker_data = Path("/app/data")
BASE_DATA_DIR = _docker_data if _docker_data.exists() else Path(__file__).parent.parent / "data"
RUNS_DIR      = BASE_DATA_DIR / "runs"
CONFIG_DIR    = Path("/tmp/gee_configs")
GEE_KEY_PATH  = Path(
    os.environ.get(
        "GOOGLE_APPLICATION_CREDENTIALS",
        str(Path(__file__).parent.parent / "config" / "gee-key.json"),
    )
)
APP_DIR       = Path(__file__).parent.parent        # repo root
SNAKEFILE     = APP_DIR / "Snakefile_parquet"
LOG_HANDLER   = APP_DIR / "scripts" / "snakemake_log_handler.py"
RUN_DB_PATH   = RUNS_DIR / "run_state.duckdb"

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

for d in [RUNS_DIR, CONFIG_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ─── Product registry (must match main.py exactly) ────────────────────────────

_REQUIRED_KEY_FIELDS = {"type", "project_id", "private_key", "client_email", "token_uri"}

PRODUCT_REGISTRY: dict[str, dict] = {
    "CHIRPS": {
        "ee_collection": "UCSB-CHG/CHIRPS/DAILY",
        "min_date": "1981-01-01",
        "max_date": "2026-02-28",
        "scale": 5566,
        "cadence": "daily",
        "categorical": False,
        "content": {
            "precipitation": {"stats": ["sum", "mean", "max"], "default_stats": ["sum"]},
        },
        "label": "CHIRPS Daily Precipitation",
        "description": "Global precipitation (0.05° resolution).",
        "resolution_m": 5566,
    },
    "ERA5_LAND": {
        "ee_collection": "ECMWF/ERA5_LAND/DAILY_AGGR",
        "min_date": "1950-01-01",
        "max_date": "2026-02-28",
        "scale": 9000,
        "cadence": "daily",
        "categorical": False,
        "content": {
            "temperature_2m":               {"stats": ["mean", "min", "max"], "default_stats": ["mean"]},
            "temperature_2m_min":           {"stats": ["mean", "min", "max"], "default_stats": ["mean"]},
            "temperature_2m_max":           {"stats": ["mean", "min", "max"], "default_stats": ["mean"]},
            "total_precipitation_sum":      {"stats": ["sum", "mean"], "default_stats": ["sum"]},
            "total_evaporation_sum":        {"stats": ["sum", "mean"], "default_stats": ["sum"]},
            "potential_evaporation_sum":    {"stats": ["sum", "mean"], "default_stats": ["sum"]},
            "snow_evaporation_sum":         {"stats": ["sum", "mean"], "default_stats": ["sum"]},
            "evaporation_from_bare_soil_sum": {"stats": ["sum", "mean"], "default_stats": ["sum"]},
            "evaporation_from_open_water_surfaces_excluding_oceans_sum": {"stats": ["sum", "mean"], "default_stats": ["sum"]},
            "evaporation_from_the_top_of_canopy_sum": {"stats": ["sum", "mean"], "default_stats": ["sum"]},
            "evaporation_from_vegetation_transpiration_sum": {"stats": ["sum", "mean"], "default_stats": ["sum"]},
        },
        "label": "ERA5-Land Climate",
        "description": "Reanalysis data for land variables (9km resolution).",
        "resolution_m": 9000,
    },
    "MODIS_LST": {
        "ee_collection": "MODIS/061/MOD11A2",
        "min_date": "2000-02-18",
        "max_date": "2026-02-10",
        "scale": 1000,
        "cadence": "composite",
        "categorical": False,
        "content": {
            "LST_Day_1km":   {"stats": ["mean", "median", "max"], "default_stats": ["mean"]},
            "LST_Night_1km": {"stats": ["mean", "median", "max"], "default_stats": ["mean"]},
        },
        "label": "MODIS Land Surface Temperature",
        "description": "Land Surface Temperature (1km resolution).",
        "resolution_m": 1000,
    },
    "MODIS_NDVI_EVI": {
        "ee_collection": "MODIS/061/MOD13Q1",
        "min_date": "2000-02-18",
        "max_date": "2026-02-02",
        "scale": 250,
        "cadence": "composite",
        "categorical": False,
        "content": {
            "NDVI": {"stats": ["mean", "median"], "default_stats": ["mean"]},
            "EVI":  {"stats": ["mean", "median"], "default_stats": ["mean"]},
        },
        "label": "MODIS NDVI / EVI",
        "description": "Vegetation Indices (250m resolution).",
        "resolution_m": 250,
    },
    "WorldCover_v100": {
        "ee_collection": "ESA/WorldCover/v100",
        "min_date": "2020-01-01",
        "max_date": "2020-12-31",
        "scale": 500,
        "cadence": "annual",
        "categorical": True,
        "content": {
            "Map": {"stats": ["histogram"], "default_stats": ["histogram"]},
        },
        "label": "ESA WorldCover 2020 (v1.0)",
        "description": "ESA WorldCover landcover classification v100 (10m, 2020).",
        "resolution_m": 10,
    },
    "WorldCover_v200": {
        "ee_collection": "ESA/WorldCover/v200",
        "min_date": "2021-01-01",
        "max_date": "2021-12-31",
        "scale": 500,
        "cadence": "annual",
        "categorical": True,
        "content": {
            "Map": {"stats": ["histogram"], "default_stats": ["histogram"]},
        },
        "label": "ESA WorldCover 2021 (v2.0)",
        "description": "ESA WorldCover landcover classification v200 (10m, 2021).",
        "resolution_m": 10,
    },
    "MODIS_LULC": {
        "ee_collection": "MODIS/061/MCD12Q1",
        "min_date": "2001-01-01",
        "max_date": "2023-12-31",
        "scale": 500,
        "cadence": "annual",
        "categorical": True,
        "content": {
            "LC_Type1": {"stats": ["histogram"], "default_stats": ["histogram"]},
            "LC_Type2": {"stats": ["histogram"], "default_stats": ["histogram"]},
            "LC_Type3": {"stats": ["histogram"], "default_stats": ["histogram"]},
            "LC_Type4": {"stats": ["histogram"], "default_stats": ["histogram"]},
            "LC_Type5": {"stats": ["histogram"], "default_stats": ["histogram"]},
        },
        "label": "MODIS Land Use / Land Cover",
        "description": "MODIS Land Cover Type (500m resolution, annual, multiple schemes).",
        "resolution_m": 500,
    },
}

# ─── App ──────────────────────────────────────────────────────────────────────

app = FastAPI(title="GEE Web App API", version="0.2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Pydantic models ──────────────────────────────────────────────────────────

class ProductConfig(BaseModel):
    product: str
    bands: list[str]
    stats: list[str]
    date_start: str   # YYYY-MM-DD
    date_end: str     # YYYY-MM-DD

class SubmitRunRequest(BaseModel):
    run_id: str
    products: list[ProductConfig]

# ─── Time chunk helpers (mirrors main.py) ─────────────────────────────────────

def _month_list(start_str: str, end_str: str) -> list[str]:
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end   = datetime.strptime(end_str,   "%Y-%m-%d")
    months, curr = [], start.replace(day=1)
    while curr <= end:
        months.append(curr.strftime("%Y-%m"))
        curr = curr.replace(month=curr.month + 1) if curr.month < 12 else curr.replace(year=curr.year + 1, month=1)
    return months

def _quarterly_chunks(start_str: str, end_str: str) -> list[str]:
    months = _month_list(start_str, end_str)
    chunks = []
    for i in range(0, len(months), 3):
        g = months[i:i + 3]
        chunks.append(f"{g[0]}_{g[-1]}")
    return chunks

def _year_list(start_str: str, end_str: str) -> list[str]:
    s, e = datetime.strptime(start_str, "%Y-%m-%d"), datetime.strptime(end_str, "%Y-%m-%d")
    return [str(y) for y in range(s.year, e.year + 1)]

def get_time_chunks(start_str: str, end_str: str, cadence: str) -> list[str]:
    if cadence == "annual":
        return _year_list(start_str, end_str)
    if cadence == "daily":
        return _month_list(start_str, end_str)
    # composite / monthly → quarterly batches
    return _quarterly_chunks(start_str, end_str)

# ─── DuckDB helpers (schema mirrors main.py) ──────────────────────────────────

def _duckdb_connect(retries: int = 8, delay: float = 0.25) -> duckdb.DuckDBPyConnection:
    last_exc = None
    for attempt in range(retries):
        try:
            return duckdb.connect(str(RUN_DB_PATH))
        except duckdb.IOException as e:
            if "lock" not in str(e).lower():
                raise
            last_exc = e
            time.sleep(delay * (attempt + 1))
    raise last_exc  # type: ignore[misc]

def ensure_run_db():
    with _duckdb_connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS run_status (
                run_id                VARCHAR PRIMARY KEY,
                status                VARCHAR,
                attempts              INTEGER,
                config_hash           VARCHAR,
                created_at            TIMESTAMP,
                updated_at            TIMESTAMP,
                last_error            VARCHAR,
                snakemake_pid         BIGINT,
                snakemake_log_path    VARCHAR,
                snakemake_config_path VARCHAR,
                snakefile             VARCHAR
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS run_events (
                event_time   TIMESTAMP,
                run_id       VARCHAR,
                event_type   VARCHAR,
                status       VARCHAR,
                message      VARCHAR,
                payload_json VARCHAR
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                run_id      VARCHAR,
                product     VARCHAR,
                band        VARCHAR,
                time_chunk  VARCHAR,
                status      VARCHAR DEFAULT 'pending',
                jobid       INTEGER,
                log_path    VARCHAR,
                started_at  TIMESTAMP,
                finished_at TIMESTAMP,
                error       VARCHAR,
                PRIMARY KEY (run_id, product, band, time_chunk)
            )
        """)

ensure_run_db()

def _upsert_run_status(run_id: str, record: dict):
    with _duckdb_connect() as conn:
        conn.execute("""
            INSERT INTO run_status (
                run_id, status, attempts, config_hash,
                created_at, updated_at, last_error,
                snakemake_pid, snakemake_log_path, snakemake_config_path, snakefile
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(run_id) DO UPDATE SET
                status                = excluded.status,
                attempts              = excluded.attempts,
                config_hash           = excluded.config_hash,
                updated_at            = excluded.updated_at,
                last_error            = excluded.last_error,
                snakemake_pid         = excluded.snakemake_pid,
                snakemake_log_path    = excluded.snakemake_log_path,
                snakemake_config_path = excluded.snakemake_config_path,
                snakefile             = excluded.snakefile
        """, [
            run_id,
            record.get("status"),
            int(record.get("attempts", 0)),
            record.get("config_hash"),
            record.get("created_at"),
            record.get("updated_at"),
            record.get("last_error"),
            record.get("snakemake_pid"),
            record.get("snakemake_log_path"),
            record.get("snakemake_config_path"),
            record.get("snakefile", str(SNAKEFILE)),
        ])

def _append_event(run_id: str, event_type: str, status: str, message: str = "", payload=None):
    with _duckdb_connect() as conn:
        conn.execute("""
            INSERT INTO run_events (event_time, run_id, event_type, status, message, payload_json)
            VALUES (CURRENT_TIMESTAMP, ?, ?, ?, ?, ?)
        """, [run_id, event_type, status, message, json.dumps(payload or {}, default=str)])

def _get_job_counts(run_id: str, meta: dict | None = None) -> dict:
    # Derive total/done from the filesystem — no DuckDB locking needed and always accurate.
    # A parquet chunk file existing means that band×chunk pair fully completed.
    if meta is None:
        meta = _load_yaml(run_id)
    payload  = (meta or {}).get("payload") or {}
    products = payload.get("products") or {}
    chunks_dir = RUNS_DIR / run_id / "intermediate" / "chunks"

    total = done = 0
    for prod, cfg in products.items():
        for band in cfg.get("bands", []):
            for chunk in cfg.get("time_chunks", []):
                total += 1
                if (chunks_dir / prod / f"{band}_{chunk}.parquet").exists():
                    done += 1

    # Best-effort: get running/failed counts from DuckDB for richer status display.
    try:
        with _duckdb_connect() as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) FROM jobs WHERE run_id=? AND status IN ('running','failed') GROUP BY status",
                [run_id],
            ).fetchall()
        db_counts = {r[0]: int(r[1]) for r in rows}
    except Exception:
        db_counts = {}

    running = db_counts.get("running", 0)
    failed  = db_counts.get("failed",  0)
    pending = max(0, total - done - running - failed)
    return {"total": total, "done": done, "failed": failed, "running": running, "pending": pending}

def _initialise_jobs(run_id: str, payload: dict):
    """Pre-populate the jobs table (mirrors main.py initialise_jobs)."""
    products = payload.get("products", {}) or {}
    rows = []
    for prod, cfg in products.items():
        for band in cfg.get("bands", []):
            for chunk in cfg.get("time_chunks", []):
                chunk_path = (
                    RUNS_DIR / run_id / "intermediate" / "chunks" / prod / f"{band}_{chunk}.parquet"
                )
                status = "done" if chunk_path.exists() else "pending"
                rows.append((run_id, prod, band, chunk, status))
    if not rows:
        return
    with _duckdb_connect() as conn:
        conn.executemany("""
            INSERT INTO jobs (run_id, product, band, time_chunk, status) VALUES (?,?,?,?,?)
            ON CONFLICT (run_id, product, band, time_chunk) DO UPDATE SET
                status = CASE
                    WHEN jobs.status = 'done'   THEN 'done'
                    WHEN jobs.status = 'failed' THEN jobs.status
                    ELSE excluded.status
                END
        """, rows)

# ─── YAML helpers (mirrors main.py update_run_registry / load_run_registry) ───

def _run_yaml_path(run_id: str) -> Path:
    return RUNS_DIR / run_id / "run.yaml"

def _load_yaml(run_id: str) -> dict | None:
    p = _run_yaml_path(run_id)
    if not p.exists():
        return None
    return yaml.safe_load(p.read_text()) or None

def _save_yaml(run_id: str, record: dict):
    p = _run_yaml_path(run_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(yaml.safe_dump(record, sort_keys=False))

def _update_registry(run_id: str, payload: dict, status: str,
                     config_hash: str | None = None,
                     error_message: str | None = None,
                     bump_attempt: bool = False):
    now      = datetime.utcnow().isoformat()
    existing = _load_yaml(run_id) or {}
    attempts = existing.get("attempts", 0)
    if bump_attempt:
        attempts += 1

    record = {
        "run_id":               run_id,
        "status":               status,
        "created_at":           existing.get("created_at", now),
        "updated_at":           now,
        "attempts":             attempts,
        "config_hash":          config_hash,
        "payload":              payload,
        "last_error":           error_message,
        # Clear the old PID when transitioning to running so that _resolve_status
        # doesn't see the dead PID from a previous attempt and immediately flip
        # the status back to failed before _set_execution_meta writes the new PID.
        "snakemake_pid":        None if status == "running" else existing.get("snakemake_pid"),
        "snakemake_log_path":   existing.get("snakemake_log_path"),
        "snakemake_config_path":existing.get("snakemake_config_path"),
        "snakefile":            existing.get("snakefile", str(SNAKEFILE)),
    }
    if status == "running":
        record["last_started_at"] = now
    if status in {"completed", "failed"}:
        record["last_finished_at"] = now

    _save_yaml(run_id, record)
    _upsert_run_status(run_id, record)

    messages = {
        "queued":    "Run queued",
        "running":   f"Run started (attempt {attempts})",
        "completed": "Run completed successfully",
        "failed":    f"Run failed{': ' + error_message if error_message else ''}",
        "stopped":   "Run stopped by user",
    }
    _append_event(run_id, "status_change", status, messages.get(status, status))

def _set_execution_meta(run_id: str, pid: int, log_path: str, config_path: str):
    existing = _load_yaml(run_id) or {}
    existing["snakemake_pid"]          = pid
    existing["snakemake_log_path"]     = log_path
    existing["snakemake_config_path"]  = config_path
    existing["updated_at"]             = datetime.utcnow().isoformat()
    _save_yaml(run_id, existing)
    _upsert_run_status(run_id, existing)
    _append_event(run_id, "pipeline_started", existing.get("status", "running"),
                  f"Snakemake launched (PID {pid})", {"log_path": log_path, "config_path": config_path})

def _list_saved_runs() -> list[dict]:
    runs = []
    for p in sorted(RUNS_DIR.glob("*/run.yaml"), key=lambda x: x.stat().st_mtime, reverse=True):
        m = _load_yaml(p.parent.name)
        if m:
            runs.append(m)
    return runs

# ─── Process helpers ──────────────────────────────────────────────────────────

def _is_pid_alive(pid: int | None) -> bool:
    if pid is None:
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except Exception:
        return False

def _get_descendants(root_pid: int) -> list[int]:
    parent_map: dict[int, list[int]] = {}
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        stat_p = f"/proc/{entry}/stat"
        try:
            content = Path(stat_p).read_text()
            after   = content.rsplit(")", 1)[1].strip().split()
            if len(after) >= 3:
                parent_map.setdefault(int(after[1]), []).append(int(entry))
        except Exception:
            continue
    descendants, stack = [], [root_pid]
    while stack:
        cur = stack.pop()
        ch  = parent_map.get(cur, [])
        descendants.extend(ch)
        stack.extend(ch)
    return descendants

def _resolve_status(meta: dict | None) -> str:
    if meta is None:
        return "unknown"
    status = meta.get("status", "unknown")
    if status == "running":
        pid = meta.get("snakemake_pid")
        if pid is None:
            # PID not yet written — submission/retry is still in flight, keep "running"
            return "running"
        if not _is_pid_alive(pid):
            # Process is gone — infer final status from jobs table.
            # pid was set so there's no retry race condition; safe to persist.
            run_id = meta.get("run_id")
            counts = _get_job_counts(run_id, meta) if run_id else {}
            total  = counts.get("total", 0)
            done   = counts.get("done", 0)
            resolved = "completed" if (total > 0 and done == total) else "failed"
            if run_id:
                error_msg = None if resolved == "completed" else "Pipeline process exited unexpectedly"
                _update_registry(run_id, meta.get("payload") or {}, status=resolved,
                                 error_message=error_msg)
            return resolved
    return status

# ─── GEE key validation ───────────────────────────────────────────────────────

def _validate_gee_key(data: dict) -> str:
    missing = _REQUIRED_KEY_FIELDS - data.keys()
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(sorted(missing))}")
    if data.get("type") != "service_account":
        raise ValueError(f"Expected type 'service_account', got '{data.get('type')}'")
    email = data.get("client_email", "")
    if not email or "@" not in email:
        raise ValueError("client_email is empty or malformed")
    return email

# ─── Run result file helpers ──────────────────────────────────────────────────

def _results_dir(run_id: str) -> Path:
    return RUNS_DIR / run_id / "results"

def _list_result_products(run_id: str) -> list[str]:
    rdir = _results_dir(run_id)
    if not rdir.exists():
        return []
    return [p.name for p in rdir.iterdir() if p.is_dir() and p.name != "partial_checkout"]

def _find_product_parquet(run_id: str, product: str) -> Path:
    rdir  = _results_dir(run_id) / product
    files = sorted(rdir.glob("*.parquet")) if rdir.exists() else []
    if not files:
        raise HTTPException(404, f"No parquet for product '{product}' in run '{run_id}'")
    return files[0]

def _run_to_summary(meta: dict) -> dict:
    payload  = meta.get("payload", {}) or {}
    products = payload.get("products", {}) or {}
    return {
        "run_id":     meta.get("run_id"),
        "status":     _resolve_status(meta),
        "created_at": meta.get("created_at", ""),
        "updated_at": meta.get("updated_at", ""),
        "products":   list(products.keys()),
        "aoi_name":   payload.get("aoi_name", ""),
    }

def _run_to_detail(run_id: str, meta: dict) -> dict:
    payload = meta.get("payload", {}) or {}
    try:
        with _duckdb_connect() as conn:
            status_events = conn.execute(
                """SELECT event_time, event_type, message
                   FROM run_events WHERE run_id=? ORDER BY event_time""",
                [run_id],
            ).fetchall()
            job_events = conn.execute(
                """SELECT started_at, finished_at, product, band, time_chunk, status, error
                   FROM jobs
                   WHERE run_id=?
                     AND (started_at IS NOT NULL OR finished_at IS NOT NULL)
                   ORDER BY COALESCE(finished_at, started_at)""",
                [run_id],
            ).fetchall()
    except Exception:
        status_events, job_events = [], []

    # Merge status events and job events into a single timeline
    merged = []
    for ts, evtype, msg in status_events:
        merged.append({"ts": str(ts), "level": evtype, "msg": msg or evtype})

    # Track which jobs already have a DB-sourced finished event to avoid duplicates
    db_finished: set[tuple] = set()
    for started, finished, prod, band, chunk, jstatus, error in job_events:
        if started:
            merged.append({
                "ts":    str(started),
                "level": "job_start",
                "msg":   f"Started {prod}/{band} [{chunk}]",
            })
        if finished:
            level = "job_done" if jstatus == "done" else "job_error"
            msg   = f"{'Finished' if jstatus == 'done' else 'Failed'} {prod}/{band} [{chunk}]"
            if error:
                msg += f": {error}"
            merged.append({"ts": str(finished), "level": level, "msg": msg})
            db_finished.add((prod, band, chunk))

    # Fill in finished events from the filesystem for jobs the log handler missed.
    # The parquet chunk file's mtime is the authoritative completion timestamp.
    chunks_dir   = RUNS_DIR / run_id / "intermediate" / "chunks"
    results_dir  = RUNS_DIR / run_id / "results"
    products_cfg = payload.get("products", {}) or {}
    for prod, cfg in products_cfg.items():
        for band in cfg.get("bands", []):
            for chunk in cfg.get("time_chunks", []):
                if (prod, band, chunk) in db_finished:
                    continue
                parquet = chunks_dir / prod / f"{band}_{chunk}.parquet"
                if parquet.exists():
                    mtime = parquet.stat().st_mtime
                    ts    = datetime.utcfromtimestamp(mtime).isoformat()
                    merged.append({
                        "ts":    ts,
                        "level": "job_done",
                        "msg":   f"Finished {prod}/{band} [{chunk}]",
                    })

    # Add merge finished events from filesystem.
    # Track merge products that already have a finished event in run_events to avoid duplicates.
    merge_finished_prods = {
        e["msg"].split()[1]  # "Finished {prod} merge" → prod
        for e in merged
        if e.get("level") == "job_done" and e.get("msg", "").endswith(" merge")
    }
    for prod in products_cfg:
        if prod in merge_finished_prods:
            continue
        prod_result_dir = results_dir / prod
        if prod_result_dir.exists():
            parquet_files = [p for p in prod_result_dir.glob("*.parquet")]
            if parquet_files:
                newest = max(parquet_files, key=lambda p: p.stat().st_mtime)
                mtime  = newest.stat().st_mtime
                ts     = datetime.utcfromtimestamp(mtime).isoformat()
                merged.append({
                    "ts":    ts,
                    "level": "job_done",
                    "msg":   f"Finished {prod} merge",
                })

    merged.sort(key=lambda e: e["ts"])

    return {
        **_run_to_summary(meta),
        "pid":        meta.get("snakemake_pid"),
        "run_dir":    str(RUNS_DIR / run_id),
        "config":     payload,
        "job_counts": _get_job_counts(run_id, meta),
        "events":     merged,
    }

# ─── Routes: GEE key ─────────────────────────────────────────────────────────

@app.get("/api/gee-key")
def gee_key_status():
    if not GEE_KEY_PATH.exists():
        return {"valid": False, "email": None, "error": "No key uploaded"}
    try:
        data  = json.loads(GEE_KEY_PATH.read_text())
        email = _validate_gee_key(data)
        return {"valid": True, "email": email, "error": None}
    except Exception as e:
        return {"valid": False, "email": None, "error": str(e)}

@app.post("/api/gee-key")
async def upload_gee_key(file: UploadFile = File(...)):
    content = await file.read()
    try:
        data  = json.loads(content)
        email = _validate_gee_key(data)
    except ValueError as e:
        return {"valid": False, "email": None, "error": str(e)}
    except Exception:
        return {"valid": False, "email": None, "error": "Not valid JSON"}
    GEE_KEY_PATH.parent.mkdir(parents=True, exist_ok=True)
    GEE_KEY_PATH.write_bytes(content)
    return {"valid": True, "email": email, "error": None}

# ─── Routes: Products ─────────────────────────────────────────────────────────

@app.get("/api/products")
def get_products():
    result = []
    for pid, info in PRODUCT_REGISTRY.items():
        bands = [
            {"name": k, "description": k, "default_stats": v["default_stats"], "available_stats": v["stats"]}
            for k, v in info["content"].items()
        ]
        result.append({
            "id":           pid,
            "label":        info["label"],
            "description":  info["description"],
            "date_min":     info["min_date"],
            "date_max":     info["max_date"],
            "resolution_m": info["resolution_m"],
            "cadence":      info["cadence"],
            "categorical":  info["categorical"],
            "bands":        bands,
            "supported_stats": sorted({s for b in info["content"].values() for s in b["stats"]}),
        })
    return result

# ─── Routes: Runs ─────────────────────────────────────────────────────────────

@app.get("/api/events")
def list_events(limit: int = 50):
    try:
        with _duckdb_connect() as conn:
            rows = conn.execute(
                """SELECT event_time, run_id, event_type, message
                   FROM run_events
                   ORDER BY event_time DESC
                   LIMIT ?""",
                [limit],
            ).fetchall()
    except Exception:
        rows = []
    return [
        {"ts": str(ts), "run_id": run_id, "level": evtype, "msg": msg}
        for ts, run_id, evtype, msg in rows
    ]

@app.get("/api/runs")
def list_runs():
    return [_run_to_summary(m) for m in _list_saved_runs()]

@app.get("/api/runs/{run_id}")
def get_run(run_id: str):
    meta = _load_yaml(run_id)
    if meta is None:
        raise HTTPException(404, "Run not found")
    return _run_to_detail(run_id, meta)

@app.post("/api/runs")
def submit_run(body: SubmitRunRequest):
    # Reject if any run is already active
    active = next(
        (m for m in _list_saved_runs() if _resolve_status(m) == "running"),
        None,
    )
    if active:
        raise HTTPException(409, f"Run '{active['run_id']}' is already running. Wait for it to finish or stop it first.")

    run_id  = re.sub(r"[^A-Za-z0-9]", "", (body.run_id or "").strip())[:40]
    if not run_id:
        run_id = "".join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(6))

    run_dir = RUNS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    # Find the AOI file saved under inputs/
    input_dir = run_dir / "inputs"
    aoi_files = sorted(input_dir.iterdir()) if input_dir.exists() else []
    # Prefer .shp, then .geojson/.parquet
    shp_file  = next((f for f in aoi_files if f.suffix == ".shp"), None)
    aoi_file  = shp_file or next(iter(aoi_files), None)
    if aoi_file is None:
        raise HTTPException(400, "No AOI uploaded for this run. Upload an AOI first.")

    # Build Snakemake payload (dict-of-dicts, matching main.py format)
    product_tasks: dict = {}
    for pc in body.products:
        info       = PRODUCT_REGISTRY.get(pc.product)
        if info is None:
            raise HTTPException(400, f"Unknown product: {pc.product}")
        cadence    = info["cadence"]
        time_chunks = get_time_chunks(pc.date_start, pc.date_end, cadence)
        # Clamp end date to the last day of the end month for monthly cadence
        dt_end     = datetime.strptime(pc.date_end, "%Y-%m-%d")
        dt_end     = dt_end.replace(day=calendar.monthrange(dt_end.year, dt_end.month)[1])

        product_tasks[pc.product] = {
            "ee_collection": info["ee_collection"],
            "bands":         pc.bands,
            "statistics":    pc.stats,       # Snakefile uses "statistics"
            "scale":         info["scale"],
            "cadence":       cadence,
            "categorical":   info["categorical"],
            "start_date":    pc.date_start,
            "end_date":      dt_end.strftime("%Y-%m-%d"),
            "time_chunks":   time_chunks,
        }

    payload = {
        "run_id":               run_id,
        "shp_path":             str(aoi_file),
        "products":             product_tasks,
        "output_dir":           str(_results_dir(run_id)),
        "chain_parallel_window": 3,
        "aoi_name":             aoi_file.name,
        "app_dir":              str(APP_DIR),
    }

    # Register run (status=queued → running)
    _update_registry(run_id, payload, status="queued")
    _update_registry(run_id, payload, status="running", bump_attempt=True)

    # Pre-populate jobs table so progress bar works immediately
    _initialise_jobs(run_id, payload)

    # Write Snakemake config YAML
    cfg_path = CONFIG_DIR / f"config_{uuid.uuid4().hex[:8]}.yaml"
    cfg_path.write_text(yaml.safe_dump(payload, sort_keys=False))

    # Unlock any stale Snakemake lock (safe only when no other pipeline is running)
    try:
        subprocess.run(
            ["snakemake", "--unlock", "--snakefile", str(SNAKEFILE),
             "--directory", str(run_dir)],
            capture_output=True, text=True, timeout=10, cwd=str(run_dir),
        )
    except Exception:
        pass

    # Snakemake log path
    log_dir  = run_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "snakemake_run.log"
    with open(log_path, "a") as lh:
        lh.write(f"\n[{datetime.utcnow().isoformat()}] API submit\n")

    # Launch Snakemake
    env = {
        **os.environ,
        "GOOGLE_APPLICATION_CREDENTIALS": str(GEE_KEY_PATH),
        "GEE_RUN_ID":  run_id,
        "GEE_DB_PATH": str(RUN_DB_PATH),
        "HOME": "/tmp",
    }
    cmd = [
        "snakemake",
        "--snakefile",           str(SNAKEFILE),
        "--configfile",          str(cfg_path),
        "--directory",           str(run_dir),
        "-j",                    "12",
        "--resources",           "gee=3",
        "--rerun-incomplete",
        "--keep-going",
        "--log-handler-script",  str(LOG_HANDLER),
    ]
    log_handle = open(log_path, "a")
    proc = subprocess.Popen(
        cmd, stdout=log_handle, stderr=subprocess.STDOUT,
        text=True, close_fds=True, env=env, cwd=str(run_dir),
    )
    log_handle.close()

    _set_execution_meta(run_id, proc.pid, str(log_path), str(cfg_path))

    # Return current run detail
    return _run_to_detail(run_id, _load_yaml(run_id) or {})

@app.delete("/api/runs/{run_id}")
def stop_run(run_id: str):
    meta = _load_yaml(run_id)
    if meta is None:
        raise HTTPException(404, "Run not found")

    pid = meta.get("snakemake_pid")
    if pid and _is_pid_alive(pid):
        for child in _get_descendants(int(pid)):
            try:
                os.kill(child, signal.SIGTERM)
            except Exception:
                pass
        try:
            os.kill(int(pid), signal.SIGTERM)
        except Exception:
            pass
        # Brief wait, then SIGKILL
        deadline = time.monotonic() + 1.5
        while time.monotonic() < deadline and _is_pid_alive(pid):
            time.sleep(0.1)
        if _is_pid_alive(pid):
            try:
                os.kill(int(pid), signal.SIGKILL)
            except Exception:
                pass

    payload = meta.get("payload", {})
    _update_registry(run_id, payload, status="stopped",
                     error_message="Stopped by user from React UI.")
    return {"ok": True}

@app.post("/api/runs/{run_id}/retry")
def retry_run(run_id: str):
    meta = _load_yaml(run_id)
    if meta is None:
        raise HTTPException(404, "Run not found")

    if _resolve_status(meta) == "running":
        raise HTTPException(409, "Run is already running.")

    active = next(
        (m for m in _list_saved_runs()
         if m.get("run_id") != run_id and _resolve_status(m) == "running"),
        None,
    )
    if active:
        raise HTTPException(409, f"Run '{active['run_id']}' is already running.")

    payload = meta.get("payload") or {}
    cfg_path = CONFIG_DIR / f"config_{uuid.uuid4().hex[:8]}.yaml"
    cfg_path.write_text(yaml.safe_dump(payload, sort_keys=False))

    run_dir  = RUNS_DIR / run_id
    log_dir  = run_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "snakemake_run.log"
    with open(log_path, "a") as lh:
        lh.write(f"\n[{datetime.utcnow().isoformat()}] API retry\n")

    try:
        subprocess.run(
            ["snakemake", "--unlock", "--snakefile", str(SNAKEFILE),
             "--directory", str(run_dir)],
            capture_output=True, text=True, timeout=10, cwd=str(run_dir),
        )
    except Exception:
        pass

    env = {
        **os.environ,
        "GOOGLE_APPLICATION_CREDENTIALS": str(GEE_KEY_PATH),
        "GEE_RUN_ID":  run_id,
        "GEE_DB_PATH": str(RUN_DB_PATH),
        "HOME": "/tmp",
    }
    cmd = [
        "snakemake",
        "--snakefile",           str(SNAKEFILE),
        "--configfile",          str(cfg_path),
        "--directory",           str(run_dir),
        "-j",                    "12",
        "--resources",           "gee=3",
        "--rerun-incomplete",
        "--keep-going",
        "--log-handler-script",  str(LOG_HANDLER),
    ]
    log_handle = open(log_path, "a")
    proc = subprocess.Popen(
        cmd, stdout=log_handle, stderr=subprocess.STDOUT,
        text=True, close_fds=True, env=env, cwd=str(run_dir),
    )
    log_handle.close()

    _update_registry(run_id, payload, status="running", bump_attempt=True)
    _set_execution_meta(run_id, proc.pid, str(log_path), str(cfg_path))

    return _run_to_detail(run_id, _load_yaml(run_id) or {})

@app.get("/api/runs/{run_id}/log")
def get_run_log(run_id: str, lines: int = 100):
    log_path = RUNS_DIR / run_id / "logs" / "snakemake_run.log"
    if not log_path.exists():
        return {"lines": []}
    with open(log_path, "r", errors="replace") as f:
        tail = f.readlines()[-lines:]
    return {"lines": [l.rstrip("\n") for l in tail]}

@app.post("/api/runs/{run_id}/partial")
def trigger_partial(run_id: str):
    script = APP_DIR / "scripts" / "build_partial.py"
    subprocess.Popen(
        [sys.executable, str(script), run_id, str(RUNS_DIR)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return {"ok": True}

def _sanitize_filename(filename: str) -> str:
    name = Path(filename).name
    if not name or name in {".", ".."}:
        raise HTTPException(400, "Invalid filename")
    return name


def _safe_extract_zip(zf: zipfile.ZipFile, destination: Path):
    dest_root = destination.resolve()
    for member in zf.infolist():
        member_path = Path(member.filename)
        if member_path.is_absolute():
            raise HTTPException(400, "Zip contains invalid entry paths")
        target = (destination / member_path).resolve(strict=False)
        if not target.is_relative_to(dest_root):
            raise HTTPException(400, "Zip contains invalid entry paths")
    zf.extractall(destination)


# ─── Routes: AOI upload ───────────────────────────────────────────────────────

@app.post("/api/runs/{run_id}/aoi")
async def upload_aoi(run_id: str, file: UploadFile = File(...)):
    run_id    = re.sub(r"[^A-Za-z0-9]", "", run_id)[:40]
    run_dir   = RUNS_DIR / run_id
    input_dir = run_dir / "inputs"
    input_dir.mkdir(parents=True, exist_ok=True)

    content  = await file.read()
    filename = _sanitize_filename(file.filename or "aoi")
    ext      = Path(filename).suffix.lower()
    dest     = input_dir / filename
    dest.write_bytes(content)

    try:
        if ext == ".zip":
            with tempfile.TemporaryDirectory() as tmp:
                zp = Path(tmp) / "aoi.zip"
                zp.write_bytes(content)
                with zipfile.ZipFile(zp) as zf:
                    zf.extractall(tmp)
                shp_files = list(Path(tmp).glob("**/*.shp"))
                if not shp_files:
                    raise HTTPException(400, "No .shp found in zip")
                # Also extract into input_dir for persistence
                with zipfile.ZipFile(dest) as zf:
                    _safe_extract_zip(zf, input_dir)
                gdf = gpd.read_file(shp_files[0])
        elif ext in (".geojson", ".json"):
            gdf = gpd.read_file(io.BytesIO(content))
        elif ext in (".parquet", ".geoparquet"):
            gdf = gpd.read_parquet(io.BytesIO(content))
        else:
            raise HTTPException(400, f"Unsupported file type: {ext}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, f"Failed to read AOI: {e}")

    if gdf.empty or gdf.geometry.isna().all():
        raise HTTPException(400, "AOI contains no valid geometries")

    gdf_4326 = gdf.to_crs(epsg=4326)
    bounds   = gdf_4326.total_bounds.tolist()
    preview  = json.loads(gdf_4326.head(200).to_json())

    return {
        "feature_count":   len(gdf),
        "crs":             str(gdf.crs),
        "bounds":          bounds,
        "geojson_preview": preview,
    }

# ─── Routes: Downloads ────────────────────────────────────────────────────────

@app.get("/api/runs/{run_id}/download/{product}")
def download_parquet(run_id: str, product: str):
    path = _find_product_parquet(run_id, product)
    def stream():
        with open(path, "rb") as f:
            while chunk := f.read(65536):
                yield chunk
    return StreamingResponse(
        stream(),
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f'attachment; filename="{path.name}"',
            "Content-Length": str(path.stat().st_size),
        },
    )

@app.get("/api/runs/{run_id}/download/{product}/csv")
def download_csv(run_id: str, product: str):
    import pyarrow.parquet as pq
    from starlette.background import BackgroundTask
    path     = _find_product_parquet(run_id, product)
    schema   = pq.read_schema(path)
    geo      = schema.metadata.get(b"geo") if schema.metadata else None
    geom_col = json.loads(geo).get("primary_column", "geometry") if geo else "geometry"
    non_geom = [c for c in schema.names if c != geom_col]
    cols_sql = ", ".join(f'"{c}"' for c in non_geom)

    tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
    tmp_path = tmp.name
    tmp.close()

    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"COPY (SELECT {cols_sql} FROM read_parquet('{path}')) "
            f"TO '{tmp_path}' (HEADER, DELIMITER ',')"
        )

    return FileResponse(
        tmp_path,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{path.stem}.csv"'},
        background=BackgroundTask(os.unlink, tmp_path),
    )

@app.get("/api/runs/{run_id}/download/{product}/partial-csv")
def download_partial_csv(run_id: str, product: str):
    import pyarrow.parquet as pq
    from starlette.background import BackgroundTask
    partial_dir = _results_dir(run_id) / "partial_checkout" / product
    files       = sorted(partial_dir.glob("*.parquet")) if partial_dir.exists() else []
    if not files:
        raise HTTPException(404, "No partial checkout file yet")
    path     = files[-1]
    schema   = pq.read_schema(path)
    geo      = schema.metadata.get(b"geo") if schema.metadata else None
    geom_col = json.loads(geo).get("primary_column", "geometry") if geo else "geometry"
    non_geom = [c for c in schema.names if c != geom_col]
    cols_sql = ", ".join(f'"{c}"' for c in non_geom)

    tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
    tmp_path = tmp.name
    tmp.close()

    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"COPY (SELECT {cols_sql} FROM read_parquet('{path}')) "
            f"TO '{tmp_path}' (HEADER, DELIMITER ',')"
        )

    return FileResponse(
        tmp_path,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{path.stem}.csv"'},
        background=BackgroundTask(os.unlink, tmp_path),
    )

@app.get("/api/runs/{run_id}/download/{product}/partial")
def download_partial_parquet(run_id: str, product: str):
    partial_dir = _results_dir(run_id) / "partial_checkout" / product
    files       = sorted(partial_dir.glob("*.parquet")) if partial_dir.exists() else []
    if not files:
        raise HTTPException(404, "No partial checkout file yet")
    path = files[-1]
    def stream():
        with open(path, "rb") as f:
            while chunk := f.read(65536):
                yield chunk
    return StreamingResponse(
        stream(),
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{path.name}"'},
    )

# ─── SPA static files (pixi / local dev only) ─────────────────────────────────
# Mount the built React app so a single `uvicorn` process serves everything.
# In Docker the nginx container handles this instead, so this is a no-op there.
_dist = APP_DIR / "frontend" / "dist"
if _dist.exists():
    app.mount("/", StaticFiles(directory=_dist, html=True), name="spa")

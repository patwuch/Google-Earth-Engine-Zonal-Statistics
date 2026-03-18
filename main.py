import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import sys
import streamlit as st
import streamlit.components.v1 as components
import yaml
import subprocess
import os
import uuid
import re
import calendar
import string
import secrets
import io
import hashlib
import signal
import time
import json
from collections import deque
import pandas as pd
import duckdb
from datetime import datetime
from pathlib import Path
import zipfile
import shutil
import tempfile
import geopandas as gpd

# --- Session State ---
if 'pipeline_running' not in st.session_state:
    st.session_state.pipeline_running = False
if 'last_config_hash' not in st.session_state:
    st.session_state.last_config_hash = None
if 'last_run_failed' not in st.session_state:
    st.session_state.last_run_failed = False
if 'active_run_payload' not in st.session_state:
    st.session_state.active_run_payload = None
if 'active_run_hash' not in st.session_state:
    st.session_state.active_run_hash = None
if 'active_run_id' not in st.session_state:
    st.session_state.active_run_id = None
if 'last_completed_run_id' not in st.session_state:
    st.session_state.last_completed_run_id = None
if 'last_submitted_run_id' not in st.session_state:
    st.session_state.last_submitted_run_id = None
if 'partial_merge_pid' not in st.session_state:
    st.session_state.partial_merge_pid = None
if 'partial_merge_run_id' not in st.session_state:
    st.session_state.partial_merge_run_id = None

# --- Directories ---
# Using /tmp for configs to avoid permission issues; using unique subdirs for sessions
# Auto-detect Docker (/app/data) vs local (./data) environment
_docker_data = Path("/app/data")
BASE_DATA_DIR = _docker_data if _docker_data.exists() else Path(__file__).parent / "data"
RUNS_DIR  = BASE_DATA_DIR / "runs"
CONFIG_DIR = Path("/tmp/gee_configs")
ACTIVE_SNAKEFILE = "Snakefile_parquet"
RUN_DB_PATH = RUNS_DIR / "run_state.duckdb"
GEE_KEY_PATH = Path(__file__).parent / "config" / "gee-key.json"

# Per-run directory helpers — all run artefacts live under RUNS_DIR/<run_id>/
def run_dir(run_id: str) -> Path:
    return RUNS_DIR / run_id

def results_dir(run_id: str) -> Path:
    return RUNS_DIR / run_id / "results"

def logs_dir(run_id: str) -> Path:
    return RUNS_DIR / run_id / "logs"

def intermediate_dir(run_id: str) -> Path:
    return RUNS_DIR / run_id / "intermediate"

for folder in [RUNS_DIR, CONFIG_DIR]:
    folder.mkdir(parents=True, exist_ok=True)

# --- GEE Key helpers ---
_REQUIRED_KEY_FIELDS = {"type", "project_id", "private_key", "client_email", "token_uri"}

def validate_gee_key(data: dict) -> str:
    """Return the service-account email if valid, raise ValueError otherwise."""
    missing = _REQUIRED_KEY_FIELDS - data.keys()
    if missing:
        raise ValueError(f"Key file is missing required fields: {', '.join(sorted(missing))}")
    if data.get("type") != "service_account":
        raise ValueError(f"Expected type 'service_account', got '{data.get('type')}'.")
    email = data.get("client_email", "")
    if not email or "@" not in email:
        raise ValueError("client_email is empty or malformed.")
    return email

def save_gee_key(raw_bytes: bytes) -> str:
    """Parse, validate, and persist a GEE key. Returns the service-account email."""
    try:
        data = json.loads(raw_bytes.decode("utf-8"))
    except Exception:
        raise ValueError("File is not valid JSON.")
    email = validate_gee_key(data)
    GEE_KEY_PATH.parent.mkdir(parents=True, exist_ok=True)
    GEE_KEY_PATH.write_bytes(raw_bytes)
    return email

def read_gee_key_email() -> str | None:
    """Return the service-account email from the stored key, or None if absent/invalid."""
    if not GEE_KEY_PATH.exists():
        return None
    try:
        data = json.loads(GEE_KEY_PATH.read_text(encoding="utf-8"))
        return data.get("client_email")
    except Exception:
        return None

def _duckdb_connect(path: str, retries: int = 8, delay: float = 0.25):
    """Open a DuckDB write connection with retry on lock contention."""
    last_exc = None
    for attempt in range(retries):
        try:
            return duckdb.connect(path)
        except duckdb.IOException as e:
            if "lock" not in str(e).lower():
                raise
            last_exc = e
            time.sleep(delay * (attempt + 1))
    raise last_exc


def ensure_run_db():
    """Initialize DuckDB tables used for run status and event logs."""
    with _duckdb_connect(str(RUN_DB_PATH)) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS run_status (
                run_id VARCHAR PRIMARY KEY,
                status VARCHAR,
                attempts INTEGER,
                config_hash VARCHAR,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                last_error VARCHAR,
                snakemake_pid BIGINT,
                snakemake_log_path VARCHAR,
                snakemake_config_path VARCHAR,
                snakefile VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS run_events (
                event_time TIMESTAMP,
                run_id VARCHAR,
                event_type VARCHAR,
                status VARCHAR,
                message VARCHAR,
                payload_json VARCHAR
            )
            """
        )
        conn.execute(
            """
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
            """
        )

def upsert_run_status_sql(run_id: str, record: dict):
    """Mirror run registry state to DuckDB for SQL-based dashboards/queries."""
    ensure_run_db()
    with _duckdb_connect(str(RUN_DB_PATH)) as conn:
        conn.execute(
            """
            INSERT INTO run_status (
                run_id, status, attempts, config_hash,
                created_at, updated_at, last_error,
                snakemake_pid, snakemake_log_path,
                snakemake_config_path, snakefile
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                status = excluded.status,
                attempts = excluded.attempts,
                config_hash = excluded.config_hash,
                updated_at = excluded.updated_at,
                last_error = excluded.last_error,
                snakemake_pid = excluded.snakemake_pid,
                snakemake_log_path = excluded.snakemake_log_path,
                snakemake_config_path = excluded.snakemake_config_path,
                snakefile = excluded.snakefile
            """,
            [
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
                record.get("snakefile", ACTIVE_SNAKEFILE),
            ],
        )

def append_run_event_sql(run_id: str, event_type: str, status: str, message: str = "", payload=None):
    """Append run events to DuckDB for SQL history queries."""
    ensure_run_db()
    payload_json = json.dumps(payload or {}, default=str)
    with _duckdb_connect(str(RUN_DB_PATH)) as conn:
        conn.execute(
            """
            INSERT INTO run_events (event_time, run_id, event_type, status, message, payload_json)
            VALUES (CURRENT_TIMESTAMP, ?, ?, ?, ?, ?)
            """,
            [run_id, event_type, status, message, payload_json],
        )

def read_log_tail(log_path: str, max_lines: int = 20):
    """Read last N lines from a run log without loading full file into memory."""
    if not log_path:
        return []
    path = Path(log_path)
    if not path.exists():
        return []
    tail = deque(maxlen=max_lines)
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            tail.append(line.rstrip())
    return list(tail)

def get_run_progress_sql(run_id: str, payload: dict):
    """Compute run progress with SQL queries over parquet/chunk files."""
    products = payload.get("products", {}) or {}
    expected_chunks = sum(
        len(cfg.get("bands", [])) * len(cfg.get("time_chunks", []))
        for cfg in products.values()
    )
    expected_final = len(products)

    chunk_glob = str((intermediate_dir(run_id) / "chunks" / "**" / "*.parquet").as_posix())
    final_glob = str((results_dir(run_id) / "*" / "*.parquet").as_posix())

    with duckdb.connect(":memory:") as conn:
        done_chunks = conn.execute("SELECT COUNT(*) FROM glob(?)", [chunk_glob]).fetchone()[0]
        done_final = conn.execute("SELECT COUNT(*) FROM glob(?)", [final_glob]).fetchone()[0]

    return {
        "expected_chunks": expected_chunks,
        "done_chunks": int(done_chunks),
        "expected_final": expected_final,
        "done_final": int(done_final),
    }

ensure_run_db()

def initialise_jobs(run_id: str, payload: dict):
    """Pre-populate the jobs table from the run payload before Snakemake launches.

    Chunks whose output parquet already exists are marked 'done' immediately so
    that resumed runs show correct progress from the first page load.
    """
    products = payload.get("products", {}) or {}
    rows = []
    for prod, cfg in products.items():
        for band in cfg.get("bands", []):
            for chunk in cfg.get("time_chunks", []):
                chunk_path = intermediate_dir(run_id) / "chunks" / prod / f"{band}_{chunk}.parquet"
                status = "done" if chunk_path.exists() else "pending"
                rows.append((run_id, prod, band, chunk, status))
    if not rows:
        return
    ensure_run_db()
    with _duckdb_connect(str(RUN_DB_PATH)) as conn:
        conn.executemany(
            """
            INSERT INTO jobs (run_id, product, band, time_chunk, status)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (run_id, product, band, time_chunk) DO UPDATE SET
                status = CASE
                    WHEN jobs.status = 'done'    THEN 'done'
                    WHEN jobs.status = 'failed'  THEN jobs.status
                    ELSE excluded.status
                END
            """,
            rows,
        )

def get_job_progress(run_id: str) -> dict:
    """Return per-status job counts from the jobs table.

    Falls back to an empty dict if the table has no rows for this run yet
    (e.g. older runs started before the jobs table existed).
    """
    ensure_run_db()
    try:
        with _duckdb_connect(str(RUN_DB_PATH)) as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) FROM jobs WHERE run_id = ? GROUP BY status",
                [run_id],
            ).fetchall()
    except Exception:
        return {}
    counts = {r[0]: int(r[1]) for r in rows}
    total = sum(counts.values())
    if total == 0:
        return {}
    return {
        "pending":  counts.get("pending",  0),
        "running":  counts.get("running",  0),
        "done":     counts.get("done",     0),
        "failed":   counts.get("failed",   0),
        "total":    total,
    }

def hashpayload(payload):
    """Generate hash of config payload to detect if user changed configuration"""
    return hashlib.md5(str(sorted(payload.items())).encode()).hexdigest()

def sanitize_run_id(run_id: str) -> str:
    """Allow only alphanumeric characters for run IDs."""
    cleaned = re.sub(r"[^A-Za-z0-9]", "", (run_id or "").strip())
    return cleaned[:40]

def generate_run_id(length: int = 6) -> str:
    """Generate a six-character alphanumeric run ID."""
    alphabet = string.ascii_uppercase + string.digits
    while True:
        candidate = "".join(secrets.choice(alphabet) for _ in range(length))
        if not (RUNS_DIR / candidate).exists():
            return candidate

def update_run_registry(run_id, payload, config_hash=None, status="queued", error_message=None, bump_attempt=False):
    """Persist run metadata for resume/audit support."""
    run_file = RUNS_DIR / run_id / "run.yaml"
    run_file.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.utcnow().isoformat()

    existing = {}
    if run_file.exists():
        with open(run_file, "r", encoding="utf-8") as f:
            existing = yaml.safe_load(f) or {}

    attempts = existing.get("attempts", 0)
    if bump_attempt:
        attempts += 1

    record = {
        "run_id": run_id,
        "status": status,
        "created_at": existing.get("created_at", now),
        "updated_at": now,
        "attempts": attempts,
        "config_hash": config_hash,
        "payload": payload,
        "last_error": error_message,
        "snakemake_pid": existing.get("snakemake_pid"),
        "snakemake_log_path": existing.get("snakemake_log_path"),
        "snakemake_config_path": existing.get("snakemake_config_path"),
        "snakefile": existing.get("snakefile", ACTIVE_SNAKEFILE),
        "last_started_at": existing.get("last_started_at"),
        "last_finished_at": existing.get("last_finished_at")
    }

    if status == "running":
        record["last_started_at"] = now
    if status in {"completed", "failed"}:
        record["last_finished_at"] = now

    with open(run_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(record, f, sort_keys=False)

    upsert_run_status_sql(run_id, record)
    append_run_event_sql(
        run_id=run_id,
        event_type="status_change",
        status=status,
        message=error_message or "",
        payload={"attempts": record.get("attempts", 0)},
    )

def set_run_snakemake_pid(run_id: str, pid: int):
    """Attach current snakemake process ID to the run registry."""
    run_file = RUNS_DIR / run_id / "run.yaml"
    if not run_file.exists():
        return
    with open(run_file, "r", encoding="utf-8") as f:
        record = yaml.safe_load(f) or {}
    record["snakemake_pid"] = int(pid)
    record["updated_at"] = datetime.utcnow().isoformat()
    with open(run_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(record, f, sort_keys=False)
    upsert_run_status_sql(run_id, record)

def set_run_execution_metadata(run_id: str, pid: int, log_path: str, config_path: str, snakefile: str):
    """Persist run execution metadata used for async monitoring."""
    run_file = RUNS_DIR / run_id / "run.yaml"
    if not run_file.exists():
        return

    with open(run_file, "r", encoding="utf-8") as f:
        record = yaml.safe_load(f) or {}

    record["snakemake_pid"] = int(pid)
    record["snakemake_log_path"] = str(log_path)
    record["snakemake_config_path"] = str(config_path)
    record["snakefile"] = snakefile
    record["updated_at"] = datetime.utcnow().isoformat()

    with open(run_file, "w", encoding="utf-8") as f:
        yaml.safe_dump(record, f, sort_keys=False)

    upsert_run_status_sql(run_id, record)
    append_run_event_sql(
        run_id=run_id,
        event_type="pipeline_started",
        status=record.get("status", "running"),
        message=f"PID {pid} using {snakefile}",
        payload={"log_path": str(log_path), "config_path": str(config_path)},
    )

def is_pid_alive(pid):
    """Check whether a process ID is currently alive."""
    try:
        os.kill(int(pid), 0)
        return True
    except Exception:
        return False

def get_pid_cmdline(pid):
    """Return process cmdline as text, or empty string if unavailable."""
    try:
        pid = int(pid)
        cmdline_path = Path(f"/proc/{pid}/cmdline")
        if not cmdline_path.exists():
            return ""
        raw = cmdline_path.read_bytes()
        return raw.replace(b"\x00", b" ").decode("utf-8", errors="replace").strip()
    except Exception:
        return ""

def list_snakemake_pids():
    """Find running Snakemake process IDs from /proc."""
    pids = []
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        pid = int(entry)
        cmdline = get_pid_cmdline(pid)
        if cmdline and "snakemake" in cmdline and "--snakefile" in cmdline:
            pids.append(pid)
    return sorted(pids)

def get_active_run():
    """Return (run_id, pid) of any run currently holding a live Snakemake process.

    Scans all saved runs whose registry status is 'running' and checks whether
    the stored PID is still alive. Returns (None, None) if nothing is executing.
    """
    for run_meta in list_saved_runs():
        if run_meta.get("status") != "running":
            continue
        pid = run_meta.get("snakemake_pid")
        if pid and is_pid_alive(int(pid)):
            cmdline = get_pid_cmdline(int(pid))
            if "snakemake" in cmdline:
                return run_meta.get("run_id"), int(pid)
    return None, None

def resolve_snakemake_pid(run_meta: dict):
    """Resolve a trustworthy snakemake PID for this run."""
    stored_pid = run_meta.get("snakemake_pid") if run_meta else None
    if stored_pid and is_pid_alive(stored_pid):
        cmdline = get_pid_cmdline(stored_pid)
        if "snakemake" in cmdline and "--snakefile" in cmdline:
            return int(stored_pid), "stored"

    candidate_pids = list_snakemake_pids()
    if len(candidate_pids) == 1:
        return candidate_pids[0], "discovered"

    return None, "none"

def get_descendant_pids(root_pid):
    """Return all descendant PIDs for a given process using /proc."""
    try:
        root_pid = int(root_pid)
    except Exception:
        return []

    parent_map = {}
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        stat_path = f"/proc/{entry}/stat"
        try:
            with open(stat_path, "r", encoding="utf-8") as f:
                stat_content = f.read()
            after_paren = stat_content.rsplit(")", 1)[1].strip()
            parts = after_paren.split()
            if len(parts) >= 3:
                ppid = int(parts[1])
                pid = int(entry)
                parent_map.setdefault(ppid, []).append(pid)
        except Exception:
            continue

    descendants = []
    stack = [root_pid]
    while stack:
        current = stack.pop()
        children = parent_map.get(current, [])
        descendants.extend(children)
        stack.extend(children)

    return descendants

def get_expected_result_files(payload: dict):
    """Build expected product-level final result files from payload."""
    run_id = payload.get("run_id")
    if not run_id:
        return []
    products = payload.get("products", {}) or {}
    expected = []
    for prod_id, prod_cfg in products.items():
        start_date = prod_cfg.get("start_date")
        end_date = prod_cfg.get("end_date")
        parquet_path = results_dir(run_id) / prod_id / f"{prod_id}_{start_date}_to_{end_date}.parquet"
        csv_path = results_dir(run_id) / prod_id / f"{prod_id}_{start_date}_to_{end_date}.csv"
        expected.append(parquet_path if parquet_path.exists() else csv_path)
    return expected

def reconcile_run_status(run_id: str, run_meta):
    """Auto-correct stale 'running' states after refresh/interruption."""
    if not run_meta or run_meta.get("status") != "running":
        return run_meta

    payload = run_meta.get("payload", {}) or {}
    expected_files = get_expected_result_files(payload)
    if expected_files and all(path.exists() for path in expected_files):
        update_run_registry(
            run_id=run_id,
            payload=payload,
            config_hash=run_meta.get("config_hash"),
            status="completed"
        )
        return load_run_registry(run_id)

    resolved_pid, source = resolve_snakemake_pid(run_meta)
    if source == "discovered" and resolved_pid is not None:
        set_run_snakemake_pid(run_id, int(resolved_pid))
    if not resolved_pid:
        update_run_registry(
            run_id=run_id,
            payload=payload,
            config_hash=run_meta.get("config_hash"),
            status="failed",
            error_message="Run interrupted or app session ended before status finalization."
        )
        try:
            subprocess.run(
                ["snakemake", "--unlock",
                 "--snakefile", str(Path(__file__).parent / ACTIVE_SNAKEFILE),
                 "--directory", str(run_dir(run_id))],
                capture_output=True, text=True, timeout=10,
                cwd=str(run_dir(run_id)),
            )
        except Exception:
            pass
        return load_run_registry(run_id)

    return run_meta

def stop_run_by_pid(pid):
    """Terminate a running Snakemake process and its child workers."""
    try:
        pid = int(pid)
    except Exception:
        return False, "Invalid PID in run registry."

    if not is_pid_alive(pid):
        return True, "Process is already stopped."

    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return True, "Process is already stopped."
    except Exception as e:
        return False, f"Could not terminate process {pid}: {str(e)}"

    descendants = get_descendant_pids(pid)
    for child_pid in descendants:
        try:
            os.kill(child_pid, signal.SIGTERM)
        except Exception:
            pass

    deadline = time.monotonic() + 1.5
    while time.monotonic() < deadline and is_pid_alive(pid):
        time.sleep(0.1)

    if is_pid_alive(pid):
        try:
            os.kill(pid, signal.SIGKILL)
            descendants = get_descendant_pids(pid)
            for child_pid in descendants:
                try:
                    os.kill(child_pid, signal.SIGKILL)
                except Exception:
                    pass
            deadline_kill = time.monotonic() + 0.5
            while time.monotonic() < deadline_kill and is_pid_alive(pid):
                time.sleep(0.1)
        except Exception as e:
            return False, f"Failed to force-kill process {pid}: {str(e)}"

    if is_pid_alive(pid):
        return False, f"Process {pid} is still running."

    return True, f"Stopped process {pid}."

def load_run_registry(run_id: str):
    """Load run metadata from disk, if available."""
    if not run_id:
        return None
    run_file = RUNS_DIR / run_id / "run.yaml"
    if not run_file.exists():
        return None
    with open(run_file, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or None

def list_saved_runs():
    """Return metadata for each saved RUN ID found on disk."""
    runs = []
    for run_file in sorted(RUNS_DIR.glob("*/run.yaml"), key=lambda p: p.stat().st_mtime, reverse=True):
        run_meta = load_run_registry(run_file.parent.name)
        if run_meta:
            runs.append(run_meta)
    return runs

def delete_run_artifacts(run_id: str):
    """Remove the entire run directory (results, logs, chunks, metadata)."""
    target = RUNS_DIR / run_id
    if target.exists():
        shutil.rmtree(target, ignore_errors=True)

def build_run_context_summary(run_meta: dict):
    """Create a compact, human-readable summary of a registered run."""
    if not run_meta:
        return None

    payload = run_meta.get("payload", {}) or {}
    products = payload.get("products", {}) or {}

    summary = {
        "run_id": run_meta.get("run_id"),
        "status": run_meta.get("status"),
        "attempts": run_meta.get("attempts", 0),
        "created_at": run_meta.get("created_at"),
        "updated_at": run_meta.get("updated_at"),
        "last_error": run_meta.get("last_error"),
        "shapefile": payload.get("shp_path"),
        "output_dir": payload.get("output_dir"),
        "products": {}
    }

    for product_id, cfg in products.items():
        bands = cfg.get("bands", [])
        time_chunks = cfg.get("time_chunks", [])
        summary["products"][product_id] = {
            "bands_selected": len(bands),
            "bands": bands,
            "statistics": cfg.get("statistics", []),
            "cadence": cfg.get("cadence"),
            "start_date": cfg.get("start_date"),
            "end_date": cfg.get("end_date"),
            "time_chunks": len(time_chunks)
        }

    return summary

def get_month_list(start_str, end_str):
    """Returns ['2024-06', '2024-07', ...] — flat list of every month in range."""
    start = datetime.strptime(start_str, '%Y-%m-%d')
    end = datetime.strptime(end_str, '%Y-%m-%d')
    months = []
    curr = start.replace(day=1)
    while curr <= end:
        months.append(curr.strftime('%Y-%m'))
        if curr.month == 12:
            curr = curr.replace(year=curr.year + 1, month=1)
        else:
            curr = curr.replace(month=curr.month + 1)
    return months

def get_quarterly_chunks(start_str, end_str):
    """
    Group months into 3-month batches. Any remainder (1 or 2 months) becomes
    its own trailing chunk.

    Returns e.g. ['2024-01_2024-03', '2024-04_2024-06', '2024-07_2024-08']
    for a range that covers 8 months.
    """
    months = get_month_list(start_str, end_str)
    chunks = []
    for i in range(0, len(months), 3):
        group = months[i:i + 3]
        chunks.append(f"{group[0]}_{group[-1]}")
    return chunks

def get_year_list(start_str, end_str):
    """Returns ['2020', '2021', ...] for annual products"""
    start = datetime.strptime(start_str, '%Y-%m-%d')
    end = datetime.strptime(end_str, '%Y-%m-%d')
    years = []
    curr_year = start.year
    while curr_year <= end.year:
        years.append(str(curr_year))
        curr_year += 1
    return years

def get_time_chunks(start_str, end_str, cadence="monthly"):
    """Returns time chunks based on cadence.
    Annual    → ['YYYY', ...].
    Monthly   → 3-month batches ['YYYY-MM_YYYY-MM', ...] with remainder chunk.
    Daily     → individual months ['YYYY-MM', ...] (per-image extraction, one month at a time).
    Composite → individual months ['YYYY-MM', ...] (per-composite extraction, one month at a time).
    """
    if cadence == "annual":
        return get_year_list(start_str, end_str)
    elif cadence == "daily":
        return get_month_list(start_str, end_str)
    elif cadence == "composite":
        return get_quarterly_chunks(start_str, end_str)
    else:
        return get_quarterly_chunks(start_str, end_str)

# --- Product Registry (Fixed MODIS bands) ---
PRODUCT_REGISTRY = {
    "CHIRPS": {
        "ee_collection": "UCSB-CHG/CHIRPS/DAILY",
        "min_date": datetime(1981, 1, 1),
        "max_date": datetime(2026, 2, 28),
        "native_resolution": "Daily",
        "scale": 5566,
        "cadence": "daily",
        "content": {
            "precipitation": {"stats": ["sum", "mean", "max"], "default_stats": ["sum"]}
        },
        "info": "Global precipitation (0.05° resolution)."
    },
    "ERA5_LAND": {
        "ee_collection": "ECMWF/ERA5_LAND/DAILY_AGGR",
        "min_date": datetime(1950, 1, 1),
        "max_date": datetime(2026, 2, 28),
        "native_resolution": "Daily",
        "scale": 9000,
        "cadence": "daily",
        "content": {
            "temperature_2m": {"stats": ["mean", "min", "max"], "default_stats": ["mean"]},
            "temperature_2m_min": {"stats": ["mean", "min", "max"], "default_stats": ["mean"]},
            "temperature_2m_max": {"stats": ["mean", "min", "max"], "default_stats": ["mean"]},
            "total_precipitation_sum": {"stats": ["sum", "mean"], "default_stats": ["sum"]},
            "total_evaporation_sum": {"stats": ["sum", "mean"], "default_stats": ["sum"]},
            "potential_evaporation_sum": {"stats": ["sum", "mean"], "default_stats": ["sum"]},
            "snow_evaporation_sum": {"stats": ["sum", "mean"], "default_stats": ["sum"]},
            "evaporation_from_bare_soil_sum": {"stats": ["sum", "mean"], "default_stats": ["sum"]},
            "evaporation_from_open_water_surfaces_excluding_oceans_sum": {"stats": ["sum", "mean"], "default_stats": ["sum"]},
            "evaporation_from_the_top_of_canopy_sum": {"stats": ["sum", "mean"], "default_stats": ["sum"]},
            "evaporation_from_vegetation_transpiration_sum": {"stats": ["sum", "mean"], "default_stats": ["sum"]}
        },
        "info": "Reanalysis data for land variables (9km resolution)."
    },
    "MODIS_LST": {
        "ee_collection": "MODIS/061/MOD11A2",
        "min_date": datetime(2000, 2, 18),
        "max_date": datetime(2026, 2, 10),
        "native_resolution": "8-day composite",
        "scale": 1000,
        "cadence": "composite",
        "content": {
            "LST_Day_1km": {"stats": ["mean", "median", "max"], "default_stats": ["mean"]},
            "LST_Night_1km": {"stats": ["mean", "median", "max"], "default_stats": ["mean"]}
        },
        "info": "Land Surface Temperature (1km resolution)."
    },
    "MODIS_NDVI_EVI": {
        "ee_collection": "MODIS/061/MOD13Q1",
        "min_date": datetime(2000, 2, 18),
        "max_date": datetime(2026, 2, 2),
        "native_resolution": "16-day composite",
        "scale": 250,
        "cadence": "composite",
        "content": {
            "NDVI": {"stats": ["mean", "median"], "default_stats": ["mean"]},
            "EVI": {"stats": ["mean", "median"], "default_stats": ["mean"]}
        },
        "info": "Vegetation Indices (250m resolution)."
    },
    "WorldCover_v100": {
        "ee_collection": "ESA/WorldCover/v100",
        "min_date": datetime(2020, 1, 1),
        "max_date": datetime(2020, 12, 31),
        "native_resolution": "Annual",
        "scale": 10,
        "cadence": "annual",
        "content": {
            "Map": {"stats": ["mean", "sum"], "default_stats": ["mean"]}
        },
        "info": "ESA WorldCover landcover classification v100 (10m resolution, annual product for 2020)."
    },
    "WorldCover_v200": {
        "ee_collection": "ESA/WorldCover/v200",
        "min_date": datetime(2021, 1, 1),
        "max_date": datetime(2021, 12, 31),
        "native_resolution": "Annual",
        "scale": 10,
        "cadence": "annual",
        "content": {
            "Map": {"stats": ["mean", "sum"], "default_stats": ["mean"]}
        },
        "info": "ESA WorldCover landcover classification v200 (10m resolution, annual product for 2021)."
    },
    "MODIS_LULC": {
        "ee_collection": "MODIS/061/MCD12Q1",
        "min_date": datetime(2001, 1, 1),
        "max_date": datetime(2023, 12, 31),
        "native_resolution": "Annual",
        "scale": 500,
        "cadence": "annual",
        "content": {
            "LC_Type1": {"stats": ["mean", "sum"], "default_stats": ["mean"]},
            "LC_Type2": {"stats": ["mean", "sum"], "default_stats": ["mean"]},
            "LC_Type3": {"stats": ["mean", "sum"], "default_stats": ["mean"]},
            "LC_Type4": {"stats": ["mean", "sum"], "default_stats": ["mean"]},
            "LC_Type5": {"stats": ["mean", "sum"], "default_stats": ["mean"]}
        },
        "info": "MODIS Land Cover Type (500m resolution, annual product, multiple classification schemes)."
    }
}

REQUIRED_EXTS = {'.shp', '.shx', '.dbf'}
GEOJSON_EXTS = {'.geojson', '.json'}
GEOPARQUET_EXTS = {'.geoparquet', '.parquet'}

def validate_vector_file(vector_path: Path):
    """Validate uploaded vector file can be read and has geometry."""
    suffix = vector_path.suffix.lower()
    if suffix in GEOPARQUET_EXTS:
        gdf = gpd.read_parquet(vector_path)
    else:
        gdf = gpd.read_file(vector_path)

    if gdf.empty:
        raise ValueError("Uploaded vector file contains no features.")
    if "geometry" not in gdf.columns:
        raise ValueError("Uploaded vector file has no geometry column.")
    if gdf.geometry.isna().all():
        raise ValueError("Uploaded vector file contains only empty geometries.")


def validate_uploaded_aoi(uploaded_file):
    """Validate uploaded AOI in a temp directory without writing to the run. Raises ValueError on failure."""
    upload_bytes = uploaded_file.getvalue()
    filename = uploaded_file.name or "uploaded_aoi"
    suffix = Path(filename).suffix.lower()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        if suffix == ".zip":
            with zipfile.ZipFile(io.BytesIO(upload_bytes), 'r') as zip_ref:
                zip_ref.extractall(tmpdir_path)
            shp_files = list(tmpdir_path.rglob("*.shp"))
            if not shp_files:
                raise ValueError("No .shp file found in uploaded ZIP.")
            found_exts = {f.suffix.lower() for f in tmpdir_path.rglob("*") if f.is_file()}
            missing = REQUIRED_EXTS - found_exts
            if missing:
                raise ValueError(f"Missing components: {', '.join(sorted(missing))}")
            validate_vector_file(shp_files[0])
        elif suffix in GEOJSON_EXTS or suffix in GEOPARQUET_EXTS:
            tmp_path = tmpdir_path / filename
            tmp_path.write_bytes(upload_bytes)
            validate_vector_file(tmp_path)
        else:
            raise ValueError("Unsupported file type. Upload a ZIP shapefile, GeoJSON, or GeoParquet.")


def save_aoi_to_run(uploaded_file, run_id: str) -> str:
    """Save uploaded AOI into the run's inputs directory. Returns the geometry file path."""
    upload_bytes = uploaded_file.getvalue()
    filename = uploaded_file.name or "uploaded_aoi"
    suffix = Path(filename).suffix.lower()

    inputs_dir = run_dir(run_id) / "inputs"
    inputs_dir.mkdir(parents=True, exist_ok=True)

    if suffix == ".zip":
        with zipfile.ZipFile(io.BytesIO(upload_bytes), 'r') as zip_ref:
            zip_ref.extractall(inputs_dir)
        shp_files = list(inputs_dir.rglob("*.shp"))
        return str(shp_files[0])

    target_path = inputs_dir / filename
    target_path.write_bytes(upload_bytes)
    return str(target_path)

# --- Page Setup ---
st.set_page_config(page_title="GEE Batch Processor", layout="wide", page_icon="🛰️")
st.title("🛰️ Earth Engine Zonal Statistics")

# --- GEE Key Gate ---
# Block the rest of the UI until a valid service-account key is present.
_key_email = read_gee_key_email()

if not _key_email:
    st.warning("No Google Earth Engine credentials found. Provide your service-account key to continue.")

    with st.container(border=True):
        st.subheader("Connect to Google Earth Engine")
        st.markdown(
            "This app needs a **GEE service-account key** to query Earth Engine. "
            "You only need to do this once — the key is stored locally and reused on every restart.\n\n"
            "**How to get one:**\n"
            "1. Open the [Google Cloud Console](https://console.cloud.google.com/iam-admin/serviceaccounts) "
            "and select your project.\n"
            "2. Create (or open) a service account that has the **Earth Engine** role.\n"
            "3. Go to **Keys → Add Key → Create new key → JSON**.\n"
            "4. Either download the file and upload it, or copy its contents and paste below."
        )

        tab_upload, tab_paste = st.tabs(["Upload file", "Paste JSON"])

        def _handle_key_bytes(raw: bytes):
            """Try to save key bytes; show success or a specific error."""
            # Detect a bare alphanumeric token before attempting JSON parse
            stripped = raw.strip()
            if stripped and not stripped.startswith(b"{"):
                st.error(
                    "This looks like a plain text token, not a service-account key. "
                    "GEE requires the full JSON key object (starts with `{`). "
                    "Download it from Google Cloud Console under "
                    "**Service Accounts → Keys → Add Key → JSON**."
                )
                return
            try:
                email = save_gee_key(raw)
                st.success(f"Key accepted. Authenticated as: **{email}**")
                st.rerun()
            except ValueError as exc:
                st.error(f"Invalid key: {exc}")

        with tab_upload:
            uploaded_key = st.file_uploader(
                "Service-account key (.json)",
                type=["json"],
                help="The JSON key file downloaded from Google Cloud Console.",
                label_visibility="collapsed",
            )
            if uploaded_key is not None:
                _handle_key_bytes(uploaded_key.read())

        with tab_paste:
            pasted = st.text_area(
                "Paste the JSON key content here",
                height=160,
                placeholder='{\n  "type": "service_account",\n  "project_id": "...",\n  ...\n}',
                label_visibility="collapsed",
            )
            if st.button("Save pasted key", key="save_pasted_key"):
                if pasted.strip():
                    _handle_key_bytes(pasted.strip().encode("utf-8"))
                else:
                    st.warning("Nothing pasted yet.")

    st.stop()

# Key is present — show a compact indicator in the sidebar with a remove option.
with st.sidebar.expander("GEE credentials", expanded=False):
    st.caption(f"Authenticated as:\n`{_key_email}`")
    if st.button("Remove / replace key", key="remove_gee_key"):
        GEE_KEY_PATH.unlink(missing_ok=True)
        st.rerun()

st.sidebar.header("0. Run Session")
# Seed the text input from the URL so a full-page reload (e.g. auto-refresh)
# doesn't blank the field and force the user to retype the run ID.
_qp_run_id = st.query_params.get("run_id", "")
resume_run_id = sanitize_run_id(
    st.sidebar.text_input(
        "RUN ID (optional)",
        value=_qp_run_id,
        key="run_id_input",
        help="Leave blank to auto-generate a 6-character run ID. Enter an existing RUN ID to resume."
    )
)
# Keep the URL in sync so the value survives any page reload.
if resume_run_id and resume_run_id != _qp_run_id:
    st.query_params["run_id"] = resume_run_id
elif not resume_run_id and _qp_run_id:
    st.query_params.pop("run_id", None)
resume_run_meta = load_run_registry(resume_run_id)
resume_run_meta = reconcile_run_status(resume_run_id, resume_run_meta)

if resume_run_id and resume_run_meta:
    resume_status = resume_run_meta.get("status", "unknown")
    st.sidebar.caption(f"Resuming existing RUN ID: {resume_run_id} (status: {resume_status})")
elif resume_run_id:
    st.sidebar.caption(f"RUN ID {resume_run_id} not found in registry; a new run record will be created.")

if resume_run_meta and resume_run_meta.get("status") == "running":
    run_pid, run_pid_source = resolve_snakemake_pid(resume_run_meta)
    if run_pid and run_pid_source == "discovered":
        set_run_snakemake_pid(resume_run_id, run_pid)
    if run_pid:
        st.sidebar.caption(f"Active process PID: {run_pid}")
    btn_col1, btn_col2 = st.sidebar.columns(2)
    if btn_col1.button("⛔ Stop", key=f"stop_run_{resume_run_id}"):
        if run_pid:
            stopped, message = stop_run_by_pid(run_pid)
            if stopped:
                update_run_registry(
                    run_id=resume_run_id,
                    payload=(resume_run_meta.get("payload") or {}),
                    config_hash=resume_run_meta.get("config_hash"),
                    status="stopped",
                    error_message="Stopped by user from UI."
                )
                st.sidebar.success(message)
                st.rerun()
            else:
                st.sidebar.error(message)
        else:
            st.sidebar.error("Cannot stop: no active Snakemake PID could be resolved for this run.")
    if btn_col2.button("🔓 Unlock", key=f"unlock_run_{resume_run_id}"):
        try:
            subprocess.run(
                ["snakemake", "--unlock",
                 "--snakefile", str(Path(__file__).parent / ACTIVE_SNAKEFILE),
                 "--directory", str(run_dir(resume_run_id))],
                capture_output=True, text=True, timeout=10,
                cwd=str(run_dir(resume_run_id)),
            )
            st.sidebar.success("Directory unlocked.")
        except Exception as e:
            st.sidebar.error(f"Unlock failed: {e}")

if resume_run_meta:
    run_context_summary = build_run_context_summary(resume_run_meta)
    with st.sidebar.expander("📄 Registered Run Summary", expanded=False):
        st.code(yaml.safe_dump(run_context_summary, sort_keys=False), language="yaml")

saved_runs = list_saved_runs()
with st.sidebar.expander("📂 Local RUN registry", expanded=False):
    if saved_runs:
        summary_rows = [
            {
                "RUN ID": run.get("run_id"),
                "Status": run.get("status", "unknown"),
                "Attempts": run.get("attempts", 0),
                "Updated": run.get("updated_at"),
            }
            for run in saved_runs
        ]
        st.dataframe(pd.DataFrame(summary_rows), use_container_width=True)

        select_options = [""] + [run.get("run_id") for run in saved_runs]
        selected_saved_run = st.selectbox(
            "Inspect a stored RUN",
            options=select_options,
            index=0,
            key="saved_run_select"
        )
        if selected_saved_run:
            selected_meta = next(
                (run for run in saved_runs if run.get("run_id") == selected_saved_run),
                None,
            )
            if selected_meta:
                summary = build_run_context_summary(selected_meta)
                if summary:
                    st.code(yaml.safe_dump(summary, sort_keys=False), language="yaml")
            if st.button("🗑️ Delete this saved run", key=f"delete_saved_{selected_saved_run}"):
                delete_run_artifacts(selected_saved_run)
                st.rerun()
    else:
        st.caption("No saved RUNs yet. Start a run to populate the registry.")

failed_resume_payload = None
if resume_run_meta and resume_run_meta.get("status") in {"failed", "stopped"}:
    payload_candidate = resume_run_meta.get("payload")
    if isinstance(payload_candidate, dict) and payload_candidate.get("products"):
        failed_resume_payload = payload_candidate

# --- 1. Product Selection Sidebar ---
st.sidebar.header("1. Data Parameters")
product_configs = {}

for prod_id, info in PRODUCT_REGISTRY.items():
    with st.sidebar.expander(f"📦 {prod_id}"):
        active = st.checkbox(f"Enable {prod_id}", key=f"check_{prod_id}")
        if active:
            native_resolution = info.get("native_resolution", "Not specified")
            processing_cadence = info.get("cadence", "monthly").capitalize()
            st.caption(
                f"Native temporal resolution: {native_resolution} | "
                f"Processing cadence in this app: {processing_cadence}"
            )

            # 1. Band Selection
            available_bands = list(info["content"].keys())
            bands = st.multiselect(
                "Select Bands", 
                options=available_bands,
                default=available_bands,
                key=f"bands_{prod_id}"
            )
            
            # 2. Statistics Selection (The part I previously missed)
            # We'll pull available stats from the first selected band's registry
            if bands:
                first_band = bands[0]
                available_stats = info["content"][first_band]["stats"]
                default_stats = info["content"][first_band]["default_stats"]
                
                stats = st.multiselect(
                    "Select Statistics (Reducers)",
                    options=available_stats,
                    default=default_stats,
                    key=f"stats_{prod_id}"
                )

            # 3. Date Selection (cadence-aware)
            cadence = info.get("cadence", "monthly")
            selected_start_date = None
            selected_end_date = None

            if cadence == "monthly":
                month_options = get_month_list(
                    info["min_date"].strftime('%Y-%m-%d'),
                    info["max_date"].strftime('%Y-%m-%d')
                )
                start_month = st.selectbox(
                    "Start Month",
                    options=month_options,
                    index=0,
                    key=f"start_month_{prod_id}"
                )
                end_month = st.selectbox(
                    "End Month",
                    options=month_options,
                    index=len(month_options) - 1,
                    key=f"end_month_{prod_id}"
                )

                start_dt = datetime.strptime(start_month, "%Y-%m")
                end_dt = datetime.strptime(end_month, "%Y-%m")
                selected_start_date = datetime(start_dt.year, start_dt.month, 1)
                selected_end_date = datetime(
                    end_dt.year,
                    end_dt.month,
                    calendar.monthrange(end_dt.year, end_dt.month)[1]
                )

            elif cadence == "annual":
                year_options = get_year_list(
                    info["min_date"].strftime('%Y-%m-%d'),
                    info["max_date"].strftime('%Y-%m-%d')
                )
                start_year = st.selectbox(
                    "Start Year",
                    options=year_options,
                    index=0,
                    key=f"start_year_{prod_id}"
                )
                end_year = st.selectbox(
                    "End Year",
                    options=year_options,
                    index=len(year_options) - 1,
                    key=f"end_year_{prod_id}"
                )

                selected_start_date = datetime(int(start_year), 1, 1)
                selected_end_date = datetime(int(end_year), 12, 31)

                if selected_start_date < info["min_date"]:
                    selected_start_date = info["min_date"]
                if selected_end_date > info["max_date"]:
                    selected_end_date = info["max_date"]

            else:
                date_range = st.date_input(
                    "Select Date Range",
                    value=(info["min_date"], info["max_date"]),
                    min_value=info["min_date"],
                    max_value=info["max_date"],
                    key=f"date_{prod_id}"
                )
                if isinstance(date_range, tuple) and len(date_range) == 2:
                    selected_start_date = date_range[0]
                    selected_end_date = date_range[1]

            if cadence in ("daily", "composite"):
                st.caption(
                    f"Output will contain one row per region per {native_resolution.lower()} image within "
                    f"the selected range. Column names follow the pattern {{band}}_{{stat}} where stat "
                    f"is the spatial reducer applied over pixels within each region."
                )
            else:
                st.caption(
                    f"Output is temporally aggregated over the selected period ({cadence}). "
                    f"Column names follow the pattern {{band}}_{{stat}} where stat reflects the "
                    f"reduction applied across all images in the window (e.g. LST_Day_1km_mean)."
                )

            valid_date_range = (
                selected_start_date is not None and
                selected_end_date is not None and
                selected_start_date <= selected_end_date
            )

            # Verification: Ensure all fields are filled before adding to config
            if bands and stats and valid_date_range and selected_start_date and selected_end_date:
                product_configs[prod_id] = {
                    "ee_collection": info["ee_collection"],
                    "bands": bands,
                    "statistics": stats, # Now correctly passed to Snakemake
                    "scale": info["scale"],
                    "start_date": selected_start_date.strftime('%Y-%m-%d'),
                    "end_date": selected_end_date.strftime('%Y-%m-%d')
                }
            elif active:
                st.warning("Please select at least one band, one statistic, and a valid date range.")

# --- Helper Functions ---
def summarize_jobs_from_payload(payload):
    """Compute expected job counts directly from payload."""
    products = payload.get("products", {})

    extraction_jobs = 0
    merge_jobs = 0
    for product_conf in products.values():
        bands = product_conf.get("bands", [])
        time_chunks = product_conf.get("time_chunks", [])
        extraction_jobs += len(bands) * len(time_chunks)
        if bands:
            merge_jobs += 1

    return {
        "extraction": extraction_jobs,
        "merge": merge_jobs,
        "total": extraction_jobs + merge_jobs
    }

def render_registered_run_context(run_id: str, run_meta: dict):
    """Show rich context for a registered RUN ID (including optional plan preview)."""
    if not run_meta:
        return

    payload = run_meta.get("payload", {}) or {}
    products = payload.get("products", {}) or {}
    if not products:
        return

    st.subheader("📌 Registered Run Context")

    summary_col1, summary_col2, summary_col3 = st.columns(3)
    with summary_col1:
        st.metric("Run Status", str(run_meta.get("status", "unknown")).upper())
    with summary_col2:
        st.metric("Attempts", int(run_meta.get("attempts", 0)))
    with summary_col3:
        st.metric("Products", len(products))

    stats = summarize_jobs_from_payload(payload)
    plan_col1, plan_col2, plan_col3 = st.columns(3)
    with plan_col1:
        st.metric("Total Jobs", stats["total"])
    with plan_col2:
        st.metric("Data Extraction Jobs", stats["extraction"])
    with plan_col3:
        st.metric("Merge Jobs", stats["merge"])

    job_progress = get_job_progress(run_id)
    if job_progress:
        jp1, jp2, jp3, jp4 = st.columns(4)
        with jp1:
            st.metric("⏳ Pending",  job_progress["pending"])
        with jp2:
            st.metric("🔄 Running",  job_progress["running"])
        with jp3:
            st.metric("✅ Done",     job_progress["done"])
        with jp4:
            st.metric("❌ Failed",   job_progress["failed"])
        total = job_progress["total"]
        done  = job_progress["done"]
        st.progress(done / total, text=f"{done} / {total} chunks complete")
        if job_progress["failed"] > 0:
            with st.expander(f"❌ Failed jobs ({job_progress['failed']})", expanded=True):
                try:
                    with duckdb.connect(str(RUN_DB_PATH)) as _jconn:
                        failed_df = _jconn.execute(
                            """
                            SELECT product, band, time_chunk, error, log_path
                            FROM jobs
                            WHERE run_id = ? AND status = 'failed'
                            ORDER BY product, band, time_chunk
                            """,
                            [run_id],
                        ).fetchdf()
                    st.dataframe(failed_df, use_container_width=True, hide_index=True)
                except Exception as _e:
                    st.caption(f"Could not load failed job details: {_e}")
    else:
        # Fallback for runs started before the jobs table existed.
        try:
            sql_progress = get_run_progress_sql(run_id, payload)
            prog_col1, prog_col2 = st.columns(2)
            with prog_col1:
                st.metric(
                    "Chunk Progress",
                    f"{sql_progress['done_chunks']} / {sql_progress['expected_chunks']}"
                )
            with prog_col2:
                st.metric(
                    "Product Merges",
                    f"{sql_progress['done_final']} / {sql_progress['expected_final']}"
                )
        except Exception as sql_error:
            st.caption(f"Progress unavailable: {sql_error}")

    with st.expander("📦 Product Selection in This Run", expanded=False):
        for product_id, config in products.items():
            bands_count = len(config.get("bands", []))
            chunks_count = len(config.get("time_chunks", []))
            st.write(
                f"- {product_id}: {bands_count} band(s), {chunks_count} time chunk(s), "
                f"{config.get('start_date')} to {config.get('end_date')}"
            )

    run_log = run_meta.get("snakemake_log_path")
    if run_log:
        with st.expander("🧾 Live Snakemake Log Tail", expanded=False):
            lines = read_log_tail(run_log, max_lines=40)
            if lines:
                st.code("\n".join(lines), language="text")
            else:
                st.caption("No log lines yet.")



def parquet_to_csv_bytes(parquet_path: Path) -> bytes:
    """Convert a GeoParquet file to a CSV with geometry removed.

    Persists the CSV next to the source parquet on first conversion so users
    can access it directly from the filesystem. Returns the CSV bytes.
    """
    csv_path = parquet_path.with_suffix(".csv")
    if csv_path.exists():
        return csv_path.read_bytes()
    gdf = gpd.read_parquet(parquet_path)
    drop_cols = [gdf.geometry.name] if gdf.geometry is not None else []
    df = pd.DataFrame(gdf).drop(columns=drop_cols, errors="ignore")
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    csv_path.write_bytes(csv_bytes)
    return csv_bytes


def run_pipeline(config_path, payload, run_id):
    # Show execution plan from payload without blocking subprocess calls.
    stats = summarize_jobs_from_payload(payload)
    plan_col1, plan_col2, plan_col3 = st.columns(3)
    with plan_col1:
        st.metric("Total Jobs", stats["total"])
    with plan_col2:
        st.metric("Data Extraction Jobs", stats["extraction"])
    with plan_col3:
        st.metric("Merge Jobs", stats["merge"])

    # Pre-populate the jobs table so progress is visible from the first refresh.
    initialise_jobs(run_id, payload)

    # Launch the actual pipeline in the background to keep Streamlit responsive.
    _log_handler_script = Path(__file__).parent / "scripts" / "snakemake_log_handler.py"
    _run_dir = run_dir(run_id)
    _run_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        "snakemake", "--snakefile", str(Path(__file__).parent / ACTIVE_SNAKEFILE),
        "--configfile", str(config_path),
        "--directory", str(_run_dir),
        "-j", "12", "--resources", "gee=3",
        "--rerun-incomplete", "--keep-going",
        "--log-handler-script", str(_log_handler_script),
    ]

    run_log_path = logs_dir(run_id) / "snakemake_run.log"
    run_log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(run_log_path, "a", encoding="utf-8") as log_handle:
        log_handle.write(f"\n[{datetime.utcnow().isoformat()}] Launching: {' '.join(cmd)}\n")

    # Pass run context to the log handler via environment variables.
    _env = os.environ.copy()
    _env["GEE_RUN_ID"] = run_id
    _env["GEE_DB_PATH"] = str(RUN_DB_PATH)

    log_handle = open(run_log_path, "a", encoding="utf-8")
    process = subprocess.Popen(
        cmd,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        text=True,
        close_fds=True,
        env=_env,
        cwd=str(_run_dir),
    )
    log_handle.close()

    set_run_execution_metadata(
        run_id=run_id,
        pid=process.pid,
        log_path=str(run_log_path),
        config_path=str(config_path),
        snakefile=ACTIVE_SNAKEFILE,
    )

    st.success(
        f"🚀 Pipeline submitted in background (RUN ID: {run_id}, PID: {process.pid}). "
        "Use the RUN ID field to monitor progress."
    )

# --- Main Layout ---
col_upload, col_status = st.columns([1, 1])

with col_upload:
    st.header("2. Area of Interest")

    monitor_run_id = resume_run_id or st.session_state.get("last_submitted_run_id")
    monitor_run_meta = load_run_registry(monitor_run_id) if monitor_run_id else None
    monitor_run_meta = reconcile_run_status(monitor_run_id, monitor_run_meta) if monitor_run_id else None
    if monitor_run_meta and monitor_run_meta.get("status") == "completed":
        st.session_state.last_completed_run_id = monitor_run_id

    is_active_resumed_run = (
        st.session_state.pipeline_running and
        st.session_state.active_run_id and
        resume_run_id and
        st.session_state.active_run_id == resume_run_id
    )

    if monitor_run_meta and not is_active_resumed_run:
        render_registered_run_context(monitor_run_id, monitor_run_meta)
        if monitor_run_meta.get("status") == "running":
            st.caption("Run is executing in background. Click refresh to update SQL progress and logs.")
            if st.button("🔄 Refresh Run Status", key=f"refresh_run_{monitor_run_id}"):
                st.rerun()
            _ar_default = st.query_params.get("auto_refresh") == "1"
            auto_refresh = st.checkbox(
                "Auto-refresh every 30s",
                value=_ar_default,
                key=f"auto_refresh_{monitor_run_id}"
            )
            if auto_refresh != _ar_default:
                if auto_refresh:
                    st.query_params["auto_refresh"] = "1"
                else:
                    st.query_params.pop("auto_refresh", None)
            if auto_refresh:
                components.html(
                    "<script>setTimeout(function(){window.parent.location.reload();}, 30000);</script>",
                    height=0,
                    width=0,
                )
    elif is_active_resumed_run:
        st.info(
            f"Resuming RUN ID {resume_run_id}. "
            "Using stored configuration; live execution context is shown below."
        )

    uploaded_aoi = st.file_uploader(
        "Upload AOI (Shapefile ZIP, GeoJSON, or GeoParquet)",
        type=["zip", "geojson", "json", "parquet", "geoparquet"]
    )

    shp_path = None
    aoi_ready = False
    registry_payload = (resume_run_meta or {}).get("payload", {}) if resume_run_meta else {}
    registry_shp_path = registry_payload.get("shp_path")

    if resume_run_id and registry_shp_path and Path(registry_shp_path).exists():
        shp_path = registry_shp_path
        aoi_ready = True
        st.info(f"Using geometry linked to RUN ID {resume_run_id}: {Path(registry_shp_path).name}")
        if uploaded_aoi is not None:
            st.warning("Uploaded AOI ignored while resuming this RUN ID to keep geometry consistent.")
    elif uploaded_aoi:
        try:
            validate_uploaded_aoi(uploaded_aoi)
            aoi_ready = True
            st.success(f"Verified: {uploaded_aoi.name}")
        except Exception as upload_error:
            st.error(str(upload_error))
    elif resume_run_id and registry_shp_path and not Path(registry_shp_path).exists():
        st.warning("Stored geometry file for this RUN ID was not found on disk. Upload the same file to continue consistently.")

    if failed_resume_payload:
        st.info("A failed run was detected for this RUN ID. Clicking resume will reuse the saved configuration.")

    # --- Start Pipeline ---
    can_resume_failed = bool(failed_resume_payload and aoi_ready)
    _active_run_id, _active_pid = get_active_run()
    _another_run_active = (
        _active_run_id is not None
        and _active_run_id != (resume_run_id or "___")
    )
    if _another_run_active:
        st.warning(
            f"Run **{_active_run_id}** is currently executing (PID {_active_pid}). "
            "Stop it or wait for it to finish before starting a new run."
        )
    btn_ready = (
        ((aoi_ready and product_configs) or can_resume_failed)
        and not st.session_state.pipeline_running
        and not _another_run_active
    )
    if st.session_state.pipeline_running:
        btn_label = "⏳ Pipeline Running..."
    elif can_resume_failed:
        btn_label = "▶️ Resume Failed Run"
    else:
        btn_label = "▶️ Run Analysis"
    
    if st.button(btn_label, type="primary", disabled=not btn_ready):
        if can_resume_failed:
            run_id = resume_run_id
            if uploaded_aoi is not None:
                shp_path = save_aoi_to_run(uploaded_aoi, run_id)
            payload = dict(failed_resume_payload or {})
            payload["run_id"] = run_id
            payload["shp_path"] = shp_path
            payload["output_dir"] = str(results_dir(run_id))
            payload.setdefault("chain_parallel_window", 3)
        else:
            run_id = resume_run_id if resume_run_id else generate_run_id(6)
            if uploaded_aoi is not None:
                shp_path = save_aoi_to_run(uploaded_aoi, run_id)

            # Freeze the run configuration so UI changes during execution do not affect this run
            product_tasks = {}
            for p_id, p_conf in product_configs.items():
                cadence = PRODUCT_REGISTRY[p_id].get("cadence", "monthly")
                product_tasks[p_id] = {
                    **p_conf,
                    "time_chunks": get_time_chunks(p_conf["start_date"], p_conf["end_date"], cadence),
                    "cadence": cadence
                }

            payload = {
                "run_id": run_id,
                "shp_path": shp_path,
                "products": product_tasks,
                "output_dir": str(results_dir(run_id)),
                "chain_parallel_window": 3
            }

        current_config_hash = hashpayload(payload)
        update_run_registry(
            run_id=run_id,
            payload=payload,
            config_hash=current_config_hash,
            status="queued"
        )

        st.session_state.active_run_id = run_id
        st.session_state.active_run_payload = payload
        st.session_state.active_run_hash = current_config_hash
        st.session_state.last_submitted_run_id = run_id
        st.session_state.pipeline_running = True
        
    if st.session_state.pipeline_running:
        payload = st.session_state.active_run_payload
        current_config_hash = st.session_state.active_run_hash
        active_run_id = st.session_state.active_run_id

        if not payload or not current_config_hash:
            st.error("Active run configuration is missing. Please start the analysis again.")
            st.session_state.pipeline_running = False
            st.session_state.active_run_payload = None
            st.session_state.active_run_hash = None
            st.stop()

        if not active_run_id or not isinstance(payload, dict):
            st.error("Active run state is invalid. Please submit the run again.")
            st.session_state.pipeline_running = False
            st.session_state.active_run_payload = None
            st.session_state.active_run_hash = None
            st.session_state.active_run_id = None
            st.stop()

        st.info("🔒 Run configuration is locked for this submission. Any UI changes apply to the next run.")
        st.info(f"🆔 RUN ID: {active_run_id}")

        # Only unlock stale lock files when no live Snakemake process holds them.
        # Unlocking while another process is running would corrupt that run.
        if not list_snakemake_pids():
            try:
                _active_dir = run_dir(active_run_id)
                subprocess.run(
                    ["snakemake", "--unlock",
                     "--snakefile", str(Path(__file__).parent / ACTIVE_SNAKEFILE),
                     "--directory", str(_active_dir)],
                    capture_output=True, text=True, timeout=10,
                    cwd=str(_active_dir),
                )
            except Exception:
                pass

        config_path = CONFIG_DIR / f"config_{uuid.uuid4().hex[:8]}.yaml"
        with open(config_path, "w", encoding="utf-8") as f:
            yaml.safe_dump({**payload, "app_dir": str(Path(__file__).parent)}, f, sort_keys=False)

        try:
            update_run_registry(
                run_id=active_run_id,
                payload=payload,
                config_hash=current_config_hash,
                status="running",
                bump_attempt=True,
            )
            run_pipeline(config_path, payload, run_id=active_run_id)
            st.session_state.last_config_hash = current_config_hash
            st.session_state.last_run_failed = False
        except Exception as e:
            st.session_state.last_config_hash = current_config_hash
            st.session_state.last_run_failed = True
            update_run_registry(
                run_id=active_run_id,
                payload=payload,
                config_hash=current_config_hash,
                status="failed",
                error_message=str(e),
            )
        finally:
            st.session_state.pipeline_running = False
            st.session_state.active_run_payload = None
            st.session_state.active_run_hash = None
            st.session_state.active_run_id = None

        # Discard the mixed render from this launch pass and show a clean
        # page with the updated run state from the registry.
        st.rerun()

with col_status:
    st.header("3. Results")
    results_run_id = st.session_state.get("last_completed_run_id") or resume_run_id

    if results_run_id:
        _rdir = results_dir(results_run_id)
        parquet_files = sorted(_rdir.rglob("*.parquet")) if _rdir.exists() else []
        csv_files = sorted(_rdir.rglob("*.csv")) if _rdir.exists() else []
        st.caption(f"RUN ID: {results_run_id}")
    else:
        parquet_files = []
        csv_files = []

    if parquet_files or csv_files:

        if parquet_files:
            st.caption("GeoParquet outputs (recommended)")
        for file_path in parquet_files:
            rel = file_path.relative_to(RUNS_DIR)
            st.caption(file_path.name)
            dl_col, csv_col = st.columns([1, 1])
            with dl_col:
                st.download_button(
                    label="📥 GeoParquet",
                    data=file_path.read_bytes(),
                    file_name=file_path.name,
                    mime="application/octet-stream",
                    key=f"dl_parquet_{str(rel).replace('/', '_')}"
                )
            with csv_col:
                st.download_button(
                    label="📄 CSV",
                    data=parquet_to_csv_bytes(file_path),
                    file_name=file_path.stem + ".csv",
                    mime="text/csv",
                    key=f"dl_csv_{str(rel).replace('/', '_')}"
                )

        if csv_files:
            st.caption("Legacy CSV outputs")
        for file_path in csv_files:
            rel = file_path.relative_to(RUNS_DIR)
            st.download_button(
                label=f"📥 Download {rel}",
                data=file_path.read_bytes(),
                file_name=file_path.name,
                mime="text/csv",
                key=f"dl_{str(rel).replace('/', '_')}"
            )
    else:
        st.info("Ready for input.")

    # Partial checkout support: merged-only checkout from completed chunks
    if results_run_id:
        st.subheader("4. Partial Checkout (Merged)")
        st.caption("Partial checkout is GeoParquet-first and is built from completed chunk outputs.")

        _merge_pid = st.session_state.partial_merge_pid
        _merge_run = st.session_state.partial_merge_run_id
        _merge_active = (
            _merge_pid is not None
            and _merge_run == results_run_id
            and is_pid_alive(_merge_pid)
        )

        if _merge_active:
            st.info("⏳ Building partial checkout files in background... refreshing automatically.")
            components.html(
                "<script>setTimeout(function(){window.parent.location.reload();}, 3000);</script>",
                height=0, width=0,
            )
        else:
            if _merge_pid and not is_pid_alive(_merge_pid) and _merge_run == results_run_id:
                st.session_state.partial_merge_pid = None
                st.session_state.partial_merge_run_id = None

            if st.button("🔄 Prepare/Refresh Partial Checkout Files", key=f"build_partial_{results_run_id}"):
                _build_script = Path(__file__).parent / "scripts" / "build_partial.py"
                proc = subprocess.Popen(
                    [sys.executable, str(_build_script), results_run_id, str(RUNS_DIR)],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    close_fds=True,
                )
                st.session_state.partial_merge_pid = proc.pid
                st.session_state.partial_merge_run_id = results_run_id
                st.rerun()

        _partial_root = results_dir(results_run_id) / "partial_checkout"
        parquet_checkout_files = sorted(_partial_root.rglob("*.parquet")) if _partial_root.exists() else []
        csv_checkout_files = sorted(_partial_root.rglob("*.csv")) if _partial_root.exists() else []

        if parquet_checkout_files or csv_checkout_files:
            if parquet_checkout_files:
                st.caption("GeoParquet partial files")
            for merged_file in parquet_checkout_files:
                _prel = merged_file.relative_to(_partial_root)
                st.caption(merged_file.name)
                dl_col, csv_col = st.columns([1, 1])
                with dl_col:
                    st.download_button(
                        label="📥 GeoParquet",
                        data=merged_file.read_bytes(),
                        file_name=merged_file.name,
                        mime="application/octet-stream",
                        key=f"dl_partial_pq_{str(_prel).replace('/', '_')}"
                    )
                with csv_col:
                    st.download_button(
                        label="📄 CSV",
                        data=parquet_to_csv_bytes(merged_file),
                        file_name=merged_file.stem + ".csv",
                        mime="text/csv",
                        key=f"dl_partial_csv_ng_{str(_prel).replace('/', '_')}"
                    )

            if csv_checkout_files:
                st.caption("Legacy CSV partial files")
            for merged_file in csv_checkout_files:
                st.download_button(
                    label=f"📥 Partial {merged_file.relative_to(_partial_root)}",
                    data=merged_file.read_bytes(),
                    file_name=merged_file.name,
                    mime="text/csv",
                    key=f"dl_partial_csv_{str(merged_file.relative_to(_partial_root)).replace('/', '_')}"
                )
        else:
            st.info("No merged partial checkout files yet. Click refresh once some chunks are completed.")

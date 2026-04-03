# workflow/state.py
import os
import json
import yaml
import duckdb
from datetime import datetime, timezone


def update_run_state(run_yaml: str, db_path: str, run_id: str, status: str, message: str):
    """
    Update run state in both the per-run YAML file and the central DuckDB events table.

    Args:
        run_yaml:  absolute path to the run's run.yaml file
        db_path:   absolute path to run_state.duckdb
        run_id:    unique identifier for this run
        status:    "completed" or "failed"
        message:   human-readable description of the outcome
    """
    now = datetime.now(timezone.utc).isoformat()

    # Update YAML
    if os.path.exists(run_yaml):
        with open(run_yaml) as f:
            meta = yaml.safe_load(f) or {}
        meta["status"] = status
        meta["updated_at"] = now
        meta["last_finished_at"] = now
        with open(run_yaml, "w") as f:
            yaml.safe_dump(meta, f, sort_keys=False)

    # Update DuckDB
    if os.path.exists(db_path):
        try:
            with duckdb.connect(db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO run_events
                        (event_time, run_id, event_type, status, message, payload_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    [now, run_id, "status_change", status, message, "{}"]
                )
        except Exception:
            pass
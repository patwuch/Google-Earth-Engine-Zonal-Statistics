# workflow/state.py
import os
import duckdb
from datetime import datetime, timezone


def update_run_state(run_yaml: str, db_path: str, run_id: str, status: str, message: str):
    """
    Append a status-change event to the central DuckDB events table.
    run.yaml is intentionally not written here — the backend is the sole
    writer of that file, avoiding concurrent-write corruption.
    """
    now = datetime.now(timezone.utc).isoformat()

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
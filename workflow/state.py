# workflow/state.py
import os
import json
from pathlib import Path
import duckdb
from datetime import datetime, timezone


def write_run_warnings_summary(db_path: str, run_id: str, runs_dir: str):
    """
    Query run_events for empty_chunk and job_shelved entries for this run,
    write a run_summary event to DuckDB, and write a human-readable
    warnings_summary.txt to the run directory.

    Called from Snakefile onsuccess so it covers all retry attempts
    (events are keyed by run_id, not PID).
    """
    if not os.path.exists(db_path):
        return

    now = datetime.now(timezone.utc).isoformat()

    try:
        with duckdb.connect(db_path) as conn:
            rows = conn.execute(
                """SELECT event_type, message, payload_json
                   FROM run_events
                   WHERE run_id = ?
                     AND event_type IN ('empty_chunk', 'job_shelved')
                   ORDER BY event_time""",
                [run_id],
            ).fetchall()
    except Exception:
        return

    if not rows:
        # Nothing to report — write a clean summary event and skip the txt.
        try:
            with duckdb.connect(db_path) as conn:
                conn.execute(
                    """INSERT INTO run_events
                           (event_time, run_id, event_type, status, message, payload_json)
                       VALUES (?, ?, 'run_summary', 'completed', ?, '{}')""",
                    [now, run_id, "Run completed with no empty or shelved chunks"],
                )
        except Exception:
            pass
        return

    empty   = [(msg, payload) for etype, msg, payload in rows if etype == "empty_chunk"]
    shelved = [(msg, payload) for etype, msg, payload in rows if etype == "job_shelved"]

    summary_lines = [
        f"Run '{run_id}' warnings summary",
        f"Generated: {now}",
        "",
    ]

    if empty:
        summary_lines.append(f"Empty chunks ({len(empty)}) — no GEE images found for these periods:")
        for msg, payload_json in empty:
            try:
                p = json.loads(payload_json or "{}")
                summary_lines.append(f"  • {p.get('prod','?')}/{p.get('band','?')} [{p.get('chunk','?')}]  ({p.get('collection','?')})")
            except Exception:
                summary_lines.append(f"  • {msg}")
        summary_lines.append("")

    if shelved:
        summary_lines.append(f"Shelved chunks ({len(shelved)}) — timed out repeatedly, written empty to unblock pipeline:")
        for msg, payload_json in shelved:
            try:
                p = json.loads(payload_json or "{}")
                summary_lines.append(f"  • {p.get('prod','?')}/{p.get('band','?')} [{p.get('chunk','?')}]")
            except Exception:
                summary_lines.append(f"  • {msg}")
        summary_lines.append("")

    summary_lines.append(
        "These chunks were written as empty (all-null values) so the rest of the "
        "pipeline could complete. Output parquets will have null rows for these periods."
    )

    summary_text = "\n".join(summary_lines)

    # Write txt file to the run directory.
    try:
        txt_path = Path(runs_dir) / run_id / "warnings_summary.txt"
        txt_path.write_text(summary_text, encoding="utf-8")
    except Exception:
        pass

    # Write a compact run_summary event to DuckDB.
    short_msg = (
        f"Run completed with {len(empty)} empty chunk(s) and {len(shelved)} shelved chunk(s) — "
        f"see warnings_summary.txt"
    )
    try:
        with duckdb.connect(db_path) as conn:
            conn.execute(
                """INSERT INTO run_events
                       (event_time, run_id, event_type, status, message, payload_json)
                   VALUES (?, ?, 'run_summary', 'completed', ?, ?)""",
                [now, run_id, short_msg,
                 json.dumps({"empty": len(empty), "shelved": len(shelved)})],
            )
    except Exception:
        pass


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
#!/usr/bin/env python3
"""
CLI script to build partial checkout files for a given run ID.
Called by main.py via subprocess.Popen so it runs out-of-process
and does not block the Streamlit UI thread.

Usage:
    python scripts/build_partial.py <run_id> <runs_dir>
"""
import sys
import re
import json
import duckdb
from datetime import datetime, timezone
from pathlib import Path


def _log_event(runs_dir: Path, run_id: str, message: str):
    db_path = runs_dir / "run_state.duckdb"
    if not db_path.exists():
        return
    try:
        now = datetime.now(timezone.utc).isoformat()
        with duckdb.connect(str(db_path)) as conn:
            conn.execute(
                """INSERT INTO run_events (event_time, run_id, event_type, status, message, payload_json)
                   VALUES (?, ?, 'info', 'info', ?, ?)""",
                [now, run_id, message, json.dumps({})],
            )
    except Exception:
        pass


def sql_quote_ident(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def merge_parquet_chunks_to_output(chunk_files, output_file: Path):
    """Merge chunk parquet files into one parquet file using UNION ALL with NULL-padding."""
    if not chunk_files:
        return False

    conn = duckdb.connect(":memory:")
    try:
        try:
            conn.execute("LOAD spatial;")
        except Exception:
            pass

        # Load all chunks and collect their column sets.
        chunk_cols_list = []
        for idx, chunk_path in enumerate(chunk_files):
            conn.execute(f"CREATE TABLE chunk_{idx} AS SELECT * FROM read_parquet(?)", [str(chunk_path)])
            cols = [row[1] for row in conn.execute(f"PRAGMA table_info('chunk_{idx}')").fetchall()]
            chunk_cols_list.append(cols)

        # Build the ordered union of all column names (preserve first-seen order).
        seen: set = set()
        all_col_names: list = []
        for cols in chunk_cols_list:
            for c in cols:
                if c not in seen:
                    all_col_names.append(c)
                    seen.add(c)

        # UNION ALL with NULL-fill so no band's data is lost even when schemas differ.
        union_parts = []
        for idx, cols in enumerate(chunk_cols_list):
            col_set = set(cols)
            exprs = [
                sql_quote_ident(c) if c in col_set else f"NULL AS {sql_quote_ident(c)}"
                for c in all_col_names
            ]
            union_parts.append(f"SELECT {', '.join(exprs)} FROM chunk_{idx}")

        conn.execute(f"CREATE TABLE merged AS {' UNION ALL '.join(union_parts)}")

        sort_cols = []
        merged_cols = [row[1] for row in conn.execute("PRAGMA table_info('merged')").fetchall()]
        if "Date" in merged_cols:
            sort_cols.append(sql_quote_ident("Date"))
        if "region_id" in merged_cols:
            sort_cols.append(sql_quote_ident("region_id"))

        if sort_cols:
            conn.execute(f"CREATE TABLE sorted AS SELECT * FROM merged ORDER BY {', '.join(sort_cols)}")
            table_to_export = "sorted"
        else:
            table_to_export = "merged"

        output_file.parent.mkdir(parents=True, exist_ok=True)
        conn.execute(
            f"COPY {table_to_export} TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
            [str(output_file)]
        )
        return output_file.exists()
    finally:
        conn.close()


def build_partial_checkout_files_parquet(run_id: str, runs_dir: Path):
    """Build merged partial checkout GeoParquet files from completed parquet chunks."""
    intermediate = runs_dir / run_id / "intermediate"
    results = runs_dir / run_id / "results"
    run_chunk_root = intermediate / "chunks"
    partial_root = results / "partial_checkout"

    if not run_chunk_root.exists():
        return []

    merged_files = []
    for product_dir in sorted([item for item in run_chunk_root.iterdir() if item.is_dir()]):
        band_chunk_files = []
        discovered_chunks = []

        for chunk_file in sorted(product_dir.glob("*.parquet")):
            match = re.match(
                r"^(?P<band>.+?)_(?P<chunk>\d{4}-\d{2}_\d{4}-\d{2}|\d{4}-\d{2}|\d{4})\.parquet$",
                chunk_file.name,
            )
            if not match:
                continue
            discovered_chunks.append(match.group("chunk"))
            band_chunk_files.append(chunk_file)

        if not band_chunk_files or not discovered_chunks:
            continue

        unique_chunks = sorted(set(discovered_chunks))
        output_dir = partial_root / product_dir.name
        output_file = output_dir / (
            f"{product_dir.name}_partial_{unique_chunks[0]}_to_{unique_chunks[-1]}.parquet"
        )

        latest_chunk_mtime = max(chunk.stat().st_mtime for chunk in band_chunk_files)
        if output_file.exists() and output_file.stat().st_mtime >= latest_chunk_mtime:
            merged_files.append(output_file)
            continue

        output_dir.mkdir(parents=True, exist_ok=True)
        if merge_parquet_chunks_to_output(band_chunk_files, output_file):
            merged_files.append(output_file)
            _log_event(runs_dir, run_id, f"Build partial output to: {output_file.name}")

    return sorted(merged_files)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: build_partial.py <run_id> <runs_dir>", file=sys.stderr)
        sys.exit(1)

    run_id = sys.argv[1]
    runs_dir = Path(sys.argv[2])

    results = build_partial_checkout_files_parquet(run_id, runs_dir)
    print(f"Built {len(results)} partial checkout file(s).")
    for f in results:
        print(f"  {f}")

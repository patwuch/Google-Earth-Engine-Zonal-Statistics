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
import tempfile
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


def _sql_path(p) -> str:
    """Return a forward-slash path string safe for embedding in DuckDB SQL literals."""
    return str(p).replace("\\", "/")


def merge_parquet_chunks_to_output(chunk_files, output_file: Path):
    """Merge chunk parquet files into one parquet file.

    Uses lazy read_parquet() references so no chunk data is loaded into memory
    upfront.  DuckDB streams the union_by_name scan through the GROUP BY pivot
    and writes directly to the output file in a single pass.
    """
    if not chunk_files:
        return False

    conn = duckdb.connect(":memory:")
    try:
        try:
            conn.execute("SET memory_limit='75%'")
        except Exception:
            try:
                import psutil
                _mem_gb = max(1, int(psutil.virtual_memory().total * 0.75 / 1024 ** 3))
            except Exception:
                _mem_gb = 4
            conn.execute(f"SET memory_limit='{_mem_gb}GB'")
        conn.execute(f"SET temp_directory='{_sql_path(tempfile.gettempdir())}'")
        try:
            conn.execute("LOAD spatial;")
        except Exception:
            try:
                conn.execute("INSTALL spatial;")
                conn.execute("LOAD spatial;")
            except Exception:
                pass

        # Lazy reference — DuckDB streams on demand; nothing is loaded yet.
        # union_by_name=true handles schema differences between chunks (NULL-fills
        # missing columns) without requiring manual NULL-padding.
        file_list_sql = "[" + ", ".join(f"'{_sql_path(f)}'" for f in chunk_files) + "]"
        src = f"read_parquet({file_list_sql}, union_by_name=true)"

        # Schema from file metadata only — no row data loaded.
        all_col_info = conn.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()
        all_col_names = [row[0] for row in all_col_info]
        col_types = {row[0]: row[1].upper() for row in all_col_info}

        join_keys = [k for k in ("region_id", "Date") if k in all_col_names]
        sort_cols = [k for k in ("Date", "region_id") if k in all_col_names]

        if join_keys:
            # GROUP BY (region_id, Date): MAX() coalesces NULLs from the union into
            # a single wide row per region/date; ANY_VALUE() picks the first geometry.
            select_parts = []
            for c in all_col_names:
                q = sql_quote_ident(c)
                if c in join_keys:
                    select_parts.append(q)
                elif "GEOMETRY" in col_types.get(c, ""):
                    select_parts.append(f"ANY_VALUE({q}) AS {q}")
                else:
                    select_parts.append(f"MAX({q}) AS {q}")
            group_clause = ", ".join(sql_quote_ident(k) for k in join_keys)
            query = f"SELECT {', '.join(select_parts)} FROM {src} GROUP BY {group_clause}"
        else:
            query = f"SELECT * FROM {src}"

        if sort_cols:
            sort_clause = ", ".join(sql_quote_ident(c) for c in sort_cols)
            query += f" ORDER BY {sort_clause}"

        output_file.parent.mkdir(parents=True, exist_ok=True)
        conn.execute(
            f"COPY ({query}) TO ? (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)",
            [str(output_file)],
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

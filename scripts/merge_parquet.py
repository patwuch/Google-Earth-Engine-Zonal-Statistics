"""
Merge multiple GeoParquet chunks into a single file.
Uses DuckDB for efficient joining with spatial data preservation.
"""
import duckdb
import os
import sys
from pathlib import Path
from datetime import datetime

def log_progress(message, log_file=None, quiet=False):
    """Write progress to log file if provided"""
    timestamp = datetime.now().isoformat()
    if not quiet:
        print(f"[{timestamp}] {message}")
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] {message}\n")

def merge_parquet_chunks(chunk_files, output_path, merge_strategy="wide", log_file=None, quiet=False):
    # ... (setup unchanged) ...

    def _log(message):
        log_progress(message, log_file, quiet)

    import tempfile
    fd, tmp_db = tempfile.mkstemp(suffix=".duckdb")
    os.close(fd)
    os.unlink(tmp_db)
    conn = duckdb.connect(tmp_db, config={"storage_compatibility_version": "v1.5.0"})
    try:
        conn.execute("INSTALL spatial;")
        conn.execute("LOAD spatial;")

        total = len(chunk_files)
        _log(f"Loading {total} chunk(s)...")

        # Get schema from first file without loading data into memory
        first_cols = [
            row[0] for row in
            conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{chunk_files[0]}')").fetchall()
        ]

        file_list_sql = "[" + ", ".join(f"'{f}'" for f in chunk_files) + "]"

        if merge_strategy == "wide":
            _log("Performing WIDE merge (bands as columns)")

            # Read schemas without loading data
            all_schemas = {}
            report_at = {max(1, total // 4), max(1, total // 2), max(1, 3 * total // 4), total}
            for idx, chunk_file in enumerate(chunk_files):
                cols = [
                    row[0] for row in
                    conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{chunk_file}')").fetchall()
                ]
                all_schemas[idx] = cols
                loaded = idx + 1
                if loaded in report_at:
                    _log(f"Loaded {loaded}/{total} chunks ({loaded * 100 // total}%)")

            join_keys = [k for k in ['region_id', 'Date'] if k in first_cols]
            if not join_keys:
                _log("WARNING: No join keys found, falling back to LONG merge.")
                common_cols = set.intersection(*[set(c) for c in all_schemas.values()])
                col_list = ", ".join(sorted(common_cols))
                merged_query = (
                    f"SELECT {col_list} FROM read_parquet({file_list_sql}, union_by_name=true)"
                )
            else:
                accumulated_cols = set(first_cols)
                join_clauses = []
                select_extras = []

                for idx in range(1, len(chunk_files)):
                    chunk_cols = all_schemas[idx]
                    new_cols = [c for c in chunk_cols if c not in accumulated_cols and c not in join_keys]
                    if not new_cols:
                        continue
                    join_on = " AND ".join([f"c0.{k} = c{idx}.{k}" for k in join_keys])
                    join_clauses.append(
                        f"LEFT JOIN read_parquet('{chunk_files[idx]}') c{idx} ON {join_on}"
                    )
                    select_extras.extend([f"c{idx}.{c}" for c in new_cols])
                    accumulated_cols.update(new_cols)

                base_cols = ", ".join([f"c0.{c}" for c in first_cols])
                extra_cols = (", " + ", ".join(select_extras)) if select_extras else ""
                merged_query = (
                    f"SELECT {base_cols}{extra_cols} "
                    f"FROM read_parquet('{chunk_files[0]}') c0 "
                    + " ".join(join_clauses)
                )

        else:
            _log("Performing LONG merge (stacking rows)")
            # Use read_parquet with file list — DuckDB streams files without loading all into memory.
            _log(f"Loaded {total}/{total} chunks (100%)")
            merged_query = f"SELECT * FROM read_parquet({file_list_sql}, union_by_name=true)"

        # Execute merge
        _log("Executing merge query...")
        conn.execute(f"CREATE TABLE merged AS {merged_query}")

        # Get result stats
        row_count = conn.execute("SELECT COUNT(*) FROM merged").fetchone()[0]
        _log(f"Merged table has {row_count} rows")

        # Sort by Date and region_id if available
        sort_cols = []
        merged_cols = [col[1] for col in conn.execute("PRAGMA table_info(merged)").fetchall()]
        if 'Date' in merged_cols:
            sort_cols.append('Date')
        if 'region_id' in merged_cols:
            sort_cols.append('region_id')

        if sort_cols:
            sort_clause = ", ".join(sort_cols)
            _log(f"Sorting by: {sort_clause}")
            conn.execute(f"CREATE TABLE sorted AS SELECT * FROM merged ORDER BY {sort_clause}")
            table_to_export = "sorted"
        else:
            table_to_export = "merged"

        # Write to Parquet
        _log(f"Writing final GeoParquet: {output_path}")
        conn.execute(f"""
            COPY {table_to_export}
            TO '{output_path}'
            (FORMAT PARQUET, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 100000)
        """)

        # Verify and report
        if not os.path.exists(output_path):
            raise RuntimeError(f"Merged parquet file not created: {output_path}")

        file_size_mb = os.path.getsize(output_path) / (1024*1024)
        _log(f"✓ Merge successful: {output_path} ({file_size_mb:.2f} MB, {row_count} rows)")

        return True

    except Exception as e:
        _log(f"ERROR during merge: {str(e)}")
        raise
    finally:
        conn.close()
        try:
            os.remove(tmp_db)
        except OSError:
            pass

if __name__ == "__main__":
    # Support both script and Snakemake usage
    try:
        # Snakemake mode
        chunk_files = snakemake.input.chunks
        output_path = snakemake.output.merged
        log_file = snakemake.log[0] if snakemake.log else None
        merge_strategy = snakemake.params.get("merge_strategy", "wide")
        quiet = True
    except NameError:
        # CLI mode
        if len(sys.argv) < 3:
            print("Usage: python merge_parquet.py <output.parquet> <chunk1.parquet> <chunk2.parquet> ...")
            sys.exit(1)
        output_path = sys.argv[1]
        chunk_files = sys.argv[2:]
        log_file = None
        merge_strategy = "wide"
        quiet = False

    try:
        merge_parquet_chunks(chunk_files, output_path, merge_strategy, log_file, quiet)
        sys.exit(0)
    except Exception as e:
        print(f"FATAL ERROR: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)

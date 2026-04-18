"""
Merge multiple GeoParquet chunks into a single file.
Uses DuckDB for efficient joining with spatial data preservation.
"""
import duckdb
import os
import sys
import tempfile
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

def merge_parquet_chunks(chunk_files, output_path, merge_strategy="wide", band=None, log_file=None, quiet=False, threads=None):
    def _log(message):
        log_progress(message, log_file, quiet)

    # Columns that identify a row but are not band-specific statistics.
    # These are kept as-is when renaming stat columns with a band prefix.
    NON_STAT_COLS = {'region_id', 'Date', 'OGC_FID', 'id', 'admin', 'name'}

    conn = duckdb.connect(":memory:")
    try:
        try:
            conn.execute("SET memory_limit='75%'")
        except Exception:
            # Percentage notation requires DuckDB ≥ 0.10; compute 75% of total RAM directly.
            try:
                import psutil
                _mem_gb = max(1, int(psutil.virtual_memory().total * 0.75 / 1024 ** 3))
            except Exception:
                _mem_gb = 4
            conn.execute(f"SET memory_limit='{_mem_gb}GB'")
        conn.execute(f"SET temp_directory='{tempfile.gettempdir()}'")
        if threads is not None:
            conn.execute(f"SET threads={int(threads)}")
        try:
            conn.execute("LOAD spatial;")
        except Exception:
            conn.execute("INSTALL spatial;")
            conn.execute("LOAD spatial;")

        total = len(chunk_files)
        _log(f"Merging {total} chunk(s) ({merge_strategy} strategy)...")

        file_list_sql = "[" + ", ".join(f"'{f}'" for f in chunk_files) + "]"
        src = f"read_parquet({file_list_sql}, union_by_name=true)"

        # Schema from file metadata only — no data loaded
        all_col_info = conn.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()
        all_col_names = [row[0] for row in all_col_info]

        sort_cols = [k for k in ['Date', 'region_id'] if k in all_col_names]

        if merge_strategy == "wide":
            _log("Performing WIDE merge (bands as columns)")
            join_keys = [k for k in ['region_id', 'Date'] if k in all_col_names]
            if not join_keys:
                _log("WARNING: No join keys found, falling back to LONG merge.")
                query = f"SELECT * FROM {src}"
            else:
                # Pivot: GROUP BY join keys, MAX() for value cols, ANY_VALUE() for geometry.
                # Per-band stat columns already carry the band prefix (e.g. LC_Type1_histogram)
                # from the preceding long-merge step, so MAX() correctly coalesces NULLs.
                select_parts = []
                for col_name, col_type, *_ in all_col_info:
                    q = f'"{col_name}"'
                    if col_name in join_keys:
                        select_parts.append(q)
                    elif 'GEOMETRY' in col_type.upper():
                        select_parts.append(f'ANY_VALUE({q}) AS {q}')
                    else:
                        select_parts.append(f'MAX({q}) AS {q}')
                group_clause = ', '.join(f'"{k}"' for k in join_keys)
                query = f"SELECT {', '.join(select_parts)} FROM {src} GROUP BY {group_clause}"
        else:
            _log("Performing LONG merge (stacking rows)")
            if band:
                # Prefix stat columns with the band name so that the subsequent wide
                # merge can distinguish LC_Type1_histogram from LC_Type2_histogram, etc.
                _log(f"Renaming stat columns with band prefix '{band}'")
                select_parts = []
                for col_name, col_type, *_ in all_col_info:
                    q = f'"{col_name}"'
                    if col_name in NON_STAT_COLS or 'GEOMETRY' in col_type.upper() or col_name.startswith(f"{band}_"):
                        select_parts.append(q)
                    else:
                        select_parts.append(f'{q} AS "{band}_{col_name}"')
                query = f"SELECT {', '.join(select_parts)} FROM {src}"
            else:
                query = f"SELECT * FROM {src}"

        if sort_cols:
            sort_clause = ", ".join(sort_cols)
            _log(f"Sorting by: {sort_clause}")
            query += f" ORDER BY {sort_clause}"

        _log(f"Writing final GeoParquet: {output_path}")
        conn.execute(f"""
            COPY ({query})
            TO '{output_path}'
            (FORMAT PARQUET, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 100000)
        """)

        if not os.path.exists(output_path):
            raise RuntimeError(f"Merged parquet file not created: {output_path}")

        file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
        row_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path}')").fetchone()[0]
        _log(f"✓ Merge successful: {output_path} ({file_size_mb:.2f} MB, {row_count} rows)")

        return True

    except Exception as e:
        _log(f"ERROR during merge: {str(e)}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    # Support both script and Snakemake usage
    try:
        # Snakemake mode
        chunk_files = snakemake.input.chunks
        output_path = snakemake.output[0]
        log_file = snakemake.log[0] if snakemake.log else None
        merge_strategy = snakemake.params.get("merge_strategy", "wide")
        band = snakemake.params.get("band", None)
        quiet = True
        threads = getattr(snakemake, "threads", None)
    except NameError:
        # CLI mode
        if len(sys.argv) < 3:
            print("Usage: python merge_parquet.py <output.parquet> <chunk1.parquet> <chunk2.parquet> ...")
            sys.exit(1)
        output_path = sys.argv[1]
        chunk_files = sys.argv[2:]
        log_file = None
        merge_strategy = "wide"
        band = None
        quiet = False
        threads = None

    try:
        merge_parquet_chunks(chunk_files, output_path, merge_strategy, band, log_file, quiet, threads)
        sys.exit(0)
    except Exception as e:
        print(f"FATAL ERROR: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)

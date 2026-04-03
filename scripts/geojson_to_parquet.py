"""
Convert GeoJSON to GeoParquet using DuckDB.
Preserves geometry and adds spatial indexing.
"""
import duckdb
import os
import sys
from pathlib import Path
from datetime import datetime

def log_progress(message, log_file=None):
    """Write progress to log file if provided"""
    timestamp = datetime.now().isoformat()
    print(f"[{timestamp}] {message}")
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] {message}\n")

def convert_geojson_to_parquet(geojson_path, parquet_path, log_file=None):
    """
    Convert GeoJSON to GeoParquet using DuckDB.
    
    DuckDB's spatial extension provides:
    - Efficient columnar storage
    - Automatic compression
    - Spatial functions for future queries
    - Schema enforcement
    """
    log_progress(f"Converting {geojson_path} → {parquet_path}", log_file)
    
    if not os.path.exists(geojson_path):
        raise FileNotFoundError(f"Input GeoJSON not found: {geojson_path}")
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
    
    # Connect to DuckDB (in-memory)
    conn = duckdb.connect(':memory:')
    
    try:
        # Install and load spatial extension
        conn.execute("INSTALL spatial;")
        conn.execute("LOAD spatial;")
        log_progress("DuckDB spatial extension loaded", log_file)
        
        # Read GeoJSON
        # st_read automatically handles geometry parsing
        log_progress(f"Reading GeoJSON: {geojson_path}", log_file)
        conn.execute(f"""
            CREATE TABLE geojson_data AS 
            SELECT * FROM st_read('{geojson_path}')
        """)
        
        # Get row count
        row_count = conn.execute("SELECT COUNT(*) FROM geojson_data").fetchone()[0]
        log_progress(f"Loaded {row_count} features", log_file)
        
        if row_count == 0:
            log_progress("WARNING: GeoJSON contains no features", log_file)
        
        # Get column info
        columns = conn.execute("PRAGMA table_info(geojson_data)").fetchall()
        column_names = [col[1] for col in columns]
        log_progress(f"Columns: {', '.join(column_names)}", log_file)

        # Collapse duplicate (region_id, Date) rows that arise when multiple
        # Landsat path/row tiles cover the same AOI region on the same date.
        # AVG on numeric columns gives the correct mean across overlapping scenes;
        # geometry is identical for all rows of the same region so ANY_VALUE is exact.
        # For products with no scene overlap this GROUP BY is a no-op.
        group_keys = [c for c in column_names if c in ('region_id', 'Date')]
        if group_keys:
            select_parts = []
            for col in columns:
                name, dtype = col[1], col[2].upper()
                if name in group_keys:
                    select_parts.append(f'"{name}"')
                elif 'GEOMETRY' in dtype:
                    select_parts.append(f'ANY_VALUE("{name}") AS "{name}"')
                elif any(t in dtype for t in ('FLOAT', 'DOUBLE', 'REAL', 'INT', 'DECIMAL',
                                               'NUMERIC', 'BIGINT', 'SMALLINT', 'TINYINT', 'HUGEINT')):
                    select_parts.append(f'AVG("{name}") AS "{name}"')
                else:
                    select_parts.append(f'ANY_VALUE("{name}") AS "{name}"')
            group_clause = ', '.join(f'"{k}"' for k in group_keys)
            conn.execute(
                f"CREATE TABLE deduped AS SELECT {', '.join(select_parts)} "
                f"FROM geojson_data GROUP BY {group_clause}"
            )
            deduped_count = conn.execute("SELECT COUNT(*) FROM deduped").fetchone()[0]
            if deduped_count < row_count:
                log_progress(
                    f"Deduplicated {row_count} → {deduped_count} rows "
                    f"(same-date scene overlap collapsed)", log_file
                )
            export_table = "deduped"
        else:
            export_table = "geojson_data"

        # Write to Parquet with compression
        log_progress(f"Writing GeoParquet: {parquet_path}", log_file)
        conn.execute(f"""
            COPY {export_table}
            TO '{parquet_path}'
            (FORMAT PARQUET, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 100000)
        """)
        
        # Verify output
        if not os.path.exists(parquet_path):
            raise RuntimeError(f"Parquet file not created: {parquet_path}")
        
        file_size_mb = os.path.getsize(parquet_path) / (1024*1024)
        geojson_size_mb = os.path.getsize(geojson_path) / (1024*1024)
        compression_ratio = (1 - file_size_mb / geojson_size_mb) * 100 if geojson_size_mb > 0 else 0
        
        log_progress(
            f"✓ Conversion successful: {file_size_mb:.2f} MB "
            f"(compressed {compression_ratio:.1f}% from {geojson_size_mb:.2f} MB GeoJSON)",
            log_file
        )
        
        return True
        
    except Exception as e:
        log_progress(f"ERROR: {str(e)}", log_file)
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    # Support both script and Snakemake usage
    try:
        # Snakemake mode
        geojson_in = snakemake.input.geojson
        parquet_out = snakemake.output.parquet
        log_file = snakemake.log[0] if snakemake.log else None
    except NameError:
        # CLI mode
        if len(sys.argv) < 3:
            print("Usage: python geojson_to_parquet.py <input.geojson> <output.parquet> [logfile]")
            sys.exit(1)
        geojson_in = sys.argv[1]
        parquet_out = sys.argv[2]
        log_file = sys.argv[3] if len(sys.argv) > 3 else None
    
    try:
        convert_geojson_to_parquet(geojson_in, parquet_out, log_file)
        sys.exit(0)
    except Exception as e:
        print(f"FATAL ERROR: {str(e)}", file=sys.stderr)
        sys.exit(1)

"""
Snakemake workflow for GEE extraction using GeoJSON → GeoParquet pipeline.

Workflow stages:
1. Extract zonal stats from GEE as GeoJSON (preserves geometry)
2. Convert GeoJSON chunks to GeoParquet using DuckDB
3. Merge GeoParquet chunks into final product files

Cloud-ready: GeoParquet is optimized for object storage (S3, GCS, Azure)
"""
import pandas as pd
import os
import sys
from pathlib import Path

import warnings
warnings.filterwarnings("ignore", category=FutureWarning, module="google")

# Ensure the project root is on sys.path for scripts run by Snakemake
_project_root = str(Path(workflow.basedir))
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)
os.environ.setdefault("PYTHONPATH", _project_root)

from workflow.time_chunks import infer_time_chunks, chunk_start_date, chunk_end_date
from workflow.state import update_run_state


# Configuration
PRODUCTS = config.get("products", {})
SHP = config.get("shp_path", "")
RUN_ID = config.get("run_id", "default")
APP_DIR = config.get("app_dir", "/app")
_gee_slots    = int(config.get("gee_concurrency", 10))
_num_products = max(1, len(PRODUCTS))
# Max gee_weight across selected products — determines true concurrent chunk limit.
# A product with gee_weight=5 can only run floor(10/5)=2 chunks at once, so unlocking
# 10 chunks via the chain window would let Snakemake schedule out-of-order.
_max_weight   = max((p.get("gee_weight", 1) for p in PRODUCTS.values()), default=1)
_max_concurrent = _gee_slots // _max_weight
_dynamic_window = max(1, _max_concurrent // _num_products)
CHAIN_PARALLEL_WINDOW = int(config.get("chain_parallel_window", _dynamic_window))
# Finest sensor resolution among all selected products — used to drive AOI simplification.
# Preprocessing simplifies to max(resolution_tol, budget_tol); workers re-simplify
# further if their product's native scale is coarser than the finest sensor.
_finest_resolution_m = min((p.get("resolution_m", 30) for p in PRODUCTS.values()), default=30)



# Directory structure — absolute paths so the Snakefile works correctly
# regardless of which per-run directory Snakemake uses as its working directory.
GEOJSON_CHUNKS_DIR = f"{APP_DIR}/data/runs/{RUN_ID}/intermediate/geojson"
PARQUET_CHUNKS_DIR = f"{APP_DIR}/data/runs/{RUN_ID}/intermediate/chunks"
RESULTS_DIR        = f"{APP_DIR}/data/runs/{RUN_ID}/results"
LOGS_DIR           = f"{APP_DIR}/data/runs/{RUN_ID}/logs"
PREPPED_AOI        = f"{APP_DIR}/data/runs/{RUN_ID}/intermediate/aoi_prepped.parquet"


def get_previous_chunk_output(wildcards):
    """
    Enforce time-series ordering within each band using a sliding window.
    Chunk N cannot start until chunk N-CHAIN_PARALLEL_WINDOW's parquet exists,
    ensuring partial checkouts always contain a contiguous time series from
    the start rather than scattered chunks.
    Uses ancient() so a re-run of the prev chunk (e.g. after a restart) does
    not cascade mtime invalidations through the rest of the chain.
    """
    chunks = infer_time_chunks(PRODUCTS[wildcards.prod])
    try:
        idx = chunks.index(wildcards.time_chunk)
    except ValueError:
        return []

    if idx < CHAIN_PARALLEL_WINDOW:
        return []

    prev_chunk = chunks[idx - CHAIN_PARALLEL_WINDOW]
    return ancient(f"{PARQUET_CHUNKS_DIR}/{wildcards.prod}/{wildcards.band}_{prev_chunk}.parquet")


def get_final_targets():
    """Define final output files: merged GeoParquet + cleanup marker per product."""
    targets = []
    for prod, settings in PRODUCTS.items():
        start, end = settings["start_date"], settings["end_date"]
        targets.append(f"{RESULTS_DIR}/{prod}/{prod}_{start}_to_{end}.parquet")
        targets.append(f"{RESULTS_DIR}/{prod}/.cleanup_done")
    return targets


# ==============================================================================
# Rules
# ==============================================================================

rule all:
    input:
        get_final_targets()

onsuccess:
    from workflow.state import update_run_state
    update_run_state(
        run_yaml = f"{APP_DIR}/data/runs/{RUN_ID}/run.yaml",
        db_path  = f"{APP_DIR}/data/runs/run_state.duckdb",
        run_id   = RUN_ID,
        status   = "completed",
        message  = "Run completed successfully"
    )

onerror:
    from workflow.state import update_run_state
    update_run_state(
        run_yaml = f"{APP_DIR}/data/runs/{RUN_ID}/run.yaml",
        db_path  = f"{APP_DIR}/data/runs/run_state.duckdb",
        run_id   = RUN_ID,
        status   = "failed",
        message  = "Run failed"
    )

rule preprocess_aoi:
    """
    Pre-process the AOI once per run: normalise CRS, assign region_id,
    apply geometry simplification, write to GeoParquet.
    All chunk workers depend on this output instead of the raw shapefile,
    so the coordinate-counting / simplification ladder runs exactly once.
    """
    input:
        shp = SHP
    output:
        aoi = PREPPED_AOI
    params:
        finest_resolution_m = _finest_resolution_m
    log:
        f"{LOGS_DIR}/preprocess_aoi.log"
    script:
        "scripts/preprocess_aoi.py"


rule extract_geojson_chunk:
    """
    Step 1: Extract zonal statistics from GEE as GeoJSON.
    One job = One time chunk + One Product + One Band.
    Preserves geometry for spatial analysis.
    """
    input:
        aoi  = PREPPED_AOI,
        prev = get_previous_chunk_output
    output:
        geojson = f"{GEOJSON_CHUNKS_DIR}/{{prod}}/{{band}}_{{time_chunk}}.geojson"
    resources:
        gee=lambda wildcards: PRODUCTS[wildcards.prod].get("gee_weight", 1)
    retries: 2  # matches GEE_TIMEOUT_MAX_RETRIES-1; on 3rd timeout the worker shelves the chunk
    threads: 1
    wildcard_constraints:
        # Band names are plain word identifiers — no date-like suffixes — so greedy
        # matching cannot consume _YYYY-MM into {band} instead of {time_chunk}.
        band       = r"[A-Za-z][A-Za-z0-9_]*",
        # YYYY (annual) | YYYY-MM_YYYY-MM (3-month batch) | YYYY-MM (legacy single-month)
        time_chunk = r"\d{4}(-\d{2}(_\d{4}-\d{2})?)?"
    params:
        stats = lambda wildcards: PRODUCTS[wildcards.prod]["statistics"],
        ee_collection = lambda wildcards: PRODUCTS[wildcards.prod].get("ee_collection"),
        multi_collections = lambda wildcards: PRODUCTS[wildcards.prod].get("multi_collections"),
        scale = lambda wildcards: PRODUCTS[wildcards.prod]["scale"],
        tile_scale = lambda wildcards: PRODUCTS[wildcards.prod].get("tile_scale", 1),
        cadence = lambda wildcards: PRODUCTS[wildcards.prod].get("cadence", "monthly"),
        categorical = lambda wildcards: PRODUCTS[wildcards.prod].get("categorical", False),
        qa_mask = lambda wildcards: PRODUCTS[wildcards.prod].get("band_masks", {}).get(wildcards.band),
        start_date = lambda wc: chunk_start_date(wc.time_chunk),
        end_date   = lambda wc: chunk_end_date(wc.time_chunk),
        finest_resolution_m = _finest_resolution_m
    log:
        f"{LOGS_DIR}/{{prod}}/{{band}}_{{time_chunk}}_geojson.log"
    script:
        "scripts/worker_geojson.py"

rule convert_to_parquet:
    """
    Step 2: Convert GeoJSON chunk to GeoParquet using DuckDB.
    Applies compression and columnar storage optimization.
    """
    input:
        geojson = f"{GEOJSON_CHUNKS_DIR}/{{prod}}/{{band}}_{{time_chunk}}.geojson"
    output:
        parquet = f"{PARQUET_CHUNKS_DIR}/{{prod}}/{{band}}_{{time_chunk}}.parquet"
    threads: 1
    wildcard_constraints:
        band       = r"[A-Za-z][A-Za-z0-9_]*",
        time_chunk = r"\d{4}(-\d{2}(_\d{4}-\d{2})?)?"
    log:
        f"{LOGS_DIR}/{{prod}}/{{band}}_{{time_chunk}}_parquet.log"
    script:
        "scripts/geojson_to_parquet.py"

rule merge_product_parquet:
    """
    Step 3: Merge all GeoParquet chunks for a product into final output.
    Combines all bands and time chunks with geometry preserved.
    """
    input:
        chunks = lambda wildcards: [
            f"{PARQUET_CHUNKS_DIR}/{wildcards.prod}/{band}_{chunk}.parquet"
            for band in PRODUCTS[wildcards.prod]["bands"]
            for chunk in infer_time_chunks(PRODUCTS[wildcards.prod])
        ]
    output:
        merged = f"{RESULTS_DIR}/{{prod}}/{{prod}}_{{start}}_to_{{end}}.parquet"
    params:
        merge_strategy = "long"
    threads: 2
    log:
        f"{LOGS_DIR}/{{prod}}/merge_{{start}}_to_{{end}}.log"
    script:
        "scripts/merge_parquet.py"

rule cleanup_intermediate:
    """
    Remove all intermediate GeoJSON and parquet chunk files for a product once
    the final merged GeoParquet has been written successfully.
    Runs automatically after merge_product_parquet completes.
    """
    input:
        merged = lambda wc: (
            f"{RESULTS_DIR}/{wc.prod}/"
            f"{wc.prod}_{PRODUCTS[wc.prod]['start_date']}_to_{PRODUCTS[wc.prod]['end_date']}.parquet"
        )
    output:
        marker = f"{RESULTS_DIR}/{{prod}}/.cleanup_done"
    run:
        import shutil, pathlib
        for d in [GEOJSON_CHUNKS_DIR + "/" + wildcards.prod,
                  PARQUET_CHUNKS_DIR + "/" + wildcards.prod]:
            if pathlib.Path(d).exists():
                shutil.rmtree(d)
        pathlib.Path(output.marker).touch()

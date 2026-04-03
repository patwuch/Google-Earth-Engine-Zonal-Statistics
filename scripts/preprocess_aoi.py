"""
Pre-process the AOI shapefile once per run.
Normalises CRS, assigns region_id, applies geometry simplification to stay
within the GEE payload budget, and writes to GeoParquet.

Each chunk worker loads this file instead of the raw shapefile, so the
coordinate-counting and simplification ladder runs exactly once per run.
"""
import os
from pathlib import Path
from datetime import datetime

import geopandas as gpd

try:
    LOG_FILE = snakemake.log[0] if snakemake.log else "preprocess_aoi.log"
except NameError:
    LOG_FILE = "preprocess_aoi.log"


def log_progress(message):
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True) if os.path.dirname(LOG_FILE) else None
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().isoformat()}] {message}\n")
        f.flush()


def _count_coords(geom):
    if geom is None or geom.is_empty:
        return 0
    if hasattr(geom, 'geoms'):
        return sum(_count_coords(g) for g in geom.geoms)
    if hasattr(geom, 'exterior'):
        return len(geom.exterior.coords) + sum(len(r.coords) for r in geom.interiors)
    if hasattr(geom, 'coords'):
        return len(geom.coords)
    return 0


_COORD_BUDGET = 200_000
_SIMPLIFY_LADDER = [0.001, 0.003, 0.01, 0.02, 0.05]


shp_path            = snakemake.input.shp
out_path            = snakemake.output.aoi
finest_resolution_m = snakemake.params.finest_resolution_m

log_progress(f"Loading AOI from {shp_path}")
input_path = Path(shp_path)
if input_path.suffix.lower() in {".parquet", ".geoparquet"}:
    gdf = gpd.read_parquet(input_path)
else:
    gdf = gpd.read_file(shp_path)
log_progress(f"Loaded {len(gdf)} features")

# Normalise CRS to EPSG:4326
if gdf.crs is None:
    gdf = gdf.set_crs("EPSG:4326")
else:
    gdf = gdf.to_crs("EPSG:4326")

# Assign and deduplicate region_id
if 'region_id' not in gdf.columns:
    id_candidates = ['ADMIN', 'NAME', 'ISO_A3', 'NAME_LONG', 'id', 'fid']
    region_col = next((c for c in id_candidates if c in gdf.columns), None)
    gdf['region_id'] = gdf[region_col].astype(str) if region_col else gdf.index.astype(str)

if gdf['region_id'].duplicated().any():
    counts = {}
    new_ids = []
    for rid in gdf['region_id']:
        if rid in counts:
            counts[rid] += 1
            new_ids.append(f"{rid}_{counts[rid]}")
        else:
            counts[rid] = 0
            new_ids.append(rid)
    gdf['region_id'] = new_ids

# Simplification uses two tolerances; the stricter (larger) one is applied:
#   resolution_tol — no point keeping geometry finer than the finest sensor's pixel size.
#   budget_tol     — minimum tolerance required to stay under the GEE coord budget.
# 1° ≈ 111 000 m, so tolerance = scale_m / 111 000 converts metres to degrees.
resolution_tol = finest_resolution_m / 111_000
log_progress(f"Resolution tolerance: {resolution_tol:.6f}° ({finest_resolution_m}m finest sensor)")

total = sum(_count_coords(g) for g in gdf.geometry)
log_progress(f"Geometry complexity: {total:,} total coordinates (budget: {_COORD_BUDGET:,})")

budget_tol = 0.0
if total > _COORD_BUDGET:
    log_progress("Coord count exceeds budget — finding minimum budget tolerance")
    for tol in _SIMPLIFY_LADDER:
        candidate = gdf.copy()
        candidate["geometry"] = candidate.geometry.simplify(tol, preserve_topology=True)
        candidate = candidate[~candidate.geometry.is_empty & candidate.geometry.notna()]
        reduced = sum(_count_coords(g) for g in candidate.geometry)
        log_progress(f"  tolerance={tol}: {reduced:,} coords")
        if reduced <= _COORD_BUDGET:
            budget_tol = tol
            break
    else:
        budget_tol = _SIMPLIFY_LADDER[-1]
        log_progress(f"WARNING: Still above budget after maximum tolerance={budget_tol}")
else:
    log_progress("Geometry within budget")

applied_tol = max(resolution_tol, budget_tol)
log_progress(
    f"Applying tolerance={applied_tol:.6f}° "
    f"(resolution_tol={resolution_tol:.6f}, budget_tol={budget_tol:.6f})"
)
simplified = gdf.copy()
simplified["geometry"] = gdf.geometry.simplify(applied_tol, preserve_topology=True)
simplified = simplified[~simplified.geometry.is_empty & simplified.geometry.notna()]
reduced_total = sum(_count_coords(g) for g in simplified.geometry)
log_progress(
    f"Simplified: {total:,} → {reduced_total:,} coords "
    f"({100 * (1 - reduced_total / total):.0f}% reduction)"
)
gdf = simplified

os.makedirs(os.path.dirname(out_path), exist_ok=True)
gdf.to_parquet(out_path)
log_progress(f"Written prepped AOI: {out_path} ({len(gdf)} features)")

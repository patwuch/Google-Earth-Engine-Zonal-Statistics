"""
Pre-process the AOI shapefile once per run.
Normalises CRS, assigns region_id, applies geometry simplification to stay
within the GEE payload budget, and writes to GeoParquet.

Each chunk worker loads this file instead of the raw shapefile, so the
coordinate-counting and simplification ladder runs exactly once per run.

Simplification is performed in EPSG:6933 (equal-area, metres) so that the
tolerance has a consistent physical meaning regardless of latitude, then
reprojected back to EPSG:4326 for GEE upload.
"""
import os
from pathlib import Path
from datetime import datetime

import geopandas as gpd
from shapely.ops import transform

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

# Simplification ladder in metres — aligns with common sensor resolutions.
# 10m: Sentinel-2, 30m: Landsat, 100m: intermediate, 250/500m: MODIS.
# No point keeping geometry finer than the coarsest pixel that will sample it.
_SIMPLIFY_LADDER_M = [10, 30, 100, 250, 500, 1000, 5566, 11132]

# Equal-area projection for simplification. Tolerances in metres are
# consistent globally up to ~75° latitude. Beyond that, polar projections
# (EPSG:3995 / EPSG:3976) would be more appropriate, but land-surface
# remote sensing workflows rarely reach those latitudes.
_METRIC_CRS = 'EPSG:6933'


shp_path            = snakemake.input.shp
out_path            = snakemake.output.aoi
finest_resolution_m = snakemake.params.finest_resolution_m
id_column           = (getattr(snakemake.params, "id_column", None) or "").strip() or None

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
    if id_column and id_column in gdf.columns:
        region_col = id_column
        log_progress(f"Using user-specified ID column: {region_col!r}")
    else:
        if id_column:
            log_progress(f"WARNING: Specified ID column {id_column!r} not found in file; falling back to auto-detection")
        id_candidates = ['ADMIN', 'NAME', 'ISO_A3', 'NAME_LONG', 'id', 'fid']
        region_col = next((c for c in id_candidates if c in gdf.columns), None)
        if region_col:
            log_progress(f"Auto-detected ID column: {region_col!r}")
        else:
            log_progress("No ID column found; using row index as region_id")
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

# Reproject to equal-area metric CRS for simplification.
# Coord counts are projection-independent so the budget check is valid in
# either CRS — but the tolerance must be in metres to be physically meaningful.
gdf_metric = gdf.to_crs(_METRIC_CRS)

# Simplification uses two tolerances; the larger one is applied:
#   resolution_tol_m — no point keeping geometry finer than the finest sensor pixel.
#   budget_tol_m     — minimum tolerance needed to stay under the GEE coord budget.
resolution_tol_m = finest_resolution_m
log_progress(f"Resolution tolerance: {resolution_tol_m}m (finest sensor)")

total = sum(_count_coords(g) for g in gdf_metric.geometry)
log_progress(f"Geometry complexity: {total:,} total coordinates (budget: {_COORD_BUDGET:,})")

budget_tol_m = 0.0
if total > _COORD_BUDGET:
    log_progress("Coord count exceeds budget — finding minimum budget tolerance")
    for tol in _SIMPLIFY_LADDER_M:
        if tol < resolution_tol_m:
            continue  # resolution_tol already covers this; max() would discard the result anyway
        candidate = gdf_metric.copy()
        candidate["geometry"] = candidate.geometry.simplify(tol, preserve_topology=True)
        candidate = candidate[~candidate.geometry.is_empty & candidate.geometry.notna()]
        reduced = sum(_count_coords(g) for g in candidate.geometry)
        log_progress(f"  tolerance={tol}m: {reduced:,} coords")
        if reduced <= _COORD_BUDGET:
            budget_tol_m = tol
            break
    else:
        budget_tol_m = _SIMPLIFY_LADDER_M[-1]
        log_progress(f"WARNING: Still above budget after maximum tolerance={budget_tol_m}m")
else:
    log_progress("Geometry within budget")

applied_tol_m = max(resolution_tol_m, budget_tol_m)
log_progress(
    f"Applying tolerance={applied_tol_m}m "
    f"(resolution_tol={resolution_tol_m}m, budget_tol={budget_tol_m}m)"
)

simplified = gdf_metric.copy()
simplified["geometry"] = gdf_metric.geometry.simplify(applied_tol_m, preserve_topology=True)
simplified = simplified[~simplified.geometry.is_empty & simplified.geometry.notna()]
reduced_total = sum(_count_coords(g) for g in simplified.geometry)
log_progress(
    f"Simplified: {total:,} → {reduced_total:,} coords "
    f"({100 * (1 - reduced_total / total):.0f}% reduction)"
)

# Reproject back to 4326 — everything downstream (GEE upload, chunk workers)
# continues to receive WGS84 geometries exactly as before.
gdf = simplified.to_crs("EPSG:4326")

# Repair any self-intersections introduced by simplify()+to_crs().
# buffer(0) is a no-op for valid polygons; for self-intersecting ones it
# resolves crossings so ee.Geometry() accepts them.
gdf["geometry"] = gdf.geometry.buffer(0)
gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notna()]

# Strip Z coordinates — ee.Geometry() only accepts 2D (lon, lat) pairs
if gdf.geometry.has_z.any():
    gdf["geometry"] = gdf.geometry.apply(
        lambda g: transform(lambda x, y, z=None: (x, y), g) if g.has_z else g
    )

os.makedirs(os.path.dirname(out_path), exist_ok=True)
gdf.to_parquet(out_path)
log_progress(f"Written prepped AOI: {out_path} ({len(gdf)} features)")
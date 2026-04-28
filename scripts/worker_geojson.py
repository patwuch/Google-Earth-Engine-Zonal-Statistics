"""
Worker script for GEE extraction using GeoJSON format.
Exports zonal statistics as GeoJSON preserving geometry.
"""
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)



import os
import sys
import json
from pathlib import Path
import re
import uuid
import tempfile
import ee
import geemap
from workflow.gee_ops import (
    apply_qa_mask,
    build_multi_ndbi_collection,
    build_reducer,
    build_compound_reducer,
    build_daily_stats,
    build_seasonal_stats,
    build_annual_stats,
    build_histogram_stats,
)
import geopandas as gpd
import pandas as pd
from datetime import datetime, timedelta, timezone
import threading
import traceback

# Prefer Snakemake job log if configured
try:
    LOG_FILE = snakemake.log[0] if snakemake.log else "worker_debug.log"
except NameError:
    LOG_FILE = "worker_debug.log"

def initialize_earth_engine():
    """Initialize Earth Engine with service account or default credentials"""
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    service_account = os.getenv("EE_SERVICE_ACCOUNT")

    if credentials_path and os.path.exists(credentials_path):
        if not service_account:
            try:
                with open(credentials_path, "r", encoding="utf-8") as fp:
                    key_data = json.load(fp)
                service_account = key_data.get("client_email")
            except Exception:
                service_account = None

        if service_account:
            credentials = ee.ServiceAccountCredentials(service_account, credentials_path)
            ee.Initialize(credentials)
            return

    ee.Initialize()

def _count_coords(geom):
    """Count all coordinates in a geometry, including holes and multi-part components."""
    if geom is None or geom.is_empty:
        return 0
    if hasattr(geom, 'geoms'):  # Multi* or GeometryCollection
        return sum(_count_coords(g) for g in geom.geoms)
    if hasattr(geom, 'exterior'):  # Polygon
        return len(geom.exterior.coords) + sum(len(r.coords) for r in geom.interiors)
    if hasattr(geom, 'coords'):  # LineString, Point
        return len(geom.coords)
    return 0



# Ordered tolerances tried when the preprocessed AOI still exceeds the budget.
_SIMPLIFY_LADDER = [0.001, 0.003, 0.01, 0.02, 0.05]


def _split_attrs(gdf):
    """
    Split a GeoDataFrame into (gdf_slim, attr_lookup).
    gdf_slim: geometry + region_id only (minimal GEE payload).
    attr_lookup: dict mapping region_id -> extra attribute columns (rejoined after extraction).
    """
    geom_col = gdf.geometry.name
    extra_cols = [c for c in gdf.columns if c not in (geom_col, 'region_id')]
    attr_lookup = (
        gdf[['region_id'] + extra_cols].set_index('region_id').to_dict('index')
        if extra_cols else {}
    )
    return gdf[['region_id', geom_col]].copy(), attr_lookup



def _gdf_to_ee(gdf_slim):
    """
    Convert a slim GeoDataFrame (geometry + region_id) to an EE FeatureCollection.
    Uses __geo_interface__ directly — avoids shapefile roundtrip which truncates
    column names to 10 chars and drops unsupported geometry types.
    """
    features = [
        ee.Feature(
            ee.Geometry(row.geometry.__geo_interface__),
            {"region_id": str(row["region_id"])}
        )
        for _, row in gdf_slim.iterrows()
    ]
    return ee.FeatureCollection(features)

def log_progress(message):
    """Write progress message to log file"""
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True) if os.path.dirname(LOG_FILE) else None
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().isoformat()}] {message}\n")
        f.flush()


GEE_TIMEOUT            = 1800  # seconds before a stalled getInfo() is treated as a hung task
GEE_TIMEOUT_MAX_RETRIES = 3    # after this many timeouts the chunk is shelved (empty GeoJSON written)


def _retry_count_path(out_path):
    return out_path + ".retries"


def _get_retry_count(out_path):
    try:
        return int(Path(_retry_count_path(out_path)).read_text().strip())
    except Exception:
        return 0


def _increment_retry_count(out_path):
    count = _get_retry_count(out_path) + 1
    Path(_retry_count_path(out_path)).write_text(str(count))
    return count


def _write_shelved_event(prod: str, band: str, chunk: str, count: int):
    """Write a job_shelved event to run_events so the UI can surface it."""
    run_id  = os.getenv("GEE_RUN_ID")
    db_path = os.getenv("GEE_DB_PATH")
    if not run_id or not db_path:
        return
    try:
        import duckdb
        payload = json.dumps({"prod": prod, "band": band, "chunk": chunk})
        msg = f"Shelved {prod}/{band} [{chunk}] — timed out {count} times, written as empty chunk"
        now = datetime.now(timezone.utc).isoformat()
        with duckdb.connect(db_path) as conn:
            conn.execute(
                """INSERT INTO run_events
                       (event_time, run_id, event_type, status, message, payload_json)
                   VALUES (?, ?, 'job_shelved', 'job_shelved', ?, ?)""",
                [now, run_id, msg, payload],
            )
    except Exception:
        pass


def _blocking_getinfo(ee_obj, interval=30, label=None):
    """
    Call ee_obj.getInfo() while emitting a heartbeat log every `interval` seconds.
    Raises TimeoutError after GEE_TIMEOUT seconds so Snakemake can reschedule.
    `label` is included in heartbeat messages to clarify which operation is running.
    """
    result_box = [None]
    exc_box    = [None]

    def _run():
        try:
            result_box[0] = ee_obj.getInfo()
        except Exception as e:
            exc_box[0] = e

    prefix = f"[{label}] " if label else ""
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    elapsed = 0
    while t.is_alive():
        t.join(timeout=interval)
        if t.is_alive():
            elapsed += interval
            log_progress(f"{prefix}Still computing on GEE server... ({elapsed}s elapsed)")
            if elapsed >= GEE_TIMEOUT:
                log_progress(
                    f"{prefix}GEE timeout after {elapsed}s — killing job so Snakemake can reschedule"
                )
                raise TimeoutError(
                    f"GEE getInfo() did not return after {elapsed}s"
                )

    if exc_box[0] is not None:
        raise exc_box[0]
    return result_box[0]

def export_to_geojson(image, regions, scale, out_geojson, max_retries=5, prop_rename=None,
                      precomputed_stats=None, categorical=False, attr_lookup=None, extra_props=None,
                      reducer=None):
    """
    Export zonal statistics as GeoJSON with geometry.
    Uses reduceRegions for proper zonal stats computation.
    Pass precomputed_stats to skip the internal reduceRegions call (e.g. for daily per-image mode).
    Pass reducer to override the spatial aggregation reducer (defaults to ee.Reducer.mean()).
    """
    log_progress(f"Exporting to GeoJSON: {out_geojson}")

    if precomputed_stats is not None:
        stats = precomputed_stats
    else:
        # Compute zonal statistics using reduceRegions
        stats = image.reduceRegions(
            collection=regions,
            reducer=reducer if reducer is not None else ee.Reducer.mean(),
            scale=scale
        )
        stats = stats.select(stats.first().propertyNames(), retainGeometry=False)


    
    # Export to GeoJSON with retries
    for attempt in range(max_retries):
        try:
            # Paginate via toList() until an empty page is returned.
            # Avoids calling stats.size().getInfo() which forces GEE to fully evaluate
            # the entire computation graph before the first byte of data arrives.
            PAGE_SIZE = 5000
            features = []
            offset = 0
            page_idx = 0
            while True:
                page_idx += 1
                page = _blocking_getinfo(stats.toList(PAGE_SIZE, offset), label=f"page {page_idx}")
                if not page:
                    break
                features.extend(page)
                log_progress(f"Fetched {len(features)} features so far (page {page_idx})")
                if len(page) < PAGE_SIZE:
                    break
                offset += PAGE_SIZE

            # Rename reducer output properties to expected {band}_{stat} convention.
            # GEE reduceRegions names output properties after the reducer (e.g. 'mean'),
            # not the band name, so rename here before writing.
            if prop_rename:
                for feature in features:
                    props = feature.get("properties", {})
                    for old_key, new_key in prop_rename.items():
                        if old_key in props:
                            props[new_key] = props.pop(old_key)

            # For categorical products, serialize histogram dicts to JSON strings
            # so downstream parquet storage remains flat/tabular.
            # normalize_histogram converts raw pixel counts to proportions (sum → 1),
            # omitting absent classes rather than storing explicit zeros.
            if categorical:
                for feature in features:
                    props = feature.get("properties", {})
                    for key, val in list(props.items()):
                        if isinstance(val, dict):
                            if normalize_histogram and val:
                                total = sum(val.values())
                                if total > 0:
                                    val = {k: v / total for k, v in val.items()}
                            props[key] = json.dumps(val)

            # Rejoin original input attributes using region_id.
            if attr_lookup:
                for feature in features:
                    rid = feature.get("properties", {}).get("region_id")
                    if rid is not None and rid in attr_lookup:
                        props = feature["properties"]
                        for k, v in attr_lookup[rid].items():
                            if k not in props:
                                props[k] = v

            if extra_props:
                for feature in features:
                    feature["properties"].update(extra_props)

            geojson_dict = {"type": "FeatureCollection", "features": features}

            # Write to file
            os.makedirs(os.path.dirname(out_geojson), exist_ok=True)
            with open(out_geojson, 'w') as f:
                json.dump(geojson_dict, f)

            log_progress(f"✓ GeoJSON export successful: {len(features)} features")
            return True

        except Exception as e:
            error_msg = str(e)
            is_rate_limit = (
                "Too many concurrent aggregations" in error_msg
                or "429" in error_msg
            )
            is_retryable = (
                is_rate_limit
                or "Request payload size exceeds" in error_msg
                or "Computation timed out" in error_msg
                or "Collection query aborted" in error_msg
            )
            if is_retryable:
                log_progress(f"✗ Export failed (attempt {attempt+1}/{max_retries}): {error_msg}")
                if attempt < max_retries - 1:
                    if is_rate_limit:
                        import time
                        wait = 60 * (2 ** attempt)
                        log_progress(f"Rate-limited by GEE — waiting {wait}s before retry {attempt+2}/{max_retries}")
                        time.sleep(wait)
                    continue
                return False
            else:
                raise

try:
    log_progress("Starting GeoJSON worker")
    initialize_earth_engine()
    log_progress("Earth Engine initialized")

    # Access snakemake parameters
    col_id            = snakemake.params.ee_collection
    multi_collections = snakemake.params.multi_collections
    scale             = snakemake.params.scale
    tile_scale        = snakemake.params.tile_scale
    stats_list        = snakemake.params.stats
    start             = snakemake.params.start_date
    end               = snakemake.params.end_date
    cadence           = snakemake.params.cadence
    categorical          = snakemake.params.categorical
    normalize_histogram  = snakemake.params.normalize_histogram
    qa_mask           = snakemake.params.qa_mask        # None or QA mask config dict
    band_transform    = snakemake.params.band_transform  # None or {"scale": float, "offset": float}
    band_compute      = snakemake.params.band_compute    # None or {"type": str, "input_bands": [...], ...}
    band       = snakemake.wildcards.band
    prod       = getattr(snakemake.wildcards, "prod", "")
    time_chunk = getattr(snakemake.wildcards, "time_chunk", "")
    aoi        = snakemake.input.aoi
    out        = snakemake.output.geojson
    preprocess_tol_m = snakemake.params.finest_resolution_m

    # Annual only: stamp the chunk start date as Date (one value per region per year).
    # Daily and composite: GEE sets Date per image via build_daily_stats.
    extra_props = {"Date": start} if cadence in ("annual", "seasonal") else None

    log_progress(f"Parameters: collection={col_id}, band={band}, stats={stats_list}, cadence={cadence}, dates={start} to {end}")

    os.makedirs(os.path.dirname(out), exist_ok=True)

    # Load pre-processed AOI (CRS normalised, region_id assigned, geometry already simplified).
    log_progress("Loading pre-processed AOI")
    gdf_full = gpd.read_parquet(aoi)
    gdf_slim, attr_lookup = _split_attrs(gdf_full)
    del gdf_full
    gdf_original = gdf_slim  # keep for empty-feature fallback

    
    
    if scale > preprocess_tol_m:
        gdf_for_gee = gdf_slim.copy()
        gdf_metric = gdf_for_gee.to_crs('EPSG:6933')
        bounds_m = gdf_metric.geometry.union_all().bounds
        min_dim_m = min(bounds_m[2] - bounds_m[0], bounds_m[3] - bounds_m[1])
        # Only re-simplify when the region is at least 3 pixel-widths across.
        # For narrower regions the simplification can distort the polygon enough
        # that no GEE analysis-grid pixel centers fall within it, producing
        # all-null statistics even when valid data exists at the native scale.
        if min_dim_m >= scale * 3:
            gdf_metric["geometry"] = gdf_metric.geometry.simplify(scale, preserve_topology=True)
            gdf_for_gee = gdf_metric.to_crs('EPSG:4326')
            gdf_for_gee = gdf_for_gee[~gdf_for_gee.geometry.is_empty & gdf_for_gee.geometry.notna()]
            log_progress(f"Re-simplified for {scale}m product (min dim {min_dim_m:.0f}m, preprocess was {preprocess_tol_m}m)")
        else:
            gdf_for_gee = gdf_slim
            log_progress(f"Skipped re-simplification: region too small ({min_dim_m:.0f}m) for {scale}m pixel scale")
    else:
        gdf_for_gee = gdf_slim
    regions = _gdf_to_ee(gdf_for_gee)
    log_progress(f"Regions built: {len(gdf_slim)} features")

    # Build a compact filter geometry in Python (no GEE lazy reference).
    # Only used for filterBounds — excludes scenes with no real overlap.
    # Simplified very aggressively (0.5°≈55km)
    # The full regions FeatureCollection is still used for reduceRegions.
    # Use a bounding-box filter region — always valid for ee.Geometry.BBox,
    # immune to degenerate geometries from aggressive simplification, and
    # sufficient for filterBounds (scene selection only, not analysis).
    minx, miny, maxx, maxy = gdf_slim.geometry.unary_union.bounds
    filter_region = ee.Geometry.BBox(minx, miny, maxx, maxy)

    end_dt = (datetime.strptime(end, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    log_progress("Filtering image collection")
    if multi_collections:
        # NDBI: masking is applied per-sensor inside build_multi_ndbi_collection.
        collection = build_multi_ndbi_collection(multi_collections, start, end_dt, region=filter_region)
        if collection is None:
            collection = ee.ImageCollection([])  # No sensors active in this chunk  # type: ignore[assignment]
    else:
        collection = ee.ImageCollection(col_id).filterDate(start, end_dt).filterBounds(filter_region)
        if band_compute:
            _bc_input_bands = band_compute["input_bands"]
            _bc_qa_masks    = band_compute.get("input_qa_masks", [])
            _bc_type        = band_compute.get("type", "mean")
            log_progress(f"Computing derived band '{band}' ({_bc_type} of {_bc_input_bands})")
            def _compute_derived(img, ibs=_bc_input_bands, iqa=_bc_qa_masks, ctype=_bc_type, out=band):
                for qam in iqa:
                    img = apply_qa_mask(img, qam)
                selected = [img.select(b) for b in ibs]
                result = selected[0]
                for sb in selected[1:]:
                    result = result.add(sb)
                if ctype == "mean":
                    result = result.divide(len(ibs))
                return result.rename(out).copyProperties(img, ["system:time_start"])
            collection = collection.map(_compute_derived)
        else:
            if qa_mask is not None:
                log_progress(f"Applying QA bit-mask: band={qa_mask['band']}, tests={qa_mask['tests']}")
                collection = collection.map(lambda img: apply_qa_mask(img, qa_mask))
            collection = collection.select([band])
        if band_transform:
            _bt_scale  = band_transform.get("scale", 1.0)
            _bt_offset = band_transform.get("offset", 0.0)
            def _apply_transform(img, s=_bt_scale, o=_bt_offset):
                transformed = img.multiply(s).add(o) if s != 1.0 else img.add(o)
                return transformed.copyProperties(img, ["system:time_start"])
            collection = collection.map(_apply_transform)
            log_progress(f"Band transform applied: ×{_bt_scale} + {_bt_offset}")

    collection_count = _blocking_getinfo(collection.size(), label="size()")
    log_progress(f"Collection has {collection_count} images")

    if collection_count == 0:
        log_progress(
            f"WARNING: No images found for {col_id}/{band} between {start} and {end}. "
            "Writing empty GeoJSON to unblock pipeline."
        )
        empty_features = []
        for idx, row in gdf_original.iterrows():
            props = {"region_id": row.get('region_id', str(idx)), "Date": start}
            if extra_props:
                props.update(extra_props)
            if categorical:
                props[f"{band}_histogram"] = None
            else:
                for s in stats_list:
                    props[f"{band}_{s.lower()}"] = None
            empty_features.append({
                "type": "Feature",
                "geometry": json.loads(gpd.GeoSeries([row.geometry]).to_json())['features'][0]['geometry'],
                "properties": props
            })
        with open(out, 'w') as f:
            json.dump({"type": "FeatureCollection", "features": empty_features}, f)
        log_progress(f"Wrote empty GeoJSON to {out}")
        sys.exit(0)

    # GEE property naming:
    # - Single stat + single-output reducer → property named after reducer (e.g. 'mean'), not band.
    #   Use prop_rename to correct this.
    # - Multiple stats via compound reducer → GEE outputs '{band}_{stat}' correctly.
    #   No rename needed.
    if len(stats_list) == 1:
        s = stats_list[0]
        prop_rename = {s.lower(): f"{band}_{s.lower()}"}
    else:
        prop_rename = {}

    def _do_export(regions_fc, max_retries):
        if categorical:
            stats_fc = build_histogram_stats(collection, regions_fc, scale, band)
            # GEE names the histogram output property after the band; rename to {band}_histogram
            hist_rename = {band: f"{band}_histogram"}
            return export_to_geojson(
                image=None, regions=regions_fc, scale=scale, out_geojson=out,
                max_retries=max_retries, prop_rename=hist_rename,
                precomputed_stats=stats_fc, categorical=True, attr_lookup=attr_lookup,
                extra_props=extra_props
            )
        elif cadence in ("daily", "composite"):
            # Per-image reduction: one row per region per image date.
            # Composite products (e.g. MODIS 8-day, Landsat 16-day) have their own
            # acquisition date per image, so treat them the same as daily.
            compound = build_compound_reducer(stats_list)
            stats_fc = build_daily_stats(collection, regions_fc, scale, compound, tile_scale)
            return export_to_geojson(
                image=None, regions=regions_fc, scale=scale, out_geojson=out,
                max_retries=max_retries, prop_rename=prop_rename,
                precomputed_stats=stats_fc, attr_lookup=attr_lookup,
                extra_props=extra_props
            )
        elif cadence == "seasonal":
            # One value per region per quarter. Temporal reducer collapses all scenes
            # in the window; spatial reducer is decoupled (mean for sum/std/variance)
            # so results are region-size-invariant.
            stats_fc = build_seasonal_stats(collection, regions_fc, scale, stats_list, band, tile_scale)
        elif cadence == "annual":
            # One value per region per year. Single image per year (e.g. WorldPop) so
            # the same reducer for both passes is correct — sum gives total population, etc.
            stats_fc = build_annual_stats(collection, regions_fc, scale, stats_list, band, tile_scale)
            return export_to_geojson(
                image=None, regions=regions_fc, scale=scale, out_geojson=out,
                max_retries=max_retries, prop_rename=prop_rename,
                precomputed_stats=stats_fc, attr_lookup=attr_lookup,
                extra_props=extra_props
            )
        else:
            raise ValueError(f"Unknown cadence '{cadence}' for product '{prod}'/{band}")

    log_progress(f"Extracting {len(stats_list)} stat(s): {stats_list}")
    success = _do_export(regions, max_retries=3)

    if not success:
        # Geometry was supposed to be already under the coord budget, so failure is likely a transient
        # GEE error or an edge case where our estimate was insufficient.
        # Apply one emergency simplification step at the maximum tolerance and retry once.
        emergency_tol = _SIMPLIFY_LADDER[-1]
        log_progress(f"Export failed — applying emergency simplification (tolerance={emergency_tol})")
        gdf_emergency = gdf_slim.copy()
        gdf_emergency["geometry"] = gdf_slim.geometry.simplify(emergency_tol, preserve_topology=True)
        gdf_emergency = gdf_emergency[~gdf_emergency.geometry.is_empty & gdf_emergency.geometry.notna()]
        regions_emergency = _gdf_to_ee(gdf_emergency)
        success = _do_export(regions_emergency, max_retries=1)

    if not success:
        raise RuntimeError(
            "Failed to export GeoJSON even with geometry simplification. "
            "The AOI may be too complex. Consider uploading a simpler shapefile."
        )

    if not os.path.exists(out):
        raise RuntimeError(f"GeoJSON export completed but file not found: {out}")

    file_size = os.path.getsize(out) / (1024*1024)
    log_progress(f"SUCCESS: GeoJSON written to {out} ({file_size:.2f} MB)")

except Exception as e:
    if isinstance(e, TimeoutError):
        try:
            count = _increment_retry_count(out)
            if count >= GEE_TIMEOUT_MAX_RETRIES:
                log_progress(
                    f"GEE timeout on attempt {count}/{GEE_TIMEOUT_MAX_RETRIES} — "
                    f"chunk is persistently slow, shelving with empty GeoJSON"
                )
                os.makedirs(os.path.dirname(out), exist_ok=True)
                with open(out, "w") as f:
                    json.dump({"type": "FeatureCollection", "features": []}, f)
                _write_shelved_event(prod, band, time_chunk, count)
                sys.exit(0)
            else:
                log_progress(
                    f"GEE timeout on attempt {count}/{GEE_TIMEOUT_MAX_RETRIES} — "
                    f"will retry on next Snakemake pass"
                )
        except Exception:
            pass  # fall through to normal error handling

    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True) if os.path.dirname(LOG_FILE) else None
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"ERROR: {str(e)}\n")
        f.write(traceback.format_exc())
        f.write("\n")
    raise e

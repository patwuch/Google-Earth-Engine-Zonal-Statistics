# workflow/products.py

PRODUCT_REGISTRY: dict[str, dict] = {
    "CHIRPS": {
        "ee_collection": "UCSB-CHG/CHIRPS/DAILY",
        "min_date": "1981-01-01",
        "max_date": "2026-02-28",
        "scale": 5566,
        "cadence": "daily",
        "categorical": False,
        "content": {
            "precipitation": {"stats": ["sum", "mean", "max"], "default_stats": ["sum"]},
        },
        "label": "CHIRPS Daily Precipitation",
        "description": "Global precipitation (0.05° resolution).",
        "resolution_m": 5566,
    },
    "ERA5_LAND": {
        "ee_collection": "ECMWF/ERA5_LAND/DAILY_AGGR",
        "min_date": "1950-01-01",
        "max_date": "2026-02-28",
        "scale": 9000,
        "cadence": "daily",
        "categorical": False,
        "content": {
            "temperature_2m":               {"stats": ["mean", "min", "max"], "default_stats": ["mean"]},
            "temperature_2m_min":           {"stats": ["mean", "min", "max"], "default_stats": ["mean"]},
            "temperature_2m_max":           {"stats": ["mean", "min", "max"], "default_stats": ["mean"]},
            "total_precipitation_sum":      {"stats": ["sum", "mean"], "default_stats": ["sum"]},
            "total_evaporation_sum":        {"stats": ["sum", "mean"], "default_stats": ["sum"]},
            "potential_evaporation_sum":    {"stats": ["sum", "mean"], "default_stats": ["sum"]},
            "snow_evaporation_sum":         {"stats": ["sum", "mean"], "default_stats": ["sum"]},
            "evaporation_from_bare_soil_sum": {"stats": ["sum", "mean"], "default_stats": ["sum"]},
            "evaporation_from_open_water_surfaces_excluding_oceans_sum": {"stats": ["sum", "mean"], "default_stats": ["sum"]},
            "evaporation_from_the_top_of_canopy_sum": {"stats": ["sum", "mean"], "default_stats": ["sum"]},
            "evaporation_from_vegetation_transpiration_sum": {"stats": ["sum", "mean"], "default_stats": ["sum"]},
        },
        "label": "ERA5-Land Climate",
        "description": "Reanalysis data for land variables (9km resolution).",
        "resolution_m": 9000,
    },
    "MODIS_LST": {
        "ee_collection": "MODIS/061/MOD11A2",
        "min_date": "2000-02-18",
        "max_date": "2026-02-10",
        "scale": 1000,
        "cadence": "composite",
        "categorical": False,
        "content": {
            "LST_Day_1km": {
                "stats": ["mean", "median", "max"],
                "default_stats": ["mean"],
                # MOD11A2 QC_Day bits 0–1: 00=good, 01=other quality — keep both.
                "qa_mask": {
                    "band": "QC_Day",
                    "tests": [{"start": 0, "end": 1, "good_values": [0, 1]}],
                },
            },
            "LST_Night_1km": {
                "stats": ["mean", "median", "max"],
                "default_stats": ["mean"],
                # MOD11A2 QC_Night bits 0–1: same encoding as QC_Day.
                "qa_mask": {
                    "band": "QC_Night",
                    "tests": [{"start": 0, "end": 1, "good_values": [0, 1]}],
                },
            },
        },
        "label": "MODIS Land Surface Temperature",
        "description": "Land Surface Temperature (1km resolution).",
        "resolution_m": 1000,
    },
    "MODIS_NDVI_EVI": {
        "ee_collection": "MODIS/061/MOD13Q1",
        "min_date": "2000-02-18",
        "max_date": "2026-02-02",
        "scale": 250,
        "cadence": "composite",
        "categorical": False,
        "content": {
            "NDVI": {
                "stats": ["mean", "median"],
                "default_stats": ["mean"],
                # MOD13Q1 SummaryQA bits 0–1: 00=good, 01=marginal — keep both.
                "qa_mask": {
                    "band": "SummaryQA",
                    "tests": [{"start": 0, "end": 1, "good_values": [0, 1]}],
                },
            },
            "EVI": {
                "stats": ["mean", "median"],
                "default_stats": ["mean"],
                "qa_mask": {
                    "band": "SummaryQA",
                    "tests": [{"start": 0, "end": 1, "good_values": [0, 1]}],
                },
            },
        },
        "label": "MODIS NDVI / EVI",
        "description": "Vegetation Indices (250m resolution).",
        "resolution_m": 250,
    },
    "WorldCover_v100": {
        "ee_collection": "ESA/WorldCover/v100",
        "min_date": "2020-01-01",
        "max_date": "2020-12-31",
        "scale": 10,
        "cadence": "annual",
        "categorical": True,
        "content": {
            "Map": {"stats": ["histogram"], "default_stats": ["histogram"]},
        },
        "label": "ESA WorldCover 2020 (v1.0)",
        "description": "Aggregated to 500m - ESA WorldCover landcover classification v100 (10m, 2020).",
        "resolution_m": 10,
    },
    "WorldCover_v200": {
        "ee_collection": "ESA/WorldCover/v200",
        "min_date": "2021-01-01",
        "max_date": "2021-12-31",
        "scale": 10,
        "cadence": "annual",
        "categorical": True,
        "content": {
            "Map": {"stats": ["histogram"], "default_stats": ["histogram"]},
        },
        "label": "ESA WorldCover 2021 (v2.0)",
        "description": "Aggregated to 500m - ESA WorldCover landcover classification v200 (10m, 2021).",
        "resolution_m": 10,
    },
    "MODIS_LULC": {
        "ee_collection": "MODIS/061/MCD12Q1",
        "min_date": "2001-01-01",
        "max_date": "2023-12-31",
        "scale": 500,
        "cadence": "annual",
        "categorical": True,
        "content": {
            "LC_Type1": {"stats": ["histogram"], "default_stats": ["histogram"]},
            "LC_Type2": {"stats": ["histogram"], "default_stats": ["histogram"]},
            "LC_Type3": {"stats": ["histogram"], "default_stats": ["histogram"]},
            "LC_Type4": {"stats": ["histogram"], "default_stats": ["histogram"]},
            "LC_Type5": {"stats": ["histogram"], "default_stats": ["histogram"]},
        },
        "label": "MODIS Land Use / Land Cover",
        "description": "MODIS Land Cover Type (500m resolution, annual, multiple schemes).",
        "resolution_m": 500,
    },
    "NDBI": {
        # No single ee_collection — worker uses multi_collections to build a merged NDBI series.
        # gee_weight=4: computed from 30m Landsat on-the-fly,  more expensive per job than
        # precomputed products.  With gee_concurrency=10, caps concurrent NDBI jobs to
        # floor(10/5)=2 . If still experiencing computational problems, drop GEE worker max count 
        # to 5 so only one NDBI could ever be active at a time + it blocks any other workers from 
        # getting schedule at the same time.
        "gee_weight": 5,
        "tile_scale": 4,
        "ee_collection": None,
        "multi_collections": [
            {
                # LT05 routine operations suspended 2011-11-18 after a power anomaly;
                # no usable imagery after November 2011.
                "id":         "LANDSAT/LT05/C02/T1_L2",
                "date_start": "1984-01-01",
                "date_end":   "2011-11-30",
                "swir_band":  "SR_B5",
                "nir_band":   "SR_B4",
                # Landsat C02 L2 QA_PIXEL: keep pixels where cloud/shadow/snow bits are clear.
                # bit 1=dilated cloud, bit 3=cloud shadow, bit 4=snow, bit 5=cloud — all must be 0.
                "qa_mask": {
                    "band": "QA_PIXEL",
                    "tests": [
                        {"start": 1, "end": 1, "good_values": [0]},
                        {"start": 3, "end": 3, "good_values": [0]},
                        {"start": 4, "end": 4, "good_values": [0]},
                        {"start": 5, "end": 5, "good_values": [0]},
                    ],
                },
            },
            {
                # LE07 (SLC-off since 2003 but usable) bridges LT05 end to LC08 start.
                "id":         "LANDSAT/LE07/C02/T1_L2",
                "date_start": "2011-12-01",
                "date_end":   "2013-04-10",
                "swir_band":  "SR_B5",
                "nir_band":   "SR_B4",
                "qa_mask": {
                    "band": "QA_PIXEL",
                    "tests": [
                        {"start": 1, "end": 1, "good_values": [0]},
                        {"start": 3, "end": 3, "good_values": [0]},
                        {"start": 4, "end": 4, "good_values": [0]},
                        {"start": 5, "end": 5, "good_values": [0]},
                    ],
                },
            },
            {
                # LC08 first operational data: 2013-04-11.
                "id":         "LANDSAT/LC08/C02/T1_L2",
                "date_start": "2013-04-11",
                "date_end":   "2025-12-31",
                "swir_band":  "SR_B6",
                "nir_band":   "SR_B5",
                "qa_mask": {
                    "band": "QA_PIXEL",
                    "tests": [
                        {"start": 1, "end": 1, "good_values": [0]},
                        {"start": 3, "end": 3, "good_values": [0]},
                        {"start": 4, "end": 4, "good_values": [0]},
                        {"start": 5, "end": 5, "good_values": [0]},
                    ],
                },
            },
        ],
        "min_date": "1984-01-01",
        "max_date": "2025-12-31",
        "scale": 30,
        "cadence": "seasonal",
        "categorical": False,
        "content": {
            "NDBI": {"stats": ["mean", "median"], "default_stats": ["mean"]},
        },
        "label": "NDBI (Landsat 5 / 7 / 8)",
        "description": (
            "Aggregated to 250m - Normalized Difference Built-up Index (30m, per-scene). "
            "Landsat 5 TM 1984–Nov 2011, Landsat 7 ETM+ (SLC-off) Dec 2011–Apr 2013, "
            "Landsat 8 OLI Apr 2013–2025. Formula: (SWIR − NIR) / (SWIR + NIR)."
        ),
        "resolution_m": 30,
    },
}

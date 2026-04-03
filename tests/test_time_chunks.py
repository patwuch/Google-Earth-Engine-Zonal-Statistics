# tests/test_time_chunks.py
from workflow.time_chunks import (
    get_time_chunks, infer_time_chunks,
    chunk_start_date, chunk_end_date
)

def test_annual_chunks():
    result = get_time_chunks("2020-01-01", "2022-12-31", "annual")
    assert result == ["2020", "2021", "2022"]

def test_daily_chunks_by_month():
    result = get_time_chunks("2021-01-01", "2021-03-31", "daily")
    assert result == ["2021-01", "2021-02", "2021-03"]

def test_composite_quarterly():
    result = get_time_chunks("2021-01-01", "2021-09-30", "composite")
    assert result == ["2021-01_2021-03", "2021-04_2021-06", "2021-07_2021-09"]

def test_chunk_start_annual():
    assert chunk_start_date("2021") == "2021-01-01"

def test_chunk_end_annual():
    assert chunk_end_date("2021") == "2021-12-31"

def test_chunk_end_batch():
    assert chunk_end_date("2021-01_2021-03") == "2021-03-31"


# tests/test_products.py
from workflow.products import PRODUCT_REGISTRY

def test_all_products_have_required_keys():
    required = {"scale", "cadence", "categorical", "content", "label"}
    for name, config in PRODUCT_REGISTRY.items():
        missing = required - config.keys()
        assert not missing, f"{name} missing keys: {missing}"

def test_chirps_cadence():
    assert PRODUCT_REGISTRY["CHIRPS"]["cadence"] == "daily"

def test_categorical_products_use_histogram():
    for name, config in PRODUCT_REGISTRY.items():
        if config["categorical"]:
            for band, band_cfg in config["content"].items():
                assert "histogram" in band_cfg["stats"], \
                    f"{name}/{band} is categorical but has no histogram stat"
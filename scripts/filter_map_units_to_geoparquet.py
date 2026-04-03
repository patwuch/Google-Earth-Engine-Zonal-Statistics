import argparse
from pathlib import Path
import geopandas as gpd


def load_vector(path: Path) -> gpd.GeoDataFrame:
    if path.suffix.lower() == ".zip":
        return gpd.read_file(f"zip://{path}")
    return gpd.read_file(path)


def main():
    parser = argparse.ArgumentParser(
        description="Filter Natural Earth admin_0 map units and export to GeoParquet."
    )
    parser.add_argument(
        "--input",
        default="ne_50m_admin_0_map_units.zip",
        help="Path to input vector file (zip shapefile or other GDAL-readable format).",
    )
    parser.add_argument(
        "--output",
        default="data/uploads/ne_50m_admin_0_map_units_filtered.geoparquet",
        help="Path to output GeoParquet file.",
    )
    parser.add_argument(
        "--level",
        type=int,
        default=2,
        help="Natural Earth LEVEL value to keep.",
    )
    parser.add_argument(
        "--types",
        nargs="+",
        default=["Sovereign country"],
        help="Values in TYPE column to keep.",
    )

    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    gdf = load_vector(input_path)

    if "TYPE" not in gdf.columns or "LEVEL" not in gdf.columns:
        raise ValueError("Input does not include expected TYPE and LEVEL fields.")

    filtered = gdf[gdf["TYPE"].isin(args.types) & (gdf["LEVEL"].astype(int) == int(args.level))].copy()
    if "ISO_A3" in filtered.columns:
        filtered = filtered[filtered["ISO_A3"].astype(str).str.fullmatch(r"[A-Z]{3}", na=False)]
    if filtered.empty:
        raise ValueError("Filter produced zero features. Adjust --types/--level.")

    preferred_columns = [
        "ADMIN",
        "NAME",
        "NAME_LONG",
        "SOVEREIGNT",
        "ADM0_A3",
        "ISO_A3",
        "TYPE",
        "LEVEL",
        "CONTINENT",
        "REGION_UN",
        "SUBREGION",
        "POP_EST",
        "GDP_MD",
        "geometry",
    ]
    keep_columns = [col for col in preferred_columns if col in filtered.columns]
    filtered = filtered[keep_columns].sort_values(by=[col for col in ["ADMIN", "NAME"] if col in filtered.columns])

    if filtered.crs is None:
        filtered = filtered.set_crs("EPSG:4326")

    filtered.to_parquet(output_path, index=False)
    print(f"Wrote {len(filtered)} features to {output_path}")


if __name__ == "__main__":
    main()

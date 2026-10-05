"""
One-time precompute: flatten each Historic Conditions year's low-water depth-polygon
shapefile (DepthPolygons/<year>_low_water_polygon/<year>_low_water.shp) into the same
{label, lons, lats}-per-bin JSON shape app.py's LOW_WATER_POLY_BY_YEAR loop builds in
memory at startup (see that loop's comments in app.py). app.py loads this JSON straight
instead of re-running gpd.read_file + the geometry-to-lonlat flatten for every historic
year on every single app startup.

These years are historic and don't change, so this only needs re-running if a year's
low-water shapefile itself is ever regenerated. Mirrors the sidecar
make_combined_depth_polygons.py now writes for the Current Conditions layer -- see that
script's _write_bins_sidecar.

Usage: python update_bathym/build_low_water_polygon_bins_cache.py
"""

import json
from pathlib import Path

import geopandas as gpd
import numpy as np

REPO_ROOT = Path(__file__).resolve().parent.parent
LOW_WATER_POLY_DIR = REPO_ROOT / "DepthPolygons"

# Mirrors app.py's _DISPLAY_BINS order (see that file for the gradient/legend side of
# this same list) and make_combined_depth_polygons.py's DISPLAY_BIN_ORDER.
DISPLAY_BIN_ORDER = {
    "20+ ft": 0, "15-20 ft": 1, "12-15 ft": 2, "9-12 ft": 3, "5-9 ft": 4, "<5 ft": 5,
}


def _geom_to_lonlat(geom):
    """Same vectorized Polygon/MultiPolygon -> flat lon/lat-with-NaN-gaps conversion as
    app.py's _geom_to_lonlat, duplicated here so this standalone script has no
    dependency on app.py itself."""
    polys = list(geom.geoms) if geom.geom_type == "MultiPolygon" else [geom]
    gap = np.array([np.nan])
    lon_chunks, lat_chunks = [], []
    for poly in polys:
        coords = np.asarray(poly.exterior.coords)
        lon_chunks.append(coords[:, 0])
        lon_chunks.append(gap)
        lat_chunks.append(coords[:, 1])
        lat_chunks.append(gap)
    lons = np.round(np.concatenate(lon_chunks), 6).tolist()
    lats = np.round(np.concatenate(lat_chunks), 6).tolist()
    return lons, lats


def build_year(year_dir):
    year = int(year_dir.name.replace("_low_water_polygon", ""))
    shp = year_dir / f"{year}_low_water.shp"
    if not shp.exists():
        print(f"No {shp} -- skipping {year}.")
        return None

    gdf = gpd.read_file(shp).to_crs(4326)
    # the shapefile's string field stores missing values as the literal text "nan"
    # (dbf strings have no real null), not an actual NaN -- .notna() alone won't drop it
    gdf = gdf[gdf["depth_rang"].notna() & (gdf["depth_rang"] != "nan")].copy()
    gdf["depth_bin"] = gdf["depth_rang"] + " ft"
    gdf["bin_order"] = gdf["depth_bin"].map(DISPLAY_BIN_ORDER)
    gdf = gdf.sort_values("bin_order")

    bins = []
    for _, row in gdf.iterrows():
        lons, lats = _geom_to_lonlat(row.geometry)
        bins.append([row["depth_bin"], lons, lats])

    out_path = year_dir / f"{year}_low_water_bins.json"
    with open(out_path, "w") as f:
        json.dump(bins, f)
    print(f"Wrote {out_path} -- {len(bins)} depth-bin polygons for {year}")
    return out_path


if __name__ == "__main__":
    for year_dir in sorted(LOW_WATER_POLY_DIR.glob("*_low_water_polygon")):
        build_year(year_dir)

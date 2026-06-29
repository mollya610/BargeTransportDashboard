"""
Stage 5: generate per-survey depth-bin polygon files.

For each survey gpkg in NAVD88Files/, computes depth under the Low Water Reference
Plane (LWRP) at every survey point, bins the points by depth, buffers + dissolves
each bin into a polygon, and writes one GeoJSON per survey to data/DepthPolygons/.

Run after process_surveys.py (and optionally compute_bathym_stats.py).
Re-runs are safe: already-processed surveys are skipped.
"""

import shutil
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.ops import unary_union

# ── CONFIG ────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
NAVD88_DIR = DATA_DIR / "NAVD88Files"
OUT_DIR = DATA_DIR / "DepthPolygons"
OUT_DIR.mkdir(parents=True, exist_ok=True)

LWRP7_FILE = SCRIPT_DIR / "lwrp7_info.csv"    # lower Mississippi (LM_*)
LWRP14_FILE = SCRIPT_DIR / "lwrp14_info.csv"  # upper Mississippi (UM_*)
MILEMARKERS_FILE = SCRIPT_DIR / "usace_river_mile_markers.csv"

UTM_CRS = "EPSG:26915"
BUFFER_M = 40        # buffer radius per survey point before dissolve
SIMPLIFY_M = 5       # simplify tolerance on dissolved polygons

# Depth bins: (lower_inclusive, upper_exclusive, label)
# None = unbounded on that side
DEPTH_BINS = [
    (20,   None,  ">20 ft"),
    (17.5, 20,    "17.5-20 ft"),
    (15,   17.5,  "15-17.5 ft"),
    (14,   15,    "14-15 ft"),
    (13,   14,    "13-14 ft"),
    (12,   13,    "12-13 ft"),
    (11,   12,    "11-12 ft"),
    (10,   11,    "10-11 ft"),
    (9,    10,    "9-10 ft"),
    (8,    9,     "8-9 ft"),
    (7,    8,     "7-8 ft"),
    (6,    7,     "6-7 ft"),
    (5,    6,     "5-6 ft"),
    (None, 5,     "<5 ft"),
]
# For ordering depth bins in the output (deep → shallow)
BIN_ORDER = {label: i for i, (_, _, label) in enumerate(DEPTH_BINS)}


def assign_bin(depth):
    for lo, hi, label in DEPTH_BINS:
        if lo is not None and depth < lo:
            continue
        if hi is not None and depth >= hi:
            continue
        return label
    return None


def interp_lwrp(mile, df, mile_col, navd_col):
    """Linearly interpolate LWRP NAVD88 elevation at a given river mile."""
    df = df.sort_values(mile_col)
    up = df[df[mile_col] <= mile]
    dn = df[df[mile_col] >= mile]
    if up.empty:
        return float(dn.iloc[0][navd_col])
    if dn.empty:
        return float(up.iloc[-1][navd_col])
    r_up, r_dn = up.iloc[-1], dn.iloc[0]
    if r_up[mile_col] == r_dn[mile_col]:
        return float(r_up[navd_col])
    frac = (mile - r_up[mile_col]) / (r_dn[mile_col] - r_up[mile_col])
    return float(r_up[navd_col]) + frac * (float(r_dn[navd_col]) - float(r_up[navd_col]))


# ── LOAD SUPPORT DATA ─────────────────────────────────────────────────────────
lwrp7 = pd.read_csv(LWRP7_FILE)
lwrp14 = pd.read_csv(LWRP14_FILE)

milemarkers = pd.read_csv(MILEMARKERS_FILE)
mm_lo = milemarkers[milemarkers["RIVER_NAME"] == "MISSISSIPPI-LO"]
mm_up = milemarkers[milemarkers["RIVER_NAME"] == "MISSISSIPPI-UP"]

mm_lo_gdf = gpd.GeoDataFrame(
    mm_lo[["MILE"]], geometry=gpd.points_from_xy(mm_lo["LON"], mm_lo["LAT"]), crs="EPSG:4326"
).to_crs(UTM_CRS)
mm_up_gdf = gpd.GeoDataFrame(
    mm_up[["MILE"]], geometry=gpd.points_from_xy(mm_up["LON"], mm_up["LAT"]), crs="EPSG:4326"
).to_crs(UTM_CRS)


def nearest_mile(midpoint_utm, is_lm):
    mm_gdf = mm_lo_gdf if is_lm else mm_up_gdf
    dists = mm_gdf.geometry.distance(midpoint_utm)
    return float(mm_gdf.loc[dists.idxmin(), "MILE"])


# ── PROCESS SURVEYS ───────────────────────────────────────────────────────────
files = sorted(NAVD88_DIR.glob("*_SurveyPoint.gpkg"))
already_done = {f.stem.replace("_depth_polygons", "") for f in OUT_DIR.glob("*_depth_polygons.geojson")}
new_files = [f for f in files if f.name.replace("_SurveyPoint.gpkg", "") not in already_done]
print(f"{len(new_files)} survey(s) to process ({len(files)} total, {len(already_done)} already done)")

for fpath in new_files:
    survey_id = fpath.name.replace("_SurveyPoint.gpkg", "")
    is_lm = survey_id.upper().startswith("LM_")

    gdf = gpd.read_file(fpath)          # EPSG:3857 from read_in_surveys.py
    gdf_utm = gdf.to_crs(UTM_CRS)

    # Look up LWRP NAVD88 elevation at the survey's river mile
    midpoint_utm = gdf_utm.union_all().centroid
    mile = nearest_mile(midpoint_utm, is_lm)
    if is_lm:
        lwrp = interp_lwrp(mile, lwrp7, "MileMarker", "NAVD88_ft")
    else:
        lwrp = interp_lwrp(mile, lwrp14, "milemarkers", "navd88")

    # Depth under LWRP at each point
    gdf_utm["depth_ft"] = lwrp - gdf_utm["Z_navd88"]
    gdf_utm["depth_bin"] = gdf_utm["depth_ft"].apply(assign_bin)
    gdf_utm = gdf_utm.dropna(subset=["depth_bin"])

    if gdf_utm.empty:
        print(f"{survey_id}: no points after binning, skipping")
        continue

    # Buffer points then dissolve into one polygon per depth bin
    pts = gdf_utm[["depth_bin", "geometry"]].copy()
    pts["geometry"] = pts.geometry.buffer(BUFFER_M)
    dissolved = pts.dissolve(by="depth_bin").reset_index()
    dissolved["geometry"] = dissolved.geometry.simplify(SIMPLIFY_M)

    # Metadata
    survey_date = str(gdf["SurveyDateStamp"].iloc[0])
    dissolved["survey_id"] = survey_id
    dissolved["date"] = survey_date
    dissolved["year"] = pd.to_datetime(survey_date, utc=True).year
    dissolved["mile"] = round(mile, 1)
    dissolved["bin_order"] = dissolved["depth_bin"].map(BIN_ORDER)

    dissolved = gpd.GeoDataFrame(dissolved, geometry="geometry", crs=UTM_CRS).to_crs("EPSG:4326")

    out_path = OUT_DIR / f"{survey_id}_depth_polygons.geojson"
    dissolved.to_file(out_path, driver="GeoJSON")
    n_pts = len(gdf_utm)
    n_bins = len(dissolved)
    depth_range = f"{gdf_utm['depth_ft'].min():.1f}–{gdf_utm['depth_ft'].max():.1f} ft"
    print(f"{survey_id}: {n_pts} pts → {n_bins} depth-bin polygons, depth {depth_range}, mile {mile:.1f}")

    # Clean up intermediate files now that the GeoJSON is confirmed written
    navd88_gpkg = fpath  # already the NAVD88Files path
    sp_gpkg = SCRIPT_DIR / "data" / "SurveyPointLayers" / fpath.name
    sp_gdb = SCRIPT_DIR / "data" / "SurveyPointLayers" / f"{survey_id}_gdb"

    navd88_gpkg.unlink(missing_ok=True)
    sp_gpkg.unlink(missing_ok=True)
    if sp_gdb.exists():
        shutil.rmtree(sp_gdb)

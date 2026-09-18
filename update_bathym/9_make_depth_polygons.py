"""
Stage 9: generate per-survey depth-bin polygon files.

For each survey gpkg in NAVD88Files/, computes depth under the Low Water Reference
Plane (LWRP) at every survey point, bins each point to its nearest whole foot of depth,
buffers + dissolves each whole-foot bin into a polygon, and writes one GeoJSON per
survey to data/DepthPolygons/.

Whole-foot resolution (not the old coarser named bands) is what lets
make_combined_depth_polygons.py later shift a survey by an exact stage offset and still
regroup precisely -- a survey stored pre-bucketed into e.g. "<5 ft" couldn't tell a point
at 0.2ft from one at 4.9ft, which matters once you're adding a 10+ft stage offset to it.
The coarse "<5 ft / 5-9 ft / ..." bands users actually see are applied later, in
make_combined_depth_polygons.py's DISPLAY_BINS, after that shift.

Run after 3_process_surveys.py (and optionally 4_compute_thresh_depth.py). Must also run
LAST of the survey-processing stages, after 7_compute_navigable_width.py AND
8_compute_width_by_stage.py -- this is the stage that deletes each survey's raw
NAVD88Files/SurveyPointLayers gpkg once its polygons are written, and both of those
still need that gpkg's depth_ft/survey_type columns:

    1_check_for_surveys.py -> 2_read_in_surveys.py -> 3_process_surveys.py ->
    4_compute_thresh_depth.py -> 7_compute_navigable_width.py ->
    8_compute_width_by_stage.py -> 9_make_depth_polygons.py

Re-runs are safe: already-processed surveys are skipped.
"""

import shutil
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Point

# ── CONFIG ────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DATA_DIR = SCRIPT_DIR / "data"
NAVD88_DIR = DATA_DIR / "NAVD88Files"
# 6_review_surveys.py's point-exclusion sidecars (7/8-only, see that script's module
# docstring) -- this stage never reads from here (depth polygons always use every
# point), but owns cleaning it up once a survey's raw files are deleted below.
TRIM_DIR = DATA_DIR / "NavigablePathTrims"
OUT_DIR = DATA_DIR / "DepthPolygons"
OUT_DIR.mkdir(parents=True, exist_ok=True)

CLEAN_BATHYMETRY_FILE = REPO_ROOT / "bathym_fixed.csv"  # gates on confirmed=="yes"

DATUM_INFO_FILE = SCRIPT_DIR / "datum_info.csv"  # Memphis=-5ft water surface (combined miles)
MILEMARKERS_FILE = SCRIPT_DIR / "usace_river_mile_markers.csv"

UTM_CRS = "EPSG:26915"
BUFFER_M = 40        # buffer radius per survey point before dissolve
SIMPLIFY_M = 5       # simplify tolerance on dissolved polygons

# Multibeam surveys can carry 300k-1M+ points -- buffering + dissolving that many
# circles is what hangs on those files. Above this count, grid-downsample per
# depth bin first: points closer than GRID_CELL_M apart produce near-identical
# buffer coverage anyway, so this cuts point count ~100-500x with no visible
# change to the output polygons.
DOWNSAMPLE_THRESHOLD_PTS = 150_000
GRID_CELL_M = 20

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
datum_info = pd.read_csv(DATUM_INFO_FILE)  # MileMarker (combined), thresh_el (NAVD88)

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

# never delete raw data for a survey that hasn't been through 6_review_surveys.py yet
# (its sign-flip check) -- this stage's gpkg deletion is one-way.
if CLEAN_BATHYMETRY_FILE.exists():
    bathym_fixed = pd.read_csv(CLEAN_BATHYMETRY_FILE)
    confirmed_files = set(bathym_fixed.loc[bathym_fixed["confirmed"] == "yes", "file"])
    # also never delete a file ahead of 8_compute_width_by_stage.py, which still needs
    # this same raw gpkg and won't run until a human has confirmed 7's path in
    # 7b_review_navigable_path.py (path_confirmed=="yes") -- but ONLY for survey_types
    # that actually go through 7/8 in the first place (checked per-file below, once
    # survey_type is available off the gpkg itself); a sparse_lines survey never gets a
    # path to confirm (7_compute_navigable_width.py skips it outright), so gating it on
    # path_confirmed here would strand it here forever.
    path_confirmed_files = set(bathym_fixed.loc[bathym_fixed["path_confirmed"] == "yes", "file"]) \
        if "path_confirmed" in bathym_fixed.columns else set()
    not_yet_confirmed = [f for f in new_files if f.name not in confirmed_files]
    new_files = [f for f in new_files if f.name in confirmed_files]
    if not_yet_confirmed:
        print(f"{len(not_yet_confirmed)} survey(s) skipped, not confirmed=yes in {CLEAN_BATHYMETRY_FILE}")
else:
    path_confirmed_files = set()

print(f"{len(new_files)} survey(s) to process ({len(files)} total, {len(already_done)} already done)")

for fpath in new_files:
    survey_id = fpath.name.replace("_SurveyPoint.gpkg", "")
    is_lm = survey_id.upper().startswith("LM_")

    gdf = gpd.read_file(fpath)          # EPSG:3857 from 2_read_in_surveys.py
    gdf_utm = gdf.to_crs(UTM_CRS)

    # interpolate_cross_section_to_blob.py tags every point "real" (an actual survey
    # measurement) or "interpolated" (synthetic grid infill) in a point_source column,
    # present only on _INTERP surveys. The interpolated infill exists so width_blob.py
    # (stages 7/8) has grid coverage everywhere in the hull -- it's not real depth data,
    # so depth polygons (this stage) should only ever be shaped by the real points.
    if "point_source" in gdf_utm.columns:
        n_before = len(gdf_utm)
        gdf_utm = gdf_utm.loc[gdf_utm["point_source"] == "real"].copy()
        gdf = gdf.loc[gdf_utm.index]
        print(f"{survey_id}: real-points-only for depth polygons ({n_before} -> {len(gdf_utm)} pts)")

    survey_type = gdf["survey_type"].iloc[0] if "survey_type" in gdf.columns else None
    if survey_type != "sparse_lines" and fpath.name not in path_confirmed_files:
        print(f"{survey_id}: path not confirmed, skipping (run 7b_review_navigable_path.py first)")
        continue

    # Look up Memphis=-5ft water surface elevation at the survey's river mile.
    # centroid of a point-union is just the mean of the points -- computing it this
    # way instead of union_all().centroid avoids GEOS dissolving millions of points
    # into one geometry, which is impractically slow for the largest LM_26_HIK surveys
    midpoint_utm = Point(gdf_utm.geometry.x.mean(), gdf_utm.geometry.y.mean())
    mile = nearest_mile(midpoint_utm, is_lm)
    combined_mile = mile if is_lm else mile + 953
    lwrp = interp_lwrp(combined_mile, datum_info, "MileMarker", "thresh_el")

    # Depth under LWRP at each point, rounded to its nearest whole foot -- see module
    # docstring for why whole feet instead of named bands.
    gdf_utm["depth_ft"] = lwrp - gdf_utm["Z_navd88"]
    gdf_utm["depth_bin"] = gdf_utm["depth_ft"].round().astype(int)

    if gdf_utm.empty:
        print(f"{survey_id}: no points after binning, skipping")
        continue

    depth_min, depth_max = gdf_utm["depth_ft"].min(), gdf_utm["depth_ft"].max()
    n_pts_orig = len(gdf_utm)
    if n_pts_orig > DOWNSAMPLE_THRESHOLD_PTS:
        gx = (gdf_utm.geometry.x // GRID_CELL_M).astype(int)
        gy = (gdf_utm.geometry.y // GRID_CELL_M).astype(int)
        gdf_utm = gdf_utm.loc[~pd.DataFrame({"b": gdf_utm["depth_bin"], "x": gx, "y": gy}).duplicated()]
        print(f"{survey_id}: downsampled {n_pts_orig} -> {len(gdf_utm)} pts ({GRID_CELL_M}m grid) before buffering")

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
    # deepest first, matching the old named-band convention -- purely a z-draw-order
    # hint for app.py, not meaningful on its own
    dissolved["bin_order"] = -dissolved["depth_bin"]

    dissolved = gpd.GeoDataFrame(dissolved, geometry="geometry", crs=UTM_CRS).to_crs("EPSG:4326")

    out_path = OUT_DIR / f"{survey_id}_depth_polygons.geojson"
    dissolved.to_file(out_path, driver="GeoJSON")
    n_bins = len(dissolved)
    depth_range = f"{depth_min:.1f}–{depth_max:.1f} ft"
    print(f"{survey_id}: {n_pts_orig} pts → {n_bins} depth-bin polygons, depth {depth_range}, mile {mile:.1f}")

    # Clean up intermediate files now that the GeoJSON is confirmed written
    navd88_gpkg = fpath  # already the NAVD88Files path
    sp_gpkg = SCRIPT_DIR / "data" / "SurveyPointLayers" / fpath.name
    sp_gdb = SCRIPT_DIR / "data" / "SurveyPointLayers" / f"{survey_id}_gdb"

    navd88_gpkg.unlink(missing_ok=True)
    sp_gpkg.unlink(missing_ok=True)
    if sp_gdb.exists():
        shutil.rmtree(sp_gdb)
    (TRIM_DIR / fpath.name).unlink(missing_ok=True)

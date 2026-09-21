"""
Converts a "cross_sections"-classified survey that doesn't actually fit the linear
cross-section process (curved/wiggly lines instead of straight transects, so
3_process_surveys.py's build_sections()/check_linearity() math doesn't describe it well)
into a synthetic "full_coverage" survey, so it can be run through the blob pathway
(width_blob.py) instead -- picking a starting orientation (see 6_review_surveys.py's
BLOB_PATH_MODE_OPTIONS: horizontal / vertical / original_horizontal / original_vertical
/ neither) rather than trying to force it through the straight-line math.

How: takes the survey's own convex hull, lays a regular 40ft grid over it, keeps only
the grid points that actually fall inside the hull, and interpolates Z_navd88 at each
of them from the real nearby points (scipy.interpolate.griddata, linear -- falls back to
nearest for any point griddata's Delaunay triangulation leaves NaN, which can happen
right at the hull boundary). The ORIGINAL real points are kept as-is alongside the new
interpolated ones (not replaced) -- this is additive infill to guarantee blob-pathway-
usable density (a point at least every 40ft everywhere in the hull), not a resample that
throws away real measurements. depth_ft is recomputed for every point (real and
interpolated) from Z_navd88 against the SAME single water_elev value already on this
survey's bathym_fixed.csv row, for internal consistency.

What gets written:
  - data/NAVD88Files/{survey_id}_INTERP_SurveyPoint.gpkg -- new synthetic survey,
    survey_type="full_coverage", ready to go through 6_review_surveys.py itself (this
    is the "run it through app 6 again" step the user asked for -- same sign-check /
    box-lasso exclude / split tools apply, plus now the blob orientation picker).
  - bathym_fixed.csv: new row for {survey_id}_INTERP (confirmed="no", milemarker/
    geometry recomputed from the new point set's own convex hull -- same nearest_mile
    approach 6_review_surveys.py's build_split_row uses; water_elev/datum/date copied
    from the original row, since it's the same real survey, just resampled; provenance
    recorded in a new interpolated_from column, deliberately NOT bathym_fixed.csv's own
    duplicate_of -- that column is read by 6_review_surveys.py's own possible-duplicate
    warning, and every interpolated survey pointing duplicate_of at its own source
    triggered that warning as a false positive on every single one, 2026-08-31). The
    ORIGINAL row is set confirmed="interpolated" (a new status, distinct from "merged"/
    "split" -- means "superseded by an interpolated full_coverage version, see
    interpolated_from") and its raw gpkg archived to data/SurveysToReview/, same
    convention used for a merge's consumed source surveys.
  - 2022_channel_width_survey_list.csv: the original survey_id is swapped for the new
    {survey_id}_INTERP one, if present (same "swap in the new id by hand" convention
    noted for a 6_review_surveys.py split).

Usage: python3 interpolate_cross_section_to_blob.py <survey_id>
  survey_id is the bare id, e.g. LM_18_FRIX_20220719_CS_651_653_SORT_LWRP (no
  _SurveyPoint.gpkg suffix). Looks for the raw gpkg in data/2022toFix/ first (where a
  deferred survey sits), then data/NAVD88Files/.

Run by hand, once per survey as the user identifies one that needs this treatment; not
part of the daily pipeline chain.
"""

import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from scipy.interpolate import griddata
from shapely.geometry import Point, box
from shapely.prepared import prep

sys.path.insert(0, str(Path(__file__).resolve().parent))
import importlib
_stage7 = importlib.import_module("OLD_compute_navigable_width")

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DEFERRED_DIR = SCRIPT_DIR / "data" / "2022toFix"
NAVD88_DIR = SCRIPT_DIR / "data" / "NAVD88Files"
SURVEYS_TO_REVIEW_DIR = SCRIPT_DIR / "data" / "SurveysToReview"
# 6_review_surveys.py's path/width-exclusion sidecars -- same filename as the raw
# gpkg, just a point SUBSET. Reading this instead of the raw file when it exists is
# how a box/lasso exclusion drawn in app 6 (checked WITHOUT "Split into 2 chunks")
# shrinks what this script sees, same convention 7_compute_navigable_width.py and
# 8_compute_width_by_stage.py already follow -- requested 2026-08-31 so a reviewer
# can cut down an oversized convex hull in app 6 BEFORE interpolating, rather than
# after.
TRIM_DIR = SCRIPT_DIR / "data" / "NavigablePathTrims"
BATHYM_FIXED_FILE = REPO_ROOT / "bathym_fixed.csv"
SURVEY_LIST_FILE = SCRIPT_DIR / "2022_channel_width_survey_list.csv"

UTM_CRS = _stage7.UTM_CRS
FT_PER_M = _stage7.FT_PER_M
GRID_SPACING_FT = 40.0


def find_survey_path(survey_id):
    fname = f"{survey_id}_SurveyPoint.gpkg"
    for d in (DEFERRED_DIR, NAVD88_DIR):
        p = d / fname
        if p.exists():
            return p
    raise SystemExit(f"{fname} not found in {DEFERRED_DIR} or {NAVD88_DIR}")


def main(survey_id):
    src_path = find_survey_path(survey_id)
    new_id = f"{survey_id}_INTERP"
    new_file = f"{new_id}_SurveyPoint.gpkg"
    orig_file = f"{survey_id}_SurveyPoint.gpkg"

    bathym_fixed = pd.read_csv(BATHYM_FIXED_FILE)
    orig_mask = bathym_fixed["file"] == orig_file
    orig_row = bathym_fixed.loc[orig_mask]
    if orig_row.empty:
        raise SystemExit(f"{orig_file} not found in {BATHYM_FIXED_FILE}")
    orig_row = orig_row.iloc[0]
    if orig_row["confirmed"] != "yes":
        raise SystemExit(f"{survey_id}: confirmed={orig_row['confirmed']!r}, not 'yes' -- "
                          f"review it in 6_review_surveys.py first (sign-check, and draw a "
                          f"box/lasso exclusion there if the hull needs trimming down before "
                          f"interpolating). Stopping.")

    trim_path = TRIM_DIR / orig_file
    read_path = trim_path if trim_path.exists() else src_path
    print(f"{survey_id}: loading {read_path}{' (trimmed sidecar)' if read_path == trim_path else ''}...")
    gdf = gpd.read_file(read_path)
    print(f"  {len(gdf)} pts loaded, survey_type={gdf['survey_type'].iloc[0]!r}")
    gdf_utm = gdf.to_crs(UTM_CRS)
    coords = np.column_stack([gdf_utm.geometry.x.values, gdf_utm.geometry.y.values])
    z = gdf_utm["Z_navd88"].values.astype(float)

    hull = gpd.GeoSeries(gpd.points_from_xy(coords[:, 0], coords[:, 1])).union_all().convex_hull
    minx, miny, maxx, maxy = hull.bounds
    spacing_m = GRID_SPACING_FT / FT_PER_M

    print(f"  hull bounds: {maxx - minx:.0f}m x {maxy - miny:.0f}m, "
          f"gridding at {GRID_SPACING_FT:.0f}ft ({spacing_m:.1f}m) spacing...")
    gx = np.arange(minx, maxx + spacing_m, spacing_m)
    gy = np.arange(miny, maxy + spacing_m, spacing_m)
    gxx, gyy = np.meshgrid(gx, gy)
    candidates = np.column_stack([gxx.ravel(), gyy.ravel()])

    prepared_hull = prep(hull)
    inside = np.array([prepared_hull.contains(Point(x, y)) for x, y in candidates])
    grid_xy = candidates[inside]
    print(f"  {len(grid_xy)} grid points inside the hull "
          f"(of {len(candidates)} candidates)")

    print("  interpolating Z_navd88 at grid points (linear, nearest fallback)...")
    grid_z = griddata(coords, z, grid_xy, method="linear")
    nan_mask = np.isnan(grid_z)
    if nan_mask.any():
        grid_z[nan_mask] = griddata(coords, z, grid_xy[nan_mask], method="nearest")
        print(f"  {int(nan_mask.sum())} point(s) needed nearest-neighbor fallback (edge of hull)")

    water_elev = float(orig_row["water_elev"])
    survey_date = gdf["SurveyDateStamp"].iloc[0]

    orig_out = gpd.GeoDataFrame({
        "SurveyDateStamp": gdf["SurveyDateStamp"].values,
        "survey_type": "full_coverage",
        "Z_navd88": z,
        "depth_ft": water_elev - z,
        "point_source": "real",
        "geometry": gdf_utm.geometry.values,
    }, crs=UTM_CRS)
    interp_out = gpd.GeoDataFrame({
        "SurveyDateStamp": survey_date,
        "survey_type": "full_coverage",
        "Z_navd88": grid_z,
        "depth_ft": water_elev - grid_z,
        "point_source": "interpolated",
        "geometry": gpd.points_from_xy(grid_xy[:, 0], grid_xy[:, 1]),
    }, crs=UTM_CRS)
    combined = pd.concat([orig_out, interp_out], ignore_index=True)
    # interp_out's SurveyDateStamp came from a scalar (broadcast to every row), which
    # concat leaves as dtype "object" instead of a proper datetime64 -- pyogrio's GPKG
    # writer chokes on that ("cannot convert float NaN to integer", confirmed
    # 2026-08-31) even though there's no actual NaN in the data.
    combined["SurveyDateStamp"] = pd.to_datetime(combined["SurveyDateStamp"], utc=True)
    combined = gpd.GeoDataFrame(combined, geometry="geometry", crs=UTM_CRS).to_crs("EPSG:3857")

    NAVD88_DIR.mkdir(parents=True, exist_ok=True)
    out_path = NAVD88_DIR / new_file
    combined.to_file(out_path, driver="GPKG")
    print(f"  wrote {out_path} ({len(orig_out)} real + {len(interp_out)} interpolated "
          f"= {len(combined)} pts)")

    # ── bathym_fixed.csv bookkeeping ─────────────────────────────────────────
    for col in ("file", "confirmed"):
        if bathym_fixed[col].dtype != object:
            bathym_fixed[col] = bathym_fixed[col].astype(object)

    hull_3857 = gpd.GeoSeries([hull], crs=UTM_CRS).to_crs("EPSG:3857").iloc[0]
    midpoint_utm = Point(coords[:, 0].mean(), coords[:, 1].mean())
    is_lm = survey_id.upper().startswith("LM_")
    mile = _stage7.nearest_mile(midpoint_utm, is_lm)

    new_row = pd.Series({col: pd.NA for col in bathym_fixed.columns})
    new_row["file"] = new_file
    new_row["date"] = orig_row["date"]
    new_row["datum"] = orig_row["datum"]
    new_row["milemarker"] = round(mile, 1)
    new_row["water_elev"] = water_elev
    new_row["geometry"] = hull_3857.wkt
    new_row["year"] = orig_row["year"]
    new_row["confirmed"] = "no"
    # provenance -- deliberately NOT duplicate_of, see module docstring
    new_row["interpolated_from"] = orig_file

    bathym_fixed.loc[orig_mask, "confirmed"] = "interpolated"
    bathym_fixed = pd.concat([bathym_fixed, pd.DataFrame([new_row])], ignore_index=True)
    bathym_fixed.to_csv(BATHYM_FIXED_FILE, index=False)
    print(f"  added {new_file} row (confirmed=no), marked {orig_file} confirmed=interpolated")

    SURVEYS_TO_REVIEW_DIR.mkdir(parents=True, exist_ok=True)
    src_path.rename(SURVEYS_TO_REVIEW_DIR / orig_file)
    print(f"  archived {orig_file} to {SURVEYS_TO_REVIEW_DIR}")

    # ── swap the survey list entry, if this survey_id is on it ──────────────
    if SURVEY_LIST_FILE.exists():
        survey_list = pd.read_csv(SURVEY_LIST_FILE)
        list_mask = survey_list["survey_id"] == survey_id
        if list_mask.any():
            survey_list.loc[list_mask, "survey_id"] = new_id
            survey_list.loc[list_mask, "n_pts"] = len(combined)
            survey_list.loc[list_mask, "geometry_source"] = "interpolated_full_coverage"
            survey_list.to_csv(SURVEY_LIST_FILE, index=False)
            print(f"  swapped {survey_id} -> {new_id} in {SURVEY_LIST_FILE}")
        else:
            print(f"  {survey_id} not found in {SURVEY_LIST_FILE}, nothing to swap")

    print(f"\nDone. {new_id}: confirmed=no -- review in 6_review_surveys.py "
          f"(pick a blob orientation, sign-check, cut/split if needed) before stage 7.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: python3 interpolate_cross_section_to_blob.py <survey_id>")
    main(sys.argv[1])

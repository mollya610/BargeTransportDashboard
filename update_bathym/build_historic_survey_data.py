"""
One-time (re-runnable) conversion of the 2015-2020 historic survey data Molly dropped
into HistoricDepthPolygons/ so app_survey_map.py can show it the same way it shows
2021+ surveys from bathym_fixed.csv.

Two outputs:

1. HistoricDepthPolygons/CSVs/*.csv (per-survey_id/year/date/lat/lon, chunked) combined
   into one file, HistoricDepthPolygons/historic_surveys_combined.csv, with a milemarker
   column added (nearest USACE mile marker, same combined LM/UM numbering bathym_fixed.csv
   uses) so app_survey_map.py can bucket each dot to a gage region exactly like it does
   for 2021+ surveys.

2. Each per-survey depth geojson (HistoricDepthPolygons/{survey_id}/{survey_id}_depth.geojson)
   reprojected to EPSG:4326 and written to
   update_bathym/data/DepthPolygons/{survey_id}_depth_polygons.geojson in the same schema
   6_make_depth_polygons.py produces (depth_bin, survey_id, date, year, mile, bin_order),
   so the existing click-to-see-depth-map code path (app.py's
   _load_depth_polygon_bins/_add_depth_polygon_traces) needs no changes to render it.

   The source geojson's depth_bin is a string range in 5ft increments (e.g. "-5-0",
   "0-5", "5-10" -- depth under LWRP, negative meaning above the LWRP plane), Molly's own
   labeling for these surveys and what she wants to see on hover -- unlike
   6_make_depth_polygons.py's single-whole-foot-int depth_bin, which app.py re-buckets
   into the shared coarse DEPTH_POLY_COLORS display bands (is_numeric_dtype check in
   _load_depth_polygon_bins) so a year's worth of surveys don't draw dozens of
   same-colored overlapping traces. Historic surveys are viewed one at a time via
   click-through, not combined, so that re-bucketing isn't needed here -- depth_bin is
   kept as the original range string (+" ft") so app.py's non-numeric branch passes it
   through unchanged, and bin_order is written explicitly (computed from each range's
   midpoint, deepest first) since that branch relies on it already being present rather
   than deriving it like the numeric branch does.

Re-runs are safe: already-converted surveys are skipped. If HistoricDepthPolygons/'s
survey set OR bin scheme changes (as happened 2026-09-21, whole-foot bins -> 5ft-range
bins), delete update_bathym/data/DepthPolygons/'s stale entries for the affected
survey_ids yourself first -- this script only ever adds.
"""
import glob
import re
from pathlib import Path

import geopandas as gpd
import pandas as pd

_BIN_RANGE_RE = re.compile(r"^(-?\d+(?:\.\d+)?)-(-?\d+(?:\.\d+)?)$")


def _bin_midpoint(depth_bin):
    """'-5-0' -> -2.5, '5-10' -> 7.5, etc -- used only to order/color the bin, see
    module docstring."""
    m = _BIN_RANGE_RE.match(str(depth_bin))
    lo, hi = float(m.group(1)), float(m.group(2))
    return (lo + hi) / 2

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
HIST_DIR = SCRIPT_DIR / "HistoricDepthPolygons"
CSV_DIR = HIST_DIR / "CSVs"
OUT_CSV = HIST_DIR / "historic_surveys_combined.csv"
OUT_POLY_DIR = SCRIPT_DIR / "data" / "DepthPolygons"
OUT_POLY_DIR.mkdir(parents=True, exist_ok=True)

UTM_CRS = "EPSG:26915"
SIMPLIFY_M = 5  # matches 6_make_depth_polygons.py's post-dissolve simplify tolerance
MILEMARKERS_FILE = SCRIPT_DIR / "usace_river_mile_markers.csv"

# ── combine the chunked survey list ────────────────────────────────────────────
# chunk files can overlap (same survey_id appearing in more than one chunk) --
# drop_duplicates keeps this idempotent under re-chunking
chunk_files = sorted(glob.glob(str(CSV_DIR / "*.csv")))
combined = pd.concat([pd.read_csv(f) for f in chunk_files], ignore_index=True)
combined = combined.drop_duplicates(subset="survey_id").sort_values("survey_id").reset_index(drop=True)
print(f"combined {len(chunk_files)} chunk file(s) -> {len(combined)} surveys")

# nearest combined-numbering mile marker for each survey point, same approach as
# 6_make_depth_polygons.py's nearest_mile() (UTM nearest-neighbor). All historic surveys
# are LM_-prefixed (Lower Mississippi), so only MISSISSIPPI-LO markers are needed --
# that river's raw MILE already matches bathym_fixed.csv's combined numbering (LM segment
# offset is 0; UM would need +953, not needed here).
milemarkers = pd.read_csv(MILEMARKERS_FILE)
mm_lo = milemarkers[milemarkers["RIVER_NAME"] == "MISSISSIPPI-LO"]
mm_lo_gdf = gpd.GeoDataFrame(
    mm_lo[["MILE"]], geometry=gpd.points_from_xy(mm_lo["LON"], mm_lo["LAT"]), crs="EPSG:4326"
).to_crs(UTM_CRS)

survey_pts = gpd.GeoDataFrame(
    combined, geometry=gpd.points_from_xy(combined["lon"], combined["lat"]), crs="EPSG:4326"
).to_crs(UTM_CRS)


def _nearest_mile(point):
    dists = mm_lo_gdf.geometry.distance(point)
    return float(mm_lo_gdf.loc[dists.idxmin(), "MILE"])


combined["milemarker"] = survey_pts.geometry.apply(_nearest_mile)
combined.to_csv(OUT_CSV, index=False)
print(f"wrote {OUT_CSV}")

# ── convert each survey's depth geojson to a depth-polygon geojson ────────────
already_done = {f.stem.replace("_depth_polygons", "") for f in OUT_POLY_DIR.glob("*_depth_polygons.geojson")}
mile_by_survey = combined.set_index("survey_id")["milemarker"]
date_by_survey = combined.set_index("survey_id")["date"]
year_by_survey = combined.set_index("survey_id")["year"]

n_done = 0
n_skipped = 0
n_missing = 0
for survey_id in combined["survey_id"]:
    if survey_id in already_done:
        n_skipped += 1
        continue
    src_path = HIST_DIR / survey_id / f"{survey_id}_depth.geojson"
    if not src_path.exists():
        print(f"{survey_id}: missing {src_path}, skipping")
        n_missing += 1
        continue

    gdf = gpd.read_file(src_path)
    midpoint = gdf["depth_bin"].apply(_bin_midpoint)
    # deepest first, matching 6_make_depth_polygons.py's convention -- a z-draw-order
    # hint for app.py, not meaningful on its own. Computed before depth_bin is
    # overwritten below since app.py's non-numeric branch expects this already present.
    gdf["bin_order"] = -midpoint
    gdf["depth_bin"] = gdf["depth_bin"].astype(str) + " ft"
    gdf["geometry"] = gdf.geometry.simplify(SIMPLIFY_M)
    gdf = gdf.to_crs("EPSG:4326")
    gdf["survey_id"] = survey_id
    gdf["date"] = date_by_survey[survey_id]
    gdf["year"] = int(year_by_survey[survey_id])
    gdf["mile"] = round(mile_by_survey[survey_id], 1)

    out_path = OUT_POLY_DIR / f"{survey_id}_depth_polygons.geojson"
    gdf.to_file(out_path, driver="GeoJSON")
    n_done += 1

print(f"{n_done} survey(s) converted, {n_skipped} already done, {n_missing} missing shapefile")

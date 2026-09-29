"""
One-time (re-runnable) conversion of the unprocessed survey data Molly dropped into
UnprocessedDepthPolygons/ so app_survey_map.py can show it the same way it shows the
2015-2020 HistoricDepthPolygons/ surveys (see build_historic_survey_data.py), as their own
pink dot layer.

Two outputs, same as build_historic_survey_data.py:

1. UnprocessedDepthPolygons/CSVs/*.csv (survey_id/year/date/datum/lat/lon, chunked)
   combined into UnprocessedDepthPolygons/unprocessed_surveys_combined.csv with a
   milemarker column added, so app_survey_map.py can bucket each dot to a gage region.

2. Each per-survey geojson (UnprocessedDepthPolygons/{survey_id}/{survey_id}_z_use.geojson)
   written to update_bathym/data/DepthPolygons/{survey_id}_depth_polygons.geojson in the
   same schema build_historic_survey_data.py writes, so app.py's click-to-see-depth-map
   code path renders it unchanged.

   Unlike HistoricDepthPolygons/'s 5ft-range string depth_bin, these store a whole-foot
   int z_bin (UTM 15N). They're regrouped into the same 5ft-range labels (e.g. "5-10 ft")
   and dissolved, so app.py's per-survey gradient coloring (_bin_colors_for_survey) and
   the click-through legend treat them exactly like the historic surveys -- and one
   survey doesn't draw 100+ single-foot traces. z_bin is used as-is whatever its datum
   (the CSVs' datum column is ACTUALDEPTH or Unknown; the two _SPECIAL surveys run
   strongly negative, i.e. look like elevations rather than depths).

Re-runs are safe: already-converted surveys are skipped. If a survey's source data
changes, delete its update_bathym/data/DepthPolygons/ entry yourself first -- this script
only ever adds.
"""
import glob
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
SRC_DIR = SCRIPT_DIR / "UnprocessedDepthPolygons"
CSV_DIR = SRC_DIR / "CSVs"
OUT_CSV = SRC_DIR / "unprocessed_surveys_combined.csv"
OUT_POLY_DIR = SCRIPT_DIR / "data" / "DepthPolygons"
OUT_POLY_DIR.mkdir(parents=True, exist_ok=True)

UTM_CRS = "EPSG:26915"
SIMPLIFY_M = 5  # matches build_historic_survey_data.py / 6_make_depth_polygons.py
BIN_FT = 5  # matches HistoricDepthPolygons/'s range width
MILEMARKERS_FILE = SCRIPT_DIR / "usace_river_mile_markers.csv"

# ── combine the chunked survey list ────────────────────────────────────────────
chunk_files = sorted(glob.glob(str(CSV_DIR / "*.csv")))
combined = pd.concat([pd.read_csv(f) for f in chunk_files], ignore_index=True)
combined = combined.drop_duplicates(subset="survey_id").sort_values("survey_id").reset_index(drop=True)
print(f"combined {len(chunk_files)} chunk file(s) -> {len(combined)} surveys")

# nearest mile marker, same approach as build_historic_survey_data.py -- all of these are
# LM_-prefixed too, so only MISSISSIPPI-LO markers are needed
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

# ── convert each survey's z_bin geojson to a depth-polygon geojson ────────────
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
    src_path = SRC_DIR / survey_id / f"{survey_id}_z_use.geojson"
    if not src_path.exists():
        print(f"{survey_id}: missing {src_path}, skipping")
        n_missing += 1
        continue

    gdf = gpd.read_file(src_path).to_crs(UTM_CRS)
    lo = (np.floor(gdf["z_bin"].astype(float) / BIN_FT) * BIN_FT).astype(int)
    gdf["depth_bin"] = lo.astype(str) + "-" + (lo + BIN_FT).astype(str) + " ft"
    gdf["_mid"] = lo + BIN_FT / 2
    gdf = gdf.dissolve(by="depth_bin", aggfunc={"_mid": "first"}, as_index=False)
    # deepest first, same z-draw-order convention as build_historic_survey_data.py
    gdf["bin_order"] = -gdf["_mid"]
    gdf = gdf.drop(columns="_mid")
    gdf["geometry"] = gdf.geometry.simplify(SIMPLIFY_M)
    gdf = gdf.to_crs("EPSG:4326")
    gdf["survey_id"] = survey_id
    gdf["date"] = date_by_survey[survey_id]
    gdf["year"] = int(year_by_survey[survey_id])
    gdf["mile"] = round(mile_by_survey[survey_id], 1)

    out_path = OUT_POLY_DIR / f"{survey_id}_depth_polygons.geojson"
    gdf[["depth_bin", "bin_order", "survey_id", "date", "year", "mile", "geometry"]].to_file(out_path, driver="GeoJSON")
    n_done += 1

print(f"{n_done} survey(s) converted, {n_skipped} already done, {n_missing} missing geojson")

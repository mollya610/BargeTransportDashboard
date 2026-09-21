from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from shapely import wkt as shapely_wkt
from shapely.geometry import Point

# ---------------- CONFIG ----------------
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DATA_DIR = SCRIPT_DIR / "data"
NAVD88_DIR = DATA_DIR / "NAVD88Files"  # output of 3_process_surveys.py

DATUMS_FILE = SCRIPT_DIR / "datum_info.csv"
CLEAN_BATHYMETRY_FILE = REPO_ROOT / "bathym_fixed.csv"  # what app.py maps

UTM_CRS = "EPSG:26915"

# ---------------- LOAD SUPPORT DATA ----------------
datums = pd.read_csv(DATUMS_FILE)
datums = gpd.GeoDataFrame(
    datums, geometry=gpd.points_from_xy(datums["LON"], datums["LAT"]), crs="EPSG:4326"
)
datums_utm = datums.to_crs(UTM_CRS)

# ---------------- SKIP SURVEYS ALREADY IN bathym_fixed.csv ----------------
if CLEAN_BATHYMETRY_FILE.exists():
    existing = pd.read_csv(CLEAN_BATHYMETRY_FILE)
    if "Unnamed: 0" in existing.columns:
        existing = existing.drop(columns=["Unnamed: 0"])
    done_files = set(existing["file"])
else:
    existing = pd.DataFrame()
    done_files = set()

# ---------------- DUPLICATE-SURVEY DETECTION ----------------
# USACE's eHydro source occasionally republishes the exact same physical survey
# under a second surveyjobidpk (confirmed 2026-07-08 via the FeatureServer: e.g.
# UM_SL_PRC_20260213_CS_1 "Prototype Reach" and UM_SL_ILC_20260213_CS_1 "Ivory
# Landing" are two different source records with identical survey geometry).
# Flag -- don't dedup automatically -- since a reviewer needs to judge which
# copy (if either) is trustworthy; existing duplicates already in bathym_fixed.csv
# are being handled separately via a full redownload/reprocess, not here.
DUP_ROUND = 4  # ~11m at these latitudes


def _dup_key(geom_wkt, date_val):
    try:
        rep = shapely_wkt.loads(geom_wkt).representative_point()
    except Exception:
        return None
    return (round(rep.x, DUP_ROUND), round(rep.y, DUP_ROUND), str(date_val)[:10])


dup_index = {}
if not existing.empty and "geometry" in existing.columns:
    for _, r in existing.iterrows():
        key = _dup_key(r["geometry"], r["date"])
        if key:
            dup_index.setdefault(key, []).append(r["file"])

files = sorted(NAVD88_DIR.glob("*_SurveyPoint.gpkg"))
new_files = [f for f in files if f.name not in done_files]
print(f"{len(new_files)} new survey(s) to process ({len(files)} total in NAVD88Files)")

rows = []
skipped = 0
for fpath in new_files:
    gdf_orig = gpd.read_file(fpath)  # EPSG:3857, written by 2_read_in_surveys.py
    gdf = gdf_orig.to_crs(epsg=4326)

    date = gdf["SurveyDateStamp"].iloc[0]
    print(f"{fpath.name}: {date}")

    # gdf.union_all() dissolves every point into one geometry first, which is
    # impractically slow for surveys with hundreds of thousands to millions of
    # points (some LM_26_HIK surveys do). Convex hull and envelope don't need that:
    # a hull is unaffected by duplicate/overlapping points, and an envelope's
    # centroid is just the bounding-box center either way.
    coords = np.column_stack([gdf.geometry.x.values, gdf.geometry.y.values])
    poly_tosave = shapely.convex_hull(shapely.multipoints(coords))
    minx, miny, maxx, maxy = gdf.total_bounds
    midpoint = Point((minx + maxx) / 2, (miny + maxy) / 2)
    midpoint_utm = gpd.GeoSeries([midpoint], crs="EPSG:4326").to_crs(UTM_CRS).iloc[0]

    datums_utm["dist"] = datums_utm.geometry.distance(midpoint_utm)
    nearest_row = datums_utm.loc[datums_utm["dist"].idxmin()]
    water_elev = float(nearest_row["thresh_el"])
    milemarker = nearest_row["MileMarker"]

    # depth under the low-water threshold at every point, persisted onto the
    # gpkg itself so downstream stages -- e.g. 7_compute_navigable_width.py --
    # can read depth_ft directly instead of re-deriving water_elev via their
    # own mile lookup. No survey-wide mean/aggregate is computed here.
    gdf_orig["depth_ft"] = water_elev - gdf_orig["Z_navd88"]
    # write to a temp path and swap it in, rather than gdf_orig.to_file(fpath, ...)
    # directly -- overwriting a gpkg in the same process that just read it is an
    # unreliable GDAL/geopandas pattern: it can silently succeed while dropping the
    # newly-added column, reverting to (most of) the original schema. Confirmed
    # 2026-09-21 -- this is why depth_ft went missing on a chunk of confirmed surveys
    # despite this line having run on them.
    tmp_path = fpath.with_suffix(".gpkg.tmp")
    gdf_orig.to_file(tmp_path, driver="GPKG")
    tmp_path.replace(fpath)

    if gdf_orig["depth_ft"].isna().all():
        print(f"  WARNING: no valid Z_navd88 values, skipping {fpath.name}")
        skipped += 1
        continue

    key = _dup_key(poly_tosave.wkt, date)
    dup_matches = dup_index.get(key, []) if key else []
    if dup_matches:
        print(f"  WARNING: possible duplicate of {', '.join(dup_matches)} (same location + date)")
    duplicate_of = ", ".join(dup_matches)
    # register this survey too, so a second new one in the same run also catches it
    if key:
        dup_index.setdefault(key, []).append(fpath.name)

    rows.append({
        "file": fpath.name,
        "date": date,
        "year": pd.to_datetime(date).year,
        "datum": "NAVD88",
        "milemarker": milemarker,
        "water_elev": water_elev,
        "geometry": poly_tosave.wkt,
        # Manual review (6_review_surveys.py) is back, but narrowed to just one job: catch
        # a survey with an inverted Z_navd88 sign before anything downstream trusts its
        # depth_ft. Risk classification is NOT part of that review anymore -- at_risk stays
        # blank; 7_compute_navigable_width.py's vessel_path_connected/width_ft drives the
        # map's risk coloring instead (app.py derives at_risk_eff from those, falling back
        # to this blank/legacy at_risk column only for surveys stage 7 hasn't measured yet).
        "confirmed": "no",
        "at_risk": "",
        "problem_lon": "",
        "problem_lat": "",
        "duplicate_of": duplicate_of,
    })

if not rows:
    print(f"No new surveys to add ({skipped} skipped, no valid Z_navd88 values).")
else:
    new_df = pd.DataFrame(rows)
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined.to_csv(CLEAN_BATHYMETRY_FILE, index=False)
    print(f"Added {len(new_df)} survey(s) to {CLEAN_BATHYMETRY_FILE} ({skipped} skipped, no valid Z_navd88 values)")

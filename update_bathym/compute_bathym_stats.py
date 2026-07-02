import math
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

# ---------------- CONFIG ----------------
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DATA_DIR = SCRIPT_DIR / "data"
NAVD88_DIR = DATA_DIR / "NAVD88Files"  # output of process_surveys.py

CORRIDOR_FILE = SCRIPT_DIR / "ais_grid_counts_clean5.csv"
DATUMS_FILE = SCRIPT_DIR / "datum_info.csv"
CLEAN_BATHYMETRY_FILE = REPO_ROOT / "bathym_fixed.csv"  # what app.py maps

UTM_CRS = "EPSG:26915"
GRID_CELL_M = 75  # ais_grid_counts_clean5.csv cell spacing, inferred from the x/y values
GRID_MATCH_DIST_M = (GRID_CELL_M / 2) * math.sqrt(2)  # half-diagonal: farthest a point can be from its cell's center

# ---------------- LOAD SUPPORT DATA ----------------
corridor_df = pd.read_csv(CORRIDOR_FILE)
corridor = gpd.GeoDataFrame(
    corridor_df[["vessel_count"]],
    geometry=gpd.points_from_xy(corridor_df["x"], corridor_df["y"]),
    crs=UTM_CRS,
)

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

files = sorted(NAVD88_DIR.glob("*_SurveyPoint.gpkg"))
new_files = [f for f in files if f.name not in done_files]
print(f"{len(new_files)} new survey(s) to process ({len(files)} total in NAVD88Files)")

rows = []
for fpath in new_files:
    gdf = gpd.read_file(fpath)  # EPSG:3857, written by read_in_surveys.py
    gdf = gdf.to_crs(epsg=4326)

    date = gdf["SurveyDateStamp"].iloc[0]
    segment_id = gdf["segment_id"].iloc[0]  # assigned in process_surveys.py
    print(f"{fpath.name}: {date}")

    poly_tosave = gdf.union_all().convex_hull
    midpoint = gdf.union_all().envelope.centroid
    midpoint_utm = gpd.GeoSeries([midpoint], crs="EPSG:4326").to_crs(UTM_CRS).iloc[0]

    # weight bathymetry by vessel traffic: assign each survey point the vessel
    # count of its nearest AIS grid cell center, dropping points outside the grid
    subset_utm = gdf.to_crs(UTM_CRS)
    joined = gpd.sjoin_nearest(subset_utm, corridor, max_distance=GRID_MATCH_DIST_M, distance_col="dist_to_cell")
    depths = joined["Z_navd88"].values
    weights = joined["vessel_count"].values

    if len(depths) > 0 and weights.sum() > 0:
        bathym_mean = np.average(depths, weights=weights)
    else:
        bathym_mean = np.nan

    datums_utm["dist"] = datums_utm.geometry.distance(midpoint_utm)
    nearest_row = datums_utm.loc[datums_utm["dist"].idxmin()]
    water_elev = float(nearest_row["thresh_el"])
    milemarker = nearest_row["MileMarker"]

    rows.append({
        "file": fpath.name,
        "date": date,
        "year": pd.to_datetime(date).year,
        "datum": "NAVD88",
        "bathym_mean": bathym_mean,
        "depth": water_elev - bathym_mean,
        "milemarker": milemarker,
        "segment_id": segment_id,
        "water_elev": water_elev,
        "weights_sum": weights.sum(),
        "geometry": poly_tosave.wkt,
        "confirmed": "no",
        "at_risk": "",
    })

if not rows:
    print("No new surveys to add.")
else:
    new_df = pd.DataFrame(rows)
    # no vessel-weighted bathymetry means no usable depth -> drop rather than map a NaN point
    skipped = new_df["bathym_mean"].isna().sum()
    new_df = new_df.dropna(subset=["bathym_mean"])

    combined = pd.concat([existing, new_df], ignore_index=True)
    combined.to_csv(CLEAN_BATHYMETRY_FILE, index=False)
    print(f"Added {len(new_df)} survey(s) to {CLEAN_BATHYMETRY_FILE} ({skipped} skipped, no AIS coverage)")

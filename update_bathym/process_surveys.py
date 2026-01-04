import os
import glob
import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.ops import unary_union

# ---------------- CONFIG ----------------
BASE_DIR = "BathymetryData"
SURVEYPOINT_DIR = os.path.join(BASE_DIR, "SurveyPointLayers")
FIXED_DIR = os.path.join(BASE_DIR, "BathymetryLayers_FIXED_NAVD88")
os.makedirs(FIXED_DIR, exist_ok=True)

# Vessel data
VESSEL_FILE = "october_data_5min.csv"

# Datum transformation files (LWRP2007, LWRP2014, etc.)
LWRP7_FILE = "lwrp_info.csv"

# River segments
SEGMENTS_FILE = "10_mile_river_segments.geojson"

# max survey points per survey to process
MAX_POINTS = 2000

# ---------------- LOAD SUPPORT DATA ----------------
segments = gpd.read_file(SEGMENTS_FILE).to_crs("EPSG:4326")
vessels = pd.read_csv(VESSEL_FILE)
vessels = gpd.GeoDataFrame(vessels,
                           geometry=gpd.points_from_xy(vessels["LON"], vessels["LAT"]),
                           crs="EPSG:4326")
vessels['BaseDateTime'] = pd.to_datetime(vessels['BaseDateTime'], errors='coerce')
utm_crs = "EPSG:26915"
vessels_utm = vessels.to_crs(utm_crs)

# LWRP2007 lookup
lwrp7 = pd.read_csv(LWRP7_FILE)
lwrp7_gdf = gpd.GeoDataFrame(lwrp7,
                              geometry=gpd.points_from_xy(lwrp7["LON"], lwrp7["LAT"]),
                              crs="EPSG:4326")

# ---------------- GET FILES TO PROCESS ----------------
files = glob.glob(os.path.join(SURVEYPOINT_DIR, "*SurveyPoint.gpkg"))
files = sorted(files)
print(f"Found {len(files)} SurveyPoint files.")

# ---------------- PROCESS EACH SURVEY ----------------
output_rows = []

for fpath in files:
    base = os.path.basename(fpath).replace("_SurveyPoint.gpkg","")
    gdf = gpd.read_file(fpath)
    
    # --- datum check ---
    datum = gdf.get("Datum", ["Unknown"])[0] if "Datum" in gdf.columns else "Unknown"
    datum = str(datum).upper()
    
    # --- convert to NAVD88 if needed ---
    if datum == "NAVD88":
        gdf["Z_navd88"] = gdf["Z_use"]
    elif datum == "LWRP2007":
        # find nearest milemarker in LWRP7 and apply conversion
        midpoint = unary_union(gdf.geometry).centroid
        lwrp7_gdf["dist"] = lwrp7_gdf.geometry.distance(midpoint)
        nearest = lwrp7_gdf.loc[lwrp7_gdf["dist"].idxmin()]
        navd88_val = nearest["NAVD88_ft"]
        gdf["Z_navd88"] = navd88_val - gdf["Z_use"]
    else:
        # Unknown or other datums: skip or save separately
        print(f"{base} has unknown datum {datum}, skipping...")
        continue
    
    # --- assign segment ---
    midpoint = unary_union(gdf.geometry).centroid
    midpoint_gdf = gpd.GeoDataFrame(geometry=[midpoint], crs="EPSG:4326")
    midpoint_proj = midpoint_gdf.to_crs(utm_crs)
    segments_proj = segments.to_crs(utm_crs)
    segments_proj["dist"] = segments_proj.geometry.distance(midpoint_proj.geometry.iloc[0])
    nearest_idx = segments_proj["dist"].idxmin()
    segment_id = segments_proj.loc[nearest_idx, "segment_id"]
    
    # --- subsample if too many points ---
    subset = gdf
    if len(subset) > MAX_POINTS:
        subset = subset.sample(MAX_POINTS, random_state=1)
    
    # --- compute convex hull ---
    poly = subset.unary_union.convex_hull
    
    # --- get vessels in survey area ---
    vessels_in = vessels_utm[vessels_utm.within(poly.to_crs(utm_crs))]
    
    vessel_bathyms = []
    for pt in vessels_in.geometry:
        buf = pt.buffer(50)  # 50m buffer
        intersecting = gdf.to_crs(utm_crs)[gdf.to_crs(utm_crs).intersects(buf)]
        if not intersecting.empty:
            vessel_bathyms.append(intersecting["Z_navd88"].mean())
    
    if vessel_bathyms:
        bathym_mean = np.mean(vessel_bathyms)
        bathym_q25 = np.percentile(vessel_bathyms,25)
        bathym_q10 = np.percentile(vessel_bathyms,10)
        bathym_q75 = np.percentile(vessel_bathyms,75)
        bathym_q90 = np.percentile(vessel_bathyms,90)
    else:
        bathym_mean = bathym_q25 = bathym_q10 = bathym_q75 = bathym_q90 = np.nan
    
    output_rows.append({
        "survey_id": base,
        "segment_id": segment_id,
        "bathym_mean": bathym_mean,
        "bathym_q25": bathym_q25,
        "bathym_q10": bathym_q10,
        "bathym_q75": bathym_q75,
        "bathym_q90": bathym_q90,
        "geometry": poly.wkt
    })
    
    # --- save fixed NAVD88 GPKG ---
    out_file = os.path.join(FIXED_DIR, f"{base}_NAVD88.gpkg")
    gdf.to_file(out_file, driver="GPKG")
    print(f"Saved {out_file}")

# --- save summary CSV ---
summary_file = os.path.join(BASE_DIR, "Bathymetry_Vessel_Summary.csv")
summary_df = pd.DataFrame(output_rows)
summary_df.to_csv(summary_file, index=False)
print(f"Saved summary to {summary_file}")

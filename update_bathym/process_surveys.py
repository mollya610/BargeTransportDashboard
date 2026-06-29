import pandas as pd
import geopandas as gpd
from pathlib import Path
from shapely.ops import unary_union

# ---------------- CONFIG ----------------
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
SURVEYPOINT_DIR = DATA_DIR / "SurveyPointLayers"
METADATA_FILE = DATA_DIR / "survey_metadata.csv"

NAVD88_DIR = DATA_DIR / "NAVD88Files"
ACTUALDEPTH_DIR = DATA_DIR / "ActualDepthFiles"  # ACTUALDEPTH conversion deferred - needs per-survey-date gage pull
OTHER_DIR = DATA_DIR / "OtherDatumFiles"          # unknown datum or failed conversion
for d in (NAVD88_DIR, ACTUALDEPTH_DIR, OTHER_DIR):
    d.mkdir(parents=True, exist_ok=True)

SEGMENTS_FILE = SCRIPT_DIR / "10_mile_river_segments.geojson"
LWRP7_FILE = SCRIPT_DIR / "lwrp7_info.csv"
LWRP14_FILE = SCRIPT_DIR / "lwrp14_info.csv"
MILEMARKERS_FILE = SCRIPT_DIR / "usace_river_mile_markers.csv"

UTM_CRS = "EPSG:26915"
LWRP7_DIST_THRESHOLD_M = 10000  # ~6-7 miles

# ---------------- LOAD SUPPORT DATA ----------------
segments = gpd.read_file(SEGMENTS_FILE).to_crs("EPSG:4326")
segments_utm = segments.to_crs(UTM_CRS)

lwrp7 = pd.read_csv(LWRP7_FILE)
lwrp7_gdf = gpd.GeoDataFrame(
    lwrp7, geometry=gpd.points_from_xy(lwrp7["LON"], lwrp7["LAT"]), crs="EPSG:4326"
).to_crs(UTM_CRS)

lwrp14 = pd.read_csv(LWRP14_FILE).sort_values("milemarkers")

milemarkers = pd.read_csv(MILEMARKERS_FILE)
milemarkers = milemarkers[milemarkers["RIVER_NAME"].isin(["MISSISSIPPI-LO", "MISSISSIPPI-UP"])]
milemarkers_gdf = gpd.GeoDataFrame(
    milemarkers[["MILE"]],
    geometry=gpd.points_from_xy(milemarkers["LON"], milemarkers["LAT"]),
    crs="EPSG:4326",
).to_crs(UTM_CRS)

metadata = pd.read_csv(METADATA_FILE)[["survey_id", "datum"]]

# ---------------- GET FILES TO PROCESS ----------------
all_files = sorted(SURVEYPOINT_DIR.glob("*_SurveyPoint.gpkg"))
navd88_done = {f.name for f in NAVD88_DIR.glob("*_SurveyPoint.gpkg")}
actual_done = {f.name for f in ACTUALDEPTH_DIR.glob("*_SurveyPoint.gpkg")}
other_done = {f.stem for f in OTHER_DIR.glob("*.gpkg")}  # stems like "UM_SL_KBC_20260211_CS_1_UNKNOWN"

def already_processed(fpath):
    sid = fpath.name.replace("_SurveyPoint.gpkg", "")
    return (
        fpath.name in navd88_done
        or fpath.name in actual_done
        or any(s.startswith(sid) for s in other_done)
    )

files = [f for f in all_files if not already_processed(f)]
print(f"Found {len(all_files)} SurveyPoint files, {len(files)} not yet processed.")


def nearest_segment_id(midpoint_utm):
    dists = segments_utm.geometry.distance(midpoint_utm)
    return segments_utm.loc[dists.idxmin(), "segment_id"]


def nearest_mile(midpoint_utm):
    dists = milemarkers_gdf.geometry.distance(midpoint_utm)
    return milemarkers_gdf.loc[dists.idxmin(), "MILE"]


for fpath in files:
    survey_id = fpath.name.replace("_SurveyPoint.gpkg", "")
    gdf = gpd.read_file(fpath)  # EPSG:3857, saved this way by read_in_surveys.py

    meta_row = metadata.loc[metadata["survey_id"] == survey_id]
    datum = str(meta_row["datum"].iloc[0]).upper() if not meta_row.empty else "UNKNOWN"

    midpoint = unary_union(gdf.geometry).centroid
    midpoint_utm = gpd.GeoSeries([midpoint], crs=gdf.crs).to_crs(UTM_CRS).iloc[0]
    gdf["segment_id"] = nearest_segment_id(midpoint_utm)

    if datum == "NAVD88":
        gdf["Z_navd88"] = gdf["Z_use"]
        gdf.to_file(NAVD88_DIR / fpath.name, driver="GPKG")
        print(f"{survey_id}: NAVD88 (passthrough)")

    elif datum == "LWRP2007":
        lwrp7_gdf["dist"] = lwrp7_gdf.geometry.distance(midpoint_utm)
        nearest = lwrp7_gdf.loc[lwrp7_gdf["dist"].idxmin()]
        if nearest["dist"] > LWRP7_DIST_THRESHOLD_M:
            print(f"[LWRP2007 FAILED] {survey_id}: nearest reference point too far ({nearest['dist']:.0f} m)")
            gdf.to_file(OTHER_DIR / f"{survey_id}_LWRP2007_FAILED.gpkg", driver="GPKG")
        else:
            gdf["Z_navd88"] = nearest["NAVD88_ft"] - gdf["Z_use"]
            gdf.to_file(NAVD88_DIR / fpath.name, driver="GPKG")
            print(f"{survey_id}: LWRP2007 -> NAVD88")

    elif datum == "LWRP2014":
        survey_mile = nearest_mile(midpoint_utm)
        up = lwrp14[lwrp14["milemarkers"] <= survey_mile]
        dn = lwrp14[lwrp14["milemarkers"] >= survey_mile]
        if up.empty or dn.empty:
            print(f"[LWRP2014 FAILED] {survey_id}: no bounding milemarkers for mile {survey_mile}")
            gdf.to_file(OTHER_DIR / f"{survey_id}_LWRP2014_FAILED.gpkg", driver="GPKG")
        else:
            m_up, m_dn = up.iloc[-1], dn.iloc[0]
            if m_up["milemarkers"] == m_dn["milemarkers"]:
                lwrp_navd88 = m_up["navd88"]
            else:
                frac = (survey_mile - m_up["milemarkers"]) / (m_dn["milemarkers"] - m_up["milemarkers"])
                lwrp_navd88 = m_up["navd88"] + frac * (m_dn["navd88"] - m_up["navd88"])
            gdf["Z_navd88"] = lwrp_navd88 - gdf["Z_use"]
            gdf.to_file(NAVD88_DIR / fpath.name, driver="GPKG")
            print(f"{survey_id}: LWRP2014 -> NAVD88 at mile {survey_mile}")

    elif datum == "ACTUALDEPTH":
        # Conversion deferred: needs a per-survey-date stream gage pull, not implemented yet.
        gdf.to_file(ACTUALDEPTH_DIR / fpath.name, driver="GPKG")
        print(f"{survey_id}: ACTUALDEPTH, conversion deferred")

    else:
        gdf.to_file(OTHER_DIR / f"{survey_id}_{datum}.gpkg", driver="GPKG")
        print(f"{survey_id}: unhandled datum '{datum}', saved to OtherDatumFiles")

"""
One-off scoped run for a specific list of surveys Molly flagged for a from-scratch
redo (2026-09-02) -- some already sit in bathym_fixed.csv but look wrong (FRIX
20230926 "doesn't look right", CARX 20230705 "not the lowest"), so this redownloads
each from eHydro fresh and reruns it through the same conversion logic as
2_read_in_surveys.py -> 3_process_surveys.py -> 4_compute_thresh_depth.py (datum
detection, NAVD88 conversion, depth_ft threshold calc).

Deliberately NOT the real pipeline stages -- this does not touch SurveyPointLayers/,
NAVD88Files/, survey_metadata.csv, lm_ids_done.csv, or bathym_fixed.csv. Molly asked
for standalone output only ("survey point layers with the thresh depths calculated
in a new folder"), skipping 5_resolve_duplicates.py and 6_review_surveys.py entirely
-- so nothing here is live on the map. Output: one gpkg per survey (SurveyPoint
schema + Z_navd88 + depth_ft + survey_type) in data/SurveysToDownload/.

Re-running is safe: always redownloads and overwrites the output file for each ID
below (that's the point -- these are known-live IDs already marked "done" upstream).
"""
import io
import zipfile
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pdfplumber
import requests
import shapely
from shapely.geometry import Point

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
OUT_DIR = DATA_DIR / "SurveysToDownload"
OUT_DIR.mkdir(exist_ok=True)
TMP_EXTRACT_DIR = DATA_DIR / "_surveystodownload_tmp"
TMP_EXTRACT_DIR.mkdir(exist_ok=True)

UTM_CRS = "EPSG:26915"
LWRP7_DIST_THRESHOLD_M = 10000

BASE_URL = "https://ehydroprod.blob.core.usgovcloudapi.net/ehydro-surveys/"
DISTRICTS_L = ['CEMVM/', 'CEMVK/', 'CEMVS/', 'CEMVK/CEMVK_DIS_', 'CEMVS/CEMVS_DIS_', 'CEMVM/CEMVM_DIS_']

# Note: MEMX_20220707 was requested twice in Molly's list -- deduped here.
SURVEY_IDS = [
    "LM_18_FRIX_20230926_CS_703_705_SORT_ACTUALDEPTHS",
    "LM_19_MEMX_20230920_CS_743_735_SORT_ACTUALDEPTHS",
    "LM_19_MEMX_20230901_CS_739_745_SORT_LWRP",
    "LM_15_CVB_20230514_CS_6098_6074",
    "LM_24_CARX_20230705_CS_849_SORT_LWRP",
    "LM_19_MEMX_20220707_CS_739_744_SORT_LWRP",
    "LM_18_FRIX_20221109_CS_705_704_SORT_ACTUALDEPTH",
]

# ---------------- support data (same files the real pipeline uses) ----------------
datums = pd.read_csv(SCRIPT_DIR / "datum_info.csv")
datums = gpd.GeoDataFrame(datums, geometry=gpd.points_from_xy(datums["LON"], datums["LAT"]), crs="EPSG:4326")
datums_utm = datums.to_crs(UTM_CRS)

lwrp7 = pd.read_csv(SCRIPT_DIR / "lwrp7_info.csv")
lwrp7_gdf = gpd.GeoDataFrame(lwrp7, geometry=gpd.points_from_xy(lwrp7["LON"], lwrp7["LAT"]), crs="EPSG:4326").to_crs(UTM_CRS)

lwrp14 = pd.read_csv(SCRIPT_DIR / "lwrp14_info.csv").sort_values("milemarkers")

milemarkers = pd.read_csv(SCRIPT_DIR / "usace_river_mile_markers.csv")
milemarkers = milemarkers[milemarkers["RIVER_NAME"].isin(["MISSISSIPPI-LO", "MISSISSIPPI-UP"])]
milemarkers_gdf = gpd.GeoDataFrame(
    milemarkers[["MILE"]], geometry=gpd.points_from_xy(milemarkers["LON"], milemarkers["LAT"]), crs="EPSG:4326"
).to_crs(UTM_CRS)


def nearest_mile(midpoint_utm):
    dists = milemarkers_gdf.geometry.distance(midpoint_utm)
    return milemarkers_gdf.loc[dists.idxmin(), "MILE"]


# ---------------- datum detection (copied from 2_read_in_surveys.py) ----------------
def get_datum_from_xyz(text: str):
    for line in text.splitlines():
        if "datum" in line.lower():
            text_line = line.strip()
            if "NAVD88" in text_line.upper():
                return "NAVD88"
            elif "2014 Low Water Reference Plane" in text_line:
                return "LWRP2014"
            elif "2007 Low Water Reference Plane" in text_line:
                return "LWRP2007"
            elif "Dredging Reference Plane" in text_line:
                return "DredgingRef"
            elif "actual depth" in text_line.lower():
                return "ACTUALDEPTH"
            else:
                return f"Unknown (found: {text_line})"
    return "Unknown"


def get_datum_from_pdf(fobj):
    try:
        with pdfplumber.open(fobj) as pdf:
            page = pdf.pages[0]
            width, height = page.width, page.height
            cropped = page.crop((width * 0.7, height * 0.7, width, height))
            text = (cropped.extract_text() or "").lower()
        if "navd88" in text:
            return "NAVD88"
        elif "dredging reference plane" in text:
            return "DredgingRef"
        elif "2014 low water reference plane" in text:
            return "LWRP2014"
        elif "2007 low water reference plane" in text:
            return "LWRP2007"
        elif "actual depth" in text:
            return "ACTUALDEPTH"
        else:
            return "Unknown"
    except Exception as e:
        print(f"  PDF reading error: {e}")
        return "Unknown"


# ---------------- survey-type classifier (copied from 3_process_surveys.py) ----------------
SPACING_SAMPLE_N = 2000
DOWNSAMPLE_THRESHOLD_PTS = 150_000
CLASSIFY_GRID_CELL_M = 20
MIN_BUFFER_M = 5.0
BUFFER_SPACING_MULT = 2.0
CLASSIFY_SIMPLIFY_M = 5
COVERAGE_RATIO_THRESH = 0.8
ALONG_REACH_FRAC_THRESH = 0.5


def _downsample_grid(gdf_utm, cell_m=CLASSIFY_GRID_CELL_M):
    gx = (gdf_utm.geometry.x // cell_m).astype(int)
    gy = (gdf_utm.geometry.y // cell_m).astype(int)
    keep = ~pd.DataFrame({"x": gx, "y": gy}).duplicated()
    return gdf_utm.loc[keep]


def _median_point_spacing(gdf_utm, sample_n=SPACING_SAMPLE_N):
    coords = np.column_stack([gdf_utm.geometry.x.values, gdf_utm.geometry.y.values])
    pts = shapely.points(coords)
    tree = shapely.STRtree(pts)
    sample = pts if len(pts) <= sample_n else pts[np.random.default_rng(0).choice(len(pts), sample_n, replace=False)]
    _, dist = tree.query_nearest(sample, exclusive=True, all_matches=False, return_distance=True)
    dist = dist[np.isfinite(dist)]
    return float(np.median(dist)) if len(dist) else 1.0


def _pca_axis(coords):
    centered = coords - coords.mean(axis=0)
    cov = np.cov(centered.T)
    eigvals, eigvecs = np.linalg.eigh(cov)
    return eigvecs[:, np.argmax(eigvals)]


def classify_survey_type(gdf_utm):
    work = gdf_utm if len(gdf_utm) <= DOWNSAMPLE_THRESHOLD_PTS else _downsample_grid(gdf_utm)
    coords = np.column_stack([work.geometry.x.values, work.geometry.y.values])
    if len(coords) < 4:
        return "sparse_lines"

    spacing_m = _median_point_spacing(work)
    buffer_m = max(BUFFER_SPACING_MULT * spacing_m, MIN_BUFFER_M)
    dissolved = work.geometry.buffer(buffer_m).unary_union
    if CLASSIFY_SIMPLIFY_M:
        dissolved = dissolved.simplify(CLASSIFY_SIMPLIFY_M)

    hull = shapely.convex_hull(shapely.multipoints(coords))
    coverage_ratio = (dissolved.area / hull.area) if hull.area > 0 else 1.0
    if coverage_ratio >= COVERAGE_RATIO_THRESH:
        return "full_coverage"

    reach_axis = _pca_axis(coords)
    proj_all = coords @ reach_axis
    reach_span = proj_all.max() - proj_all.min()
    if reach_span <= 0:
        return "cross_sections"

    pieces = list(dissolved.geoms) if hasattr(dissolved, "geoms") else [dissolved]
    tree = shapely.STRtree(shapely.points(coords))
    fracs = []
    for piece in pieces:
        idx = tree.query(piece, predicate="intersects")
        if len(idx) == 0:
            continue
        proj = coords[idx] @ reach_axis
        fracs.append((proj.max() - proj.min()) / reach_span)

    if not fracs:
        return "cross_sections"
    return "sparse_lines" if np.median(fracs) >= ALONG_REACH_FRAC_THRESH else "cross_sections"


# ---------------- per-survey pipeline ----------------
def download_zip(survey_id):
    for dist in DISTRICTS_L:
        url = f"{BASE_URL}{dist}{survey_id}.ZIP"
        try:
            r = requests.get(url, timeout=60)
        except Exception as e:
            print(f"  error fetching {url}: {e}")
            continue
        if r.status_code == 200:
            print(f"  downloaded: {url}")
            return r.content
    return None


def process_survey(survey_id):
    print(f"=== {survey_id} ===")
    zip_content = download_zip(survey_id)
    if zip_content is None:
        print(f"  FAILED: could not find ZIP for {survey_id}")
        return None

    datum_xyz, datum_pdf = "Unknown", "Unknown"
    extract_path = TMP_EXTRACT_DIR / f"{survey_id}_gdb"
    gdf = None
    with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
        xyz_files = [n for n in z.namelist() if n.lower().endswith(".xyz")]
        if xyz_files:
            with z.open(xyz_files[0]) as f:
                datum_xyz = get_datum_from_xyz(f.read().decode(errors="ignore"))
        pdf_files = [n for n in z.namelist() if n.lower().endswith(".pdf")]
        if pdf_files:
            with z.open(pdf_files[0]) as f:
                datum_pdf = get_datum_from_pdf(f)

        gdb_folders = [n for n in z.namelist() if n.endswith(".gdb/")]
        if not gdb_folders:
            print(f"  FAILED: no .gdb found in {survey_id} ZIP")
            return None
        gdb_name = gdb_folders[0]
        extract_path.mkdir(exist_ok=True)
        for file_name in z.namelist():
            if file_name.startswith(gdb_name):
                z.extract(file_name, path=extract_path)
        gdb_path = extract_path / gdb_name
        try:
            gdf = gpd.read_file(gdb_path, layer="SurveyPoint").to_crs(epsg=3857)
        except Exception as e:
            print(f"  FAILED: could not read SurveyPoint layer: {e}")
            return None

    if datum_xyz == "Unknown" and datum_pdf == "Unknown":
        datum = "Unknown"
    elif datum_xyz == "Unknown":
        datum = datum_pdf
    elif datum_pdf == "Unknown":
        datum = datum_xyz
    elif datum_xyz != datum_pdf:
        datum = datum_xyz  # prefer XYZ header on mismatch, same as stage 3's upstream priority
        print(f"  WARNING: datum mismatch XYZ={datum_xyz} / PDF={datum_pdf}, using XYZ")
    else:
        datum = datum_xyz
    print(f"  datum: {datum} (xyz={datum_xyz}, pdf={datum_pdf}), {len(gdf)} points")

    gdf_utm = gdf.to_crs(UTM_CRS)
    survey_type = classify_survey_type(gdf_utm)
    gdf["survey_type"] = survey_type
    print(f"  survey_type: {survey_type}")

    midpoint = Point(gdf.geometry.x.mean(), gdf.geometry.y.mean())
    midpoint_utm = gpd.GeoSeries([midpoint], crs=gdf.crs).to_crs(UTM_CRS).iloc[0]

    if datum == "NAVD88":
        gdf["Z_navd88"] = gdf["Z_use"]
    elif datum == "LWRP2007":
        lwrp7_gdf["dist"] = lwrp7_gdf.geometry.distance(midpoint_utm)
        nearest = lwrp7_gdf.loc[lwrp7_gdf["dist"].idxmin()]
        if nearest["dist"] > LWRP7_DIST_THRESHOLD_M:
            print(f"  FAILED: LWRP2007 nearest reference point too far ({nearest['dist']:.0f} m)")
            gdf["Z_navd88"] = np.nan
        else:
            gdf["Z_navd88"] = nearest["NAVD88_ft"] - gdf["Z_use"]
    elif datum == "LWRP2014":
        survey_mile = nearest_mile(midpoint_utm)
        up = lwrp14[lwrp14["milemarkers"] <= survey_mile]
        dn = lwrp14[lwrp14["milemarkers"] >= survey_mile]
        if up.empty or dn.empty:
            print(f"  FAILED: LWRP2014 no bounding milemarkers for mile {survey_mile}")
            gdf["Z_navd88"] = np.nan
        else:
            m_up, m_dn = up.iloc[-1], dn.iloc[0]
            if m_up["milemarkers"] == m_dn["milemarkers"]:
                lwrp_navd88 = m_up["navd88"]
            else:
                frac = (survey_mile - m_up["milemarkers"]) / (m_dn["milemarkers"] - m_up["milemarkers"])
                lwrp_navd88 = m_up["navd88"] + frac * (m_dn["navd88"] - m_up["navd88"])
            gdf["Z_navd88"] = lwrp_navd88 - gdf["Z_use"]
    elif datum == "ACTUALDEPTH":
        # Deferred per repo convention (see 3_process_surveys.py) -- needs a per-survey-date
        # gage pull, not implemented. Save raw points, no depth_ft.
        print("  ACTUALDEPTH datum -- conversion deferred, saving without depth_ft")
        gdf["Z_navd88"] = np.nan
    else:
        print(f"  unhandled datum '{datum}', saving without depth_ft")
        gdf["Z_navd88"] = np.nan

    # ---- stage-4 equivalent: threshold depth ----
    gdf_4326 = gdf.to_crs(epsg=4326)
    date = gdf_4326["SurveyDateStamp"].iloc[0] if "SurveyDateStamp" in gdf_4326.columns else None
    minx, miny, maxx, maxy = gdf_4326.total_bounds
    midpoint4326 = Point((minx + maxx) / 2, (miny + maxy) / 2)
    midpoint_utm4326 = gpd.GeoSeries([midpoint4326], crs="EPSG:4326").to_crs(UTM_CRS).iloc[0]
    datums_utm["dist"] = datums_utm.geometry.distance(midpoint_utm4326)
    nearest_row = datums_utm.loc[datums_utm["dist"].idxmin()]
    water_elev = float(nearest_row["thresh_el"])
    milemarker = nearest_row["MileMarker"]

    gdf["depth_ft"] = water_elev - gdf["Z_navd88"]
    print(f"  date={date}, milemarker={milemarker}, water_elev={water_elev:.2f} ft")
    if gdf["depth_ft"].notna().any():
        print(f"  depth_ft: min={gdf['depth_ft'].min():.2f}, max={gdf['depth_ft'].max():.2f}, mean={gdf['depth_ft'].mean():.2f}")
    else:
        print("  depth_ft: all NaN (datum conversion failed/deferred)")

    out_path = OUT_DIR / f"{survey_id}_SurveyPoint.gpkg"
    gdf.to_file(out_path, driver="GPKG")
    print(f"  saved -> {out_path}")
    return out_path


if __name__ == "__main__":
    results = {}
    for sid in SURVEY_IDS:
        results[sid] = process_survey(sid)
        print()

    print("=== SUMMARY ===")
    for sid, path in results.items():
        print(f"{sid}: {'OK -> ' + str(path.name) if path else 'FAILED'}")

    import shutil
    shutil.rmtree(TMP_EXTRACT_DIR, ignore_errors=True)

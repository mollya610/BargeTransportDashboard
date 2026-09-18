import shutil

import numpy as np
import pandas as pd
import geopandas as gpd
import shapely
from pathlib import Path
from shapely.geometry import Point

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

LWRP7_FILE = SCRIPT_DIR / "lwrp7_info.csv"
LWRP14_FILE = SCRIPT_DIR / "lwrp14_info.csv"
MILEMARKERS_FILE = SCRIPT_DIR / "usace_river_mile_markers.csv"

UTM_CRS = "EPSG:26915"
LWRP7_DIST_THRESHOLD_M = 10000  # ~6-7 miles

# ---------------- SURVEY-TYPE CLASSIFICATION ----------------
# Independent of vertical datum -- classifies the *geometric pattern* of each
# survey's point cloud into one of three USACE eHydro survey styles:
#   "full_coverage" -- entire riverbed filled with points, no line structure
#       (multibeam block surveys).
#   "cross_sections" -- many short, closely-spaced parallel lines running
#       bank-to-bank, perpendicular to the reach -- the ideal shape for
#       measuring channel width.
#   "sparse_lines" -- a handful of long lines running down the river,
#       parallel to the reach (reconnaissance/thalweg-style surveys, seen on
#       the Upper Mississippi) -- too sparse to measure width across the
#       channel.
SPACING_SAMPLE_N = 2000
DOWNSAMPLE_THRESHOLD_PTS = 150_000  # grid-downsample above this many points before classifying (dissolve/PCA would hang otherwise)
CLASSIFY_GRID_CELL_M = 20
MIN_BUFFER_M = 5.0
BUFFER_SPACING_MULT = 2.0
CLASSIFY_SIMPLIFY_M = 5
COVERAGE_RATIO_THRESH = 0.8    # dissolved-buffer-area / convex-hull-area >= this -> full_coverage
ALONG_REACH_FRAC_THRESH = 0.5  # median (piece's span along the reach axis / whole survey's span along it) >= this -> sparse_lines, else cross_sections


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
    """Unit vector along the dominant axis of a 2D point set."""
    centered = coords - coords.mean(axis=0)
    cov = np.cov(centered.T)
    eigvals, eigvecs = np.linalg.eigh(cov)
    return eigvecs[:, np.argmax(eigvals)]


def classify_survey_type(gdf_utm):
    """Returns "full_coverage", "cross_sections", or "sparse_lines" -- see
    block comment above. Runs on the full point cloud regardless of the
    NAVD88/LWRP/ACTUALDEPTH datum outcome."""
    work = gdf_utm if len(gdf_utm) <= DOWNSAMPLE_THRESHOLD_PTS else _downsample_grid(gdf_utm)
    coords = np.column_stack([work.geometry.x.values, work.geometry.y.values])
    if len(coords) < 4:
        return "sparse_lines"  # too few points to say anything else

    spacing_m = _median_point_spacing(work)
    buffer_m = max(BUFFER_SPACING_MULT * spacing_m, MIN_BUFFER_M)
    dissolved = work.geometry.buffer(buffer_m).unary_union
    if CLASSIFY_SIMPLIFY_M:
        dissolved = dissolved.simplify(CLASSIFY_SIMPLIFY_M)

    hull = shapely.convex_hull(shapely.multipoints(coords))
    coverage_ratio = (dissolved.area / hull.area) if hull.area > 0 else 1.0
    if coverage_ratio >= COVERAGE_RATIO_THRESH:
        return "full_coverage"

    # "lines" case -- a line running the length of the reach covers most of
    # the survey's own along-reach extent; a cross-section, being roughly
    # perpendicular to the reach, barely moves along it.
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


# ---------------- LOAD SUPPORT DATA ----------------
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


def nearest_mile(midpoint_utm):
    dists = milemarkers_gdf.geometry.distance(midpoint_utm)
    return milemarkers_gdf.loc[dists.idxmin(), "MILE"]


for fpath in files:
    survey_id = fpath.name.replace("_SurveyPoint.gpkg", "")
    gdf = gpd.read_file(fpath)  # EPSG:3857, saved this way by 2_read_in_surveys.py

    meta_row = metadata.loc[metadata["survey_id"] == survey_id]
    datum = str(meta_row["datum"].iloc[0]).upper() if not meta_row.empty else "UNKNOWN"

    # centroid of a MultiPoint union is just the arithmetic mean of its points --
    # computing it this way instead of unary_union(...).centroid avoids GEOS having
    # to build/dissolve a union geometry, which is impractically slow for surveys
    # with hundreds of thousands to millions of points (some LM_26_HIK surveys do)
    midpoint = Point(gdf.geometry.x.mean(), gdf.geometry.y.mean())
    midpoint_utm = gpd.GeoSeries([midpoint], crs=gdf.crs).to_crs(UTM_CRS).iloc[0]

    gdf["survey_type"] = classify_survey_type(gdf.to_crs(UTM_CRS))
    print(f"{survey_id}: survey_type={gdf['survey_type'].iloc[0]}")

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

    # every branch above has now written this survey's converted copy to NAVD88Files/
    # ActualDepthFiles/OtherDatumFiles -- the raw SurveyPointLayers copy served only as
    # this stage's input and is never read again (redo_chunk.py deletes it explicitly on
    # an intentional redo), so leaving it around just wastes disk. Confirmed 2026-08-25:
    # nothing ever cleaned this up, and it had grown to 9.4GB of stale raw survey data.
    fpath.unlink()
    gdb_dir = SURVEYPOINT_DIR / f"{survey_id}_gdb"
    if gdb_dir.exists():
        shutil.rmtree(gdb_dir)

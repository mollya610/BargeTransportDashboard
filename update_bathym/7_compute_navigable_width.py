"""
Navigable width stage: for each confirmed survey, measure the width of the
channel that is at least WIDTH_TARGET_DEPTH_FT deep, as a profile of
width-vs-station along the reach -- not a single mean depth for the whole
survey (that's 4_compute_thresh_depth.py's approach). This measures the
channel itself, from the survey points.

Surveys arrive in two shapes: full-coverage "blobs" (multibeam) and fans of
separated line transects (single-beam cross-sections), handled by two
independent pathways with different measurement approaches. Cross-section
surveys use the survey's own lines as transects (see the LINES PATHWAY
section below). Blob surveys use width_blob.py's approach: step a path
along whichever of a horizontal/vertical centerline of the survey's own
convex hull is perpendicular to the reach, hopping at each step to the
deepest nearby 300ft-averaged section (hill-climbing to find the local
cross-channel angle at each point, unless 6_review_surveys.py's per-survey
blob_path_mode override -- "horizontal"/"vertical" -- fixes that angle to the
survey's chosen starting line instead of hill-climbing it; see
width_blob.get_navigable_path), then measure width at each path chunk
by walking outward from its midpoint until the average depth in a 50ft
buffer drops below WIDTH_TARGET_DEPTH_FT. No external river polygon
(OSM/NHD) needed -- every survey already covers its own reach bank-to-bank.

Gated on 6_review_surveys.py's confirmed=="yes" flag in bathym_fixed.csv -- not just
as a safety check like 9_make_depth_polygons.py's, but a real dependency: a sign-flipped
survey's depth_ft is inverted (wet reads as dry and vice versa), so width computed
before that flip is caught and fixed would be meaningless, and this stage has no way to
tell after the fact that it ran on bad data. Reads depth_ft and survey_type straight off
each survey's gpkg (written by 4_compute_thresh_depth.py and 3_process_surveys.py
respectively) rather than re-deriving either -- UNLESS 6_review_surveys.py has left a
trim sidecar for this survey_id in TRIM_DIR, in which case that (a point SUBSET of the
same gpkg, same columns) is read instead, so a cluster of overlapping/bad points a
human excluded doesn't wreck this stage's pathfinding -- see that script's module
docstring; every other stage still uses the full survey. Must still run BEFORE
9_make_depth_polygons.py, which deletes each survey's raw NAVD88Files points once it has
processed them (as must 8_compute_width_by_stage.py, which reruns this file's own
path-finding on that same raw gpkg to recompute width at other hypothetical river
stages):

    1_check_for_surveys.py -> 2_read_in_surveys.py -> 3_process_surveys.py ->
    4_compute_thresh_depth.py -> 6_review_surveys.py (manual) ->
    7_compute_navigable_width.py -> 7b_review_navigable_path.py (manual) ->
    8_compute_width_by_stage.py -> 9_make_depth_polygons.py

Writes its result into _pending columns (vessel_path_connected_pending,
vessel_path_width_ft_pending, etc.), NOT the live vessel_path_* columns app.py reads for
risk coloring -- 7b_review_navigable_path.py is what promotes a pending result to live
(and sets path_confirmed="yes") once a human has checked that the path actually threads
through the channel sensibly. 8 and 9 both gate on path_confirmed=="yes" too, for the
same never-trust-unreviewed-output reason described above for confirmed=="yes".

Re-runs are safe: already-processed surveys (their _transects.geojson already
exists) are skipped. Does not delete anything -- 9_make_depth_polygons.py still
owns raw-file cleanup.
"""

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from shapely.geometry import LineString, Point

import width_blob
from crosssection_updated import (
    find_longest_section,
    find_deepest_point_barge_width,
    calculate_barge_width_depths,
    navigable_path_from_longest,
    combine_bidirectional_paths,
)

# ── CONFIG ────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DATA_DIR = SCRIPT_DIR / "data"
NAVD88_DIR = DATA_DIR / "NAVD88Files"
# 6_review_surveys.py's point-exclusion sidecars: same filename as the matching
# NAVD88_DIR gpkg, just a subset of its points, for path/width measurement ONLY --
# see that script's module docstring. 4_compute_thresh_depth.py, 6_review_surveys.py,
# and 9_make_depth_polygons.py all still use every point, straight off NAVD88_DIR.
TRIM_DIR = DATA_DIR / "NavigablePathTrims"
OUT_DIR = DATA_DIR / "NavigableWidth"
OUT_DIR.mkdir(parents=True, exist_ok=True)

PROFILE_FILE = REPO_ROOT / "navigable_width_profile.csv"
CLEAN_BATHYMETRY_FILE = REPO_ROOT / "bathym_fixed.csv"  # vessel-path columns (below) written back onto each survey's row here

MILEMARKERS_FILE = SCRIPT_DIR / "usace_river_mile_markers.csv"

UTM_CRS = "EPSG:26915"
FT_PER_M = 3.280839895

# depth_ft comes from 4_compute_thresh_depth.py (water_elev - Z_navd88, per
# point, persisted on the gpkg) -- that script's water_elev lookup is the
# place to change if this becomes a per-day actual-stage lookup instead of
# the static low-water reference plane.
WIDTH_TARGET_DEPTH_FT = 9.0

# dissolve_survey's buffer radius per point before dissolve -- used only for
# survey-type classification (classify_survey) and line-clustering (lines pathway's
# dissolve_survey_lines is separate, see LINE_BUFFER_M below). Deliberately NOT
# 9_make_depth_polygons.py's BUFFER_M=40 -- that's sized for visually filling gaps in a
# sparse map layer, where a systematic +2*BUFFER_M pad on every dissolved shape doesn't
# matter. It matters here (a classification boundary case, or a line-clustering
# false-merge), so the buffer is sized off the survey's own point spacing instead
# (median_point_spacing), just large enough to bridge real gaps between adjacent
# points/pings.
SIMPLIFY_M = 5
MIN_BUFFER_M = 5.0
BUFFER_SPACING_MULT = 2.0

# Survey type (full_coverage / cross_sections / sparse_lines) is normally
# read straight off the survey_type column tagged by 3_process_surveys.py.
# COVERAGE_RATIO_THRESH below is only used by the fallback classifier (buffer
# each point by a couple of point-spacings and dissolve; sparse gap-separated
# strips cover much less of their own convex hull than full-coverage does)
# for surveys processed before that tagging existed.
COVERAGE_RATIO_THRESH = 0.8
MIN_LINE_POINTS = 5  # smaller clusters are noise, not a real transect line

# Lines pathway: cross-section clustering and edge-to-edge pathfinding,
# ported from crosssection_centerline.py -- see that section below.
# Section-building buffer is a fixed distance (matching crosssection_updated.py's
# extract_cross_sections buffer_m=8) rather than dissolve_survey's point-spacing-
# scaled buffer -- individual transect lines need a buffer sized to bridge gaps
# within one line's own points, not one that grows with the whole survey's
# median spacing.
LINE_BUFFER_M = 8
LINEARITY_THRESH = 0.15          # PCA eigenvalue-ratio straightness check per cluster
COLLINEAR_ANGLE_TOL_DEG = 5.0    # merge clusters that are really one line split by the dissolve
STRAGGLER_MIN_FRAC = 0.1         # drop sections shorter than this fraction of the reach's typical length
JUMP_ANGLE_TOL_DEG = 45.0        # edge-to-edge method: how far off-perpendicular a path hop may be

# Choosing between the two path-candidate methods below: they're treated as
# comparably wide (and the longer one wins, to cover more of the reach) as
# long as the narrower one's min width is within this fraction of the wider
# one's; below that the wider path wins regardless of length.
PATH_MIN_WIDTH_TOL_FRAC = 0.75

# Blob pathway: path-stepping and width-measurement constants (SECTION_WIDTH_FT,
# SAMPLE_INTERVAL_FT, CROSS_SECTION_SPACING_FT, the 50ft hop/sampling buffers) live in
# width_blob.py itself, not here -- see that module for the algorithm.

# Same big-multibeam-survey concern as 9_make_depth_polygons.py: buffering/
# dissolving hundreds of thousands to millions of points is what hangs.
DOWNSAMPLE_THRESHOLD_PTS = 150_000
GRID_CELL_M = 20


# ── LOAD SUPPORT DATA ─────────────────────────────────────────────────────────
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


# ── GEOMETRY HELPERS ──────────────────────────────────────────────────────────

def downsample_grid(gdf_utm, cell_m=GRID_CELL_M):
    """Grid-downsample points closer than cell_m apart -- near-identical
    buffer coverage, ~100-500x fewer points. Same technique as
    9_make_depth_polygons.py, applied here whenever a survey is large enough
    that classification/footprint/threshold buffering would otherwise hang."""
    gx = (gdf_utm.geometry.x // cell_m).astype(int)
    gy = (gdf_utm.geometry.y // cell_m).astype(int)
    keep = ~pd.DataFrame({"x": gx, "y": gy}).duplicated()
    return gdf_utm.loc[keep]


def median_point_spacing(gdf_utm, sample_n=2000):
    """Median nearest-neighbor spacing among points, estimated from a sample
    for speed on large surveys. Uses shapely's STRtree directly (exclusive
    nearest-neighbor query) rather than a geopandas self sjoin_nearest, which
    has no clean way to exclude a point matching itself."""
    coords = np.column_stack([gdf_utm.geometry.x.values, gdf_utm.geometry.y.values])
    pts = shapely.points(coords)
    tree = shapely.STRtree(pts)
    if len(pts) > sample_n:
        sample = pts[np.random.default_rng(0).choice(len(pts), sample_n, replace=False)]
    else:
        sample = pts
    _, dist = tree.query_nearest(sample, exclusive=True, all_matches=False, return_distance=True)
    dist = dist[np.isfinite(dist)]
    return float(np.median(dist)) if len(dist) else 1.0


def buffer_dissolve(gdf_utm, buffer_m, simplify_m=None):
    """Buffer every point and dissolve into one (Multi)Polygon."""
    if gdf_utm.empty:
        return None
    poly = gdf_utm.geometry.buffer(buffer_m).unary_union
    if simplify_m:
        poly = poly.simplify(simplify_m)
    return poly


def explode_polygon(poly):
    if poly is None or poly.is_empty:
        return []
    return list(poly.geoms) if poly.geom_type == "MultiPolygon" else [poly]


def pca_axis(coords):
    """Unit vector along the dominant axis of a 2D point set."""
    centered = coords - coords.mean(axis=0)
    cov = np.cov(centered.T)
    eigvals, eigvecs = np.linalg.eigh(cov)
    return eigvecs[:, np.argmax(eigvals)]


def point_buffer_m(spacing_m):
    return max(BUFFER_SPACING_MULT * spacing_m, MIN_BUFFER_M)


def all_wet_runs(stations, depths, threshold):
    """Every contiguous run (start, end) along `stations` (sorted ascending) where
    `depths` >= threshold, linearly interpolating the exact crossing station between
    adjacent points where depth crosses the threshold -- the standard way to read a
    channel width off a cross-section profile. A depth of -inf (used for "no survey
    point nearby") is always below threshold. Returns every run, not just the longest
    -- a single cross-section can have more than one separate deep channel (e.g. split
    by a mid-channel shoal), and any of them might be the one that lines up with the
    next cross-section's deep water. Used by width_at_crossing (lines pathway)."""
    runs = []
    run_start = None
    run_end = None
    for i in range(len(stations) - 1):
        s0, s1 = stations[i], stations[i + 1]
        v0, v1 = depths[i] - threshold, depths[i + 1] - threshold
        if v0 >= 0 and v1 >= 0:
            if run_start is None:
                run_start = s0
            run_end = s1
        elif v0 < 0 and v1 < 0:
            if run_start is not None:
                runs.append((run_start, run_end))
                run_start = None
        elif np.isfinite(v0) and np.isfinite(v1):
            t = v0 / (v0 - v1)
            s_cross = s0 + t * (s1 - s0)
            if v0 >= 0:
                if run_start is None:
                    run_start = s0
                runs.append((run_start, s_cross))
                run_start = None
            else:
                run_start = s_cross
                run_end = s1
        else:
            if v0 >= 0 and run_start is not None:
                runs.append((run_start, s0))
            run_start = None
    if run_start is not None:
        runs.append((run_start, run_end))
    return runs


def dissolve_survey(gdf_utm):
    """Returns (dissolved_polygon, spacing_m). dissolved_polygon buffers each point by
    ~2x its own spacing and dissolves -- tight enough that a line-transect survey's
    dissolve produces one polygon part per recovered line (used by classify_survey's
    coverage-ratio check below), and a full-coverage survey's dissolve is one solid
    footprint. Only used for survey-type classification now (classify_survey) -- the
    blob pathway's own measurement (width_blob.py) doesn't use this dissolve."""
    spacing_m = median_point_spacing(gdf_utm)
    dissolved = buffer_dissolve(gdf_utm, point_buffer_m(spacing_m), SIMPLIFY_M)
    return dissolved, spacing_m


def dissolve_survey_lines(gdf_utm, buffer_m=LINE_BUFFER_M):
    """Section-building dissolve for the lines pathway (find_optimal_path) --
    fixed buffer_m, matching crosssection_updated.py's extract_cross_sections,
    instead of dissolve_survey's point-spacing-scaled buffer (used only by
    survey-type classification now)."""
    return buffer_dissolve(gdf_utm, buffer_m, SIMPLIFY_M)


def classify_survey(gdf_utm):
    """Fallback coverage-ratio classifier (blob vs lines) for surveys
    processed before 3_process_surveys.py started tagging survey_type on the
    gpkg. Returns (method, dissolved_polygon, spacing_m)."""
    dissolved, spacing_m = dissolve_survey(gdf_utm)
    hull_area = shapely.convex_hull(shapely.multipoints(
        np.column_stack([gdf_utm.geometry.x.values, gdf_utm.geometry.y.values])
    )).area
    coverage_ratio = (dissolved.area / hull_area) if hull_area > 0 else 1.0
    method = "lines" if coverage_ratio < COVERAGE_RATIO_THRESH else "blob"
    return method, dissolved, spacing_m


# ── BLOB PATHWAY ──────────────────────────────────────────────────────────────
# Ported from width_blob.py (see that module for the full algorithm): step a path
# across the reach by hopping between deepest 300ft-averaged sections, then measure
# width at each path chunk by walking outward from its midpoint until the average
# depth in a 50ft buffer drops below threshold.

def compute_blob_vessel_path(gdf_utm, blob_mode="original"):
    """Returns (vessel, rows) for a full_coverage survey, using width_blob.py's
    get_navigable_path (the path itself) and get_minimum_width (per-chunk widths along
    it) -- both reused as-is here (and again by 8_compute_width_by_stage.py, which
    reruns get_navigable_path to reproduce the SAME path deterministically, then
    recomputes each chunk's width at other depth thresholds via
    width_blob.calculate_width_at_chunk directly).

    blob_mode: "original" (default, auto-detected orientation + hill-climbed
    cross-section angle), "horizontal", or "vertical" -- see 6_review_surveys.py's
    per-blob orientation choice and width_blob.get_navigable_path's docstring.

    rows is one row per path chunk (not per path point): station_ft is the chunk
    midpoint's distance along the path, width_ft is that chunk's total width
    (width_blob's width_left + width_right), geometry is the perpendicular width line
    itself (from -width_left to +width_right around the chunk midpoint) -- same shape
    lines_qa_rows produces so main() can build the transects geojson identically for
    both pathways.

    connected is True only if every chunk's width is > 0 -- a single 0-width chunk
    means the path's deepest-section hop landed somewhere that doesn't actually clear
    WIDTH_TARGET_DEPTH_FT at its own midpoint, i.e. the "navigable" path isn't
    navigable there. break_station_ft is that chunk's station in that case;
    bottleneck_xy is always the narrowest chunk's midpoint (get_minimum_width's
    'location'), connected or not.
    """
    path_result = width_blob.get_navigable_path(gdf_utm, mode=blob_mode)
    if path_result is None or path_result["center_point"][0] is None:
        return {"connected": False, "through_width_ft": 0.0, "break_station_ft": None,
                "bottleneck_xy": None, "path_xy": None}, []

    complete_path = path_result["complete_path"]
    if len(complete_path) < 2:
        return {"connected": False, "through_width_ft": 0.0, "break_station_ft": None,
                "bottleneck_xy": None, "path_xy": None}, []

    width_result = width_blob.get_minimum_width(gdf_utm, complete_path, depth_threshold=WIDTH_TARGET_DEPTH_FT)
    if not width_result:
        return {"connected": False, "through_width_ft": 0.0, "break_station_ft": None,
                "bottleneck_xy": None, "path_xy": None}, []

    path_xy = np.array([(x, y) for x, y, _ in complete_path])
    cum_dist_m = np.concatenate([[0.0], np.cumsum(np.linalg.norm(np.diff(path_xy, axis=0), axis=1))])

    rows = []
    for i, w in enumerate(width_result["all_widths"]):
        station_m = (cum_dist_m[i] + cum_dist_m[i + 1]) / 2
        mid_x, mid_y = w["midpoint"]
        angle = w["perpendicular_angle"]
        dx, dy = np.cos(angle), np.sin(angle)
        width_left_m = w["width_left"] / FT_PER_M
        width_right_m = w["width_right"] / FT_PER_M
        rows.append({
            "station_id": i,
            "station_ft": round(station_m * FT_PER_M, 1),
            "width_ft": round(w["total_width"], 1),
            "geometry": LineString([
                (mid_x - width_left_m * dx, mid_y - width_left_m * dy),
                (mid_x + width_right_m * dx, mid_y + width_right_m * dy),
            ]),
            "lon_lat_source": Point(mid_x, mid_y),
        })
    flag_width_outliers(rows)

    connected = all(r["width_ft"] > 0 for r in rows)
    break_station_ft = None
    if not connected:
        break_station_ft = min(rows, key=lambda r: r["width_ft"])["station_ft"]

    bottleneck_xy = np.array(width_result["location"])
    vessel = {
        "connected": connected,
        "through_width_ft": round(width_result["min_total_width"], 1),
        "break_station_ft": break_station_ft,
        "bottleneck_xy": bottleneck_xy,
        "path_xy": path_xy,
        "path_method": "blob",
    }
    return vessel, rows


def flag_width_outliers(rows):
    """Mark stations whose width is way out of line with their immediate
    neighbors. Real channel width changes gradually; a spike usually means
    the transect grazed a rounded/complex corner of channel_footprint (e.g.
    near a sharp real feature like a dike field) and picked up a piece that
    runs along the padded boundary instead of cutting straight across --
    flagged for a human to check on the map rather than silently trusted."""
    widths = [r["width_ft"] for r in rows]
    for i, r in enumerate(rows):
        neighbors = widths[max(0, i - 2):i] + widths[i + 1:i + 3]
        if not neighbors:
            r["flag"] = "ok"
            continue
        local_median = float(np.median(neighbors))
        r["flag"] = "check" if widths[i] > max(3 * local_median, local_median + 500) else "ok"


# ── LINES PATHWAY ─────────────────────────────────────────────────────────────
# Section-building (cluster survey points into physical cross-section lines,
# merge dissolve-fragments of the same physical line back together) is
# ported from exploratory work in crosssection_centerline.py, and shared by
# both pathfinding methods below.
#
# Two independent methods build a candidate path across the reach, and
# find_optimal_path picks between them (see its docstring):
#   - edge-to-edge (find_optimal_path_edge_to_edge): thread a greedy
#     hop-to-deepest-point path from each of the reach's two edge sections
#     toward the other, then take whichever of the two directional
#     candidates comes out better.
#   - from-longest (find_optimal_path_from_longest): ported from
#     crosssection_updated.py's navigable_path_from_longest /
#     combine_bidirectional_paths (imported above) -- start from the deepest
#     barge-width-averaged point on the survey's longest cross-section and
#     expand outward in both directions into one path. No directional
#     candidates to score against each other within this method.
# Both hop rules are the same: a section is "reachable" only if the hop
# doesn't cross another section's line and stays within the allowed angle of
# perpendicular. "Deepest point" on a candidate section, in both methods, means
# the same barge-width-averaged depth (calculate_barge_width_depths /
# find_deepest_point_barge_width, imported above) -- the individual point's own
# depth isn't what matters to a vessel, the depth across the ~300ft it's
# centered on is.

def check_linearity(coords, threshold=LINEARITY_THRESH):
    """PCA eigenvalue-ratio straightness check -- a real cross-section is
    ~1-dimensional; a cluster that isn't (e.g. two crossing lines dissolved
    together) has a much larger minor-axis eigenvalue relative to its major
    one."""
    if len(coords) < 2:
        return True
    eigvals = np.sort(np.linalg.eigvalsh(np.cov((coords - coords.mean(axis=0)).T)))[::-1]
    ratio = eigvals[1] / eigvals[0] if eigvals[0] > 1e-10 else 0.0
    return ratio < threshold


def line_segment_intersects_line(p1, p2, line_coords):
    """Whether straight hop p1->p2 crosses another section's line (not just
    touches an endpoint) -- keeps a greedy path from jumping over a section
    instead of through it. Used by the edge-to-edge method's hop checks."""
    segment = LineString([p1, p2])
    line = LineString(line_coords)
    if not segment.intersects(line):
        return False
    inter = segment.intersection(line)
    if inter.geom_type == "LineString":
        return True
    if inter.geom_type == "Point":
        return not (np.allclose(inter.coords[0], p1) or np.allclose(inter.coords[0], p2))
    return False


def _angle_diff_deg(a, b):
    """Signed angle difference in degrees, folded to (-90, 90] -- axes and
    connecting lines are undirected (a line and its 180-degree flip are the
    same line), unlike a heading vector."""
    diff = np.degrees(a - b)
    diff = (diff + 180) % 360 - 180
    diff = (diff + 90) % 180 - 90
    return diff


def are_collinear(origin1, axis1, origin2, axis2, angle_tol_deg=COLLINEAR_ANGLE_TOL_DEG):
    """Two cross-sections are really the same physical line, split apart by
    the dissolve, if the line connecting their midpoints matches both their
    own axes within tolerance."""
    connecting = origin2 - origin1
    if np.linalg.norm(connecting) < 1e-6:
        return False
    connecting_angle = np.arctan2(connecting[1], connecting[0])
    axis1_angle = np.arctan2(axis1[1], axis1[0])
    axis2_angle = np.arctan2(axis2[1], axis2[0])
    return (abs(_angle_diff_deg(connecting_angle, axis1_angle)) <= angle_tol_deg and
            abs(_angle_diff_deg(connecting_angle, axis2_angle)) <= angle_tol_deg)


def build_sections(gdf_utm, fine_dissolved):
    """Cluster survey points into physical cross-section lines -- one
    dissolved polygon piece per line, same clustering the DP-era
    _cluster_lines used -- keep only clusters that are actually linear
    (check_linearity; a non-linear cluster is two lines dissolved into one
    blob, not a real cross-section), and order each one's points along its
    own PCA axis."""
    parts = explode_polygon(fine_dissolved)
    if not parts:
        return []

    parts_gdf = gpd.GeoDataFrame({"line_id": range(len(parts))}, geometry=parts, crs=gdf_utm.crs)
    joined = gpd.sjoin(gdf_utm, parts_gdf, how="inner", predicate="within")

    sections = []
    for line_id, grp in joined.groupby("line_id"):
        if len(grp) < MIN_LINE_POINTS:
            continue
        coords = np.column_stack([grp.geometry.x.values, grp.geometry.y.values])
        if not check_linearity(coords):
            continue
        axis = pca_axis(coords)
        origin = coords.mean(axis=0)
        order = np.argsort((coords - origin) @ axis)
        sections.append({
            "line_id": int(line_id),
            "coords": coords[order],
            "depths": grp["depth_ft"].values[order],
            "centroid": origin,
            "axis": axis,
        })
    return sections


def merge_collinear_sections(sections):
    """Merge sections that are really the same physical line split apart by
    the dissolve (are_collinear), concatenating their points and re-sorting
    along the first section's axis."""
    merged = []
    used = set()
    for i, sec1 in enumerate(sections):
        if i in used:
            continue
        group_coords, group_depths = [sec1["coords"]], [sec1["depths"]]
        for j in range(i + 1, len(sections)):
            if j in used:
                continue
            sec2 = sections[j]
            if are_collinear(sec1["centroid"], sec1["axis"], sec2["centroid"], sec2["axis"]):
                group_coords.append(sec2["coords"])
                group_depths.append(sec2["depths"])
                used.add(j)
        if len(group_coords) == 1:
            merged.append(sec1)
            continue
        all_coords = np.vstack(group_coords)
        order = np.argsort((all_coords - sec1["centroid"]) @ sec1["axis"])
        merged.append({
            "line_id": sec1["line_id"],
            "coords": all_coords[order],
            "depths": np.concatenate(group_depths)[order],
            "centroid": all_coords.mean(axis=0),
            "axis": sec1["axis"],
        })
    return merged


def filter_straggler_sections(sections):
    """Drop sections much shorter than the reach's typical (top-half-depth)
    cross-section -- dissolve/merge residue, not a real transect worth
    pathfinding through."""
    if not sections:
        return sections
    all_depths = np.concatenate([s["depths"] for s in sections])
    depth_thresh = np.percentile(all_depths, 50)
    high_depth = [s for s in sections if np.max(s["depths"]) >= depth_thresh]
    if not high_depth:
        return sections
    avg_len = np.mean([np.linalg.norm(s["coords"][-1] - s["coords"][0]) for s in high_depth])
    min_len = STRAGGLER_MIN_FRAC * avg_len
    return [s for s in sections if np.linalg.norm(s["coords"][-1] - s["coords"][0]) >= min_len]


def find_edge_sections(sections):
    """The two sections at the ends of the reach: the min- and max-position
    sections along the reach's own dominant axis (PCA of all section
    centroids)."""
    centroids = np.array([s["centroid"] for s in sections])
    reach_axis = pca_axis(centroids)
    reach_origin = centroids.mean(axis=0)
    positions = (centroids - reach_origin) @ reach_axis
    return int(np.argmin(positions)), int(np.argmax(positions)), reach_axis, reach_origin


def _hop_endpoints_clear(sections, current_idx, candidate_idx):
    """Edge-to-edge method's endpoint pre-check: the straight line between
    any endpoint of the current section and any endpoint of the candidate
    must not cross a third section, or the candidate is judged unreachable
    from here at all."""
    cur, cand = sections[current_idx]["coords"], sections[candidate_idx]["coords"]
    for p1, p2 in [(cur[0], cand[0]), (cur[0], cand[-1]), (cur[-1], cand[0]), (cur[-1], cand[-1])]:
        for j, other in enumerate(sections):
            if j in (current_idx, candidate_idx):
                continue
            if line_segment_intersects_line(p1, p2, other["coords"]):
                return False
    return True


def _hop_crosses_other_section(sections, current_idx, candidate_idx, p1, p2):
    for j, other in enumerate(sections):
        if j in (current_idx, candidate_idx):
            continue
        if line_segment_intersects_line(p1, p2, other["coords"]):
            return True
    return False


def find_navigable_path(sections, start_idx, end_idx, jump_angle_tol_deg=JUMP_ANGLE_TOL_DEG):
    """Edge-to-edge method: greedily thread a path from section start_idx to
    section end_idx, always hopping to the deepest point reachable on an
    unvisited section -- "deepest" meaning the highest barge-width-averaged
    depth (calculate_barge_width_depths: mean depth over the ~300ft the point
    is centered on), same as the from-longest method uses, not the point's own
    individual depth. A point with fewer than 150ft of section on either side
    has no averaged depth (NaN) and is never a hop target. Returns
    (path_coords, path_section_idx, path_depths); if it reaches a section with
    nowhere valid to hop to next, it stops short of end_idx (caller checks
    path_section_idx[-1] == end_idx)."""
    visited = {start_idx}
    current_idx = start_idx
    current_pt, current_depth = find_deepest_point_barge_width(
        sections[current_idx]["coords"], sections[current_idx]["depths"])

    path_section_idx = [start_idx]
    path_coords = [current_pt]
    path_depths = [current_depth]

    while current_idx != end_idx:
        axis = sections[current_idx]["axis"]
        perp_angle = np.arctan2(axis[1], axis[0]) + np.pi / 2

        reachable = []  # (section_idx, point, barge-width-averaged depth)
        for i, section in enumerate(sections):
            if i == current_idx or i in visited:
                continue
            if not _hop_endpoints_clear(sections, current_idx, i):
                continue

            barge_avg_depths = calculate_barge_width_depths(section["coords"], section["depths"])
            valid_points = []
            for pt, avg_depth in zip(section["coords"], barge_avg_depths):
                if np.isnan(avg_depth):
                    continue
                if _hop_crosses_other_section(sections, current_idx, i, current_pt, pt):
                    continue
                jump_vec = pt - current_pt
                jump_angle = np.arctan2(jump_vec[1], jump_vec[0])
                angle_diff = _angle_diff_deg(jump_angle, perp_angle)
                if abs(angle_diff) <= jump_angle_tol_deg:
                    valid_points.append((pt, avg_depth))

            if valid_points:
                pt, depth = max(valid_points, key=lambda x: x[1])
                reachable.append((i, pt, depth))

        if not reachable:
            break

        next_idx, next_pt, next_depth = max(reachable, key=lambda r: r[2])
        path_section_idx.append(next_idx)
        path_coords.append(next_pt)
        path_depths.append(next_depth)
        visited.add(next_idx)
        current_idx, current_pt, current_depth = next_idx, next_pt, next_depth

    return np.array(path_coords), path_section_idx, np.array(path_depths)


def nearest_section(local_utm, point_xy):
    """Reconstructs `local_utm`'s cross-section lines (build_sections +
    merge_collinear_sections; deliberately NOT filter_straggler_sections -- that
    heuristic needs many sections' worth of stats to mean anything, useless on a
    handful of local lines) and returns whichever one is closest to point_xy. Returns
    None if no valid section is found (e.g. too few points, or none pass
    check_linearity). Shared by 7b_review_navigable_path.py's waypoint tool
    (width_at_waypoint_on_lines) and 8_compute_width_by_stage.py's confirmed-path
    width re-measurement -- both need "which physical line is this point actually
    on" for a cross_sections survey, just from different callers/read patterns."""
    fine_dissolved = dissolve_survey_lines(local_utm)
    sections = merge_collinear_sections(build_sections(local_utm, fine_dissolved))
    if not sections:
        return None
    point = np.asarray(point_xy)
    return min(sections, key=lambda s: float(np.min(np.linalg.norm(s["coords"] - point, axis=1))))


def width_at_crossing(section, crossing_pt, threshold=WIDTH_TARGET_DEPTH_FT):
    """Width of clear (>=threshold) water at the point along `section`
    closest to where the path crosses it -- the run that contains that
    crossing station specifically, not necessarily the section's widest run
    (a shoal can split a section into more than one deep channel; only the
    one the path actually threads through counts here). Reuses
    all_wet_runs's interpolated run boundaries rather than a raw
    point-index expansion."""
    origin, axis = section["centroid"], section["axis"]
    stations = (section["coords"] - origin) @ axis  # already sorted ascending
    target_station = float((crossing_pt - origin) @ axis)
    for lo, hi in all_wet_runs(stations, section["depths"], threshold):
        if lo <= target_station <= hi:
            return (hi - lo) * FT_PER_M
    return 0.0


def evaluate_path(path_coords, path_section_idx, sections):
    """Width of clear water at every section a path crosses. Returns
    (widths_ft, min_width_ft, avg_width_ft, min_width_xy)."""
    widths = np.array([width_at_crossing(sections[idx], pt)
                        for pt, idx in zip(path_coords, path_section_idx)])
    min_idx = int(np.argmin(widths)) if len(widths) else 0
    min_width = float(widths[min_idx]) if len(widths) else 0.0
    avg_width = float(widths.mean()) if len(widths) else 0.0
    min_width_xy = path_coords[min_idx] if len(path_coords) else None
    return widths, min_width, avg_width, min_width_xy


def _path_length_m(coords):
    return float(np.sum(np.linalg.norm(np.diff(coords, axis=0), axis=1))) if len(coords) > 1 else 0.0


def find_optimal_path_edge_to_edge(sections, start_idx, end_idx, reach_axis, reach_origin):
    """Edge-to-edge method: thread a greedy hop-to-deepest-point path
    (find_navigable_path) from each of the reach's two edge sections toward
    the other, then report whichever of the two directional candidates
    comes out better -- whichever actually reaches the opposite edge if
    either does (connected=True); if neither does, whichever partial
    attempt has the better (min_width, avg_width, -length) profile, with
    break_station_ft set to where it stalled.

    Threads from BOTH edges because the greedy deepest-point hop is
    directional and can find a different route, or get stuck at a different
    dead end, depending which end it starts from.
    """
    candidates = []
    for a, b in [(start_idx, end_idx), (end_idx, start_idx)]:
        coords, sec_idx, _ = find_navigable_path(sections, a, b)
        reached = len(sec_idx) > 0 and sec_idx[-1] == b
        candidates.append({"coords": coords, "sections": sec_idx, "reached": reached})

    reached_candidates = [c for c in candidates if c["reached"]]
    connected = bool(reached_candidates)
    pool = reached_candidates if connected else candidates

    scored = []
    for c in pool:
        _, min_w, avg_w, min_xy = evaluate_path(c["coords"], c["sections"], sections)
        scored.append((min_w, avg_w, -_path_length_m(c["coords"]), c, min_xy))
    min_w, avg_w, _, best, min_xy = max(scored, key=lambda s: (s[0], s[1], s[2]))

    break_station_ft = None
    if not connected:
        break_idx = best["sections"][-1] if best["sections"] else start_idx
        break_xy = sections[break_idx]["centroid"]
        break_station_ft = round(float((break_xy - reach_origin) @ reach_axis) * FT_PER_M, 1)

    return {"connected": connected, "through_width_ft": round(min_w, 1), "avg_width_ft": round(avg_w, 1),
            "bottleneck_xy": min_xy, "break_station_ft": break_station_ft,
            "path_xy": best["coords"], "path_sections": best["sections"],
            "path_length_m": _path_length_m(best["coords"])}


def find_optimal_path_from_longest(sections, start_idx, end_idx, reach_axis, reach_origin):
    """From-longest method: start from the deepest (300ft-barge-width-
    averaged) point on the survey's longest cross-section and expand
    outward in both directions into ONE continuous path
    (crosssection_updated.py's navigable_path_from_longest /
    combine_bidirectional_paths) -- no directional candidates to score
    against each other within this method (find_optimal_path still scores
    this method's result against find_optimal_path_edge_to_edge's).

    connected is True only if the single path's sections include both of
    the reach's edge sections (start_idx/end_idx) -- i.e. the outward
    expansion reached both ends of the reach, not just wherever it happened
    to run out of reachable sections. If it stalled short of one end,
    break_station_ft is set to that end's stalled section; if it stalled
    short of both, break_station_ft only reports one of the two stall
    points (the first section in path order).
    """
    longest_idx, _ = find_longest_section(sections)
    longest_section = sections[longest_idx]
    starting_point, starting_depth = find_deepest_point_barge_width(
        longest_section["coords"], longest_section["depths"])

    bidirectional_paths = navigable_path_from_longest(
        sections, longest_idx, starting_point, starting_depth)
    combined = combine_bidirectional_paths(bidirectional_paths)

    _, min_w, avg_w, min_xy = evaluate_path(combined["coords"], combined["sections"], sections)

    connected = start_idx in combined["sections"] and end_idx in combined["sections"]

    break_station_ft = None
    if not connected:
        stalled_idx = combined["sections"][0] if start_idx not in combined["sections"] else combined["sections"][-1]
        break_xy = sections[stalled_idx]["centroid"]
        break_station_ft = round(float((break_xy - reach_origin) @ reach_axis) * FT_PER_M, 1)

    return {"connected": connected, "through_width_ft": round(min_w, 1), "avg_width_ft": round(avg_w, 1),
            "bottleneck_xy": min_xy, "break_station_ft": break_station_ft,
            "path_xy": combined["coords"], "path_sections": combined["sections"],
            "path_length_m": _path_length_m(combined["coords"])}


def find_optimal_path(gdf_utm, fine_dissolved, preferred_method=None):
    """Build this cross_sections-type survey's physical transect lines, run
    BOTH pathfinding methods (find_optimal_path_edge_to_edge,
    find_optimal_path_from_longest) over them, and pick which one's path to
    actually report.

    Picking between the two is driven by min width (through_width_ft) --
    the narrowest point either path actually threads through, i.e. the
    number that answers "how wide a vessel can this reach actually take."
    Whichever candidate has the higher through_width_ft is the "wider" one.

    Special case first: if exactly one candidate has a 0 ft min width (its
    path's narrowest crossing missed every wet run entirely) and the other
    doesn't, the non-zero one always wins outright -- a 0 ft reading isn't a
    real width to weigh against a real one, regardless of length or how
    close PATH_MIN_WIDTH_TOL_FRAC would otherwise judge them.

    Otherwise: if the narrower candidate's min width is still within
    PATH_MIN_WIDTH_TOL_FRAC (75%) of the wider one's, the two are judged
    comparably good and the LONGER of the two paths wins instead (it covers
    more of the reach for about the same width). Below that tolerance, the
    wider path wins regardless of length.

    The rejected candidate's path (rejected_path_xy) and method name
    (rejected_path_method) are returned alongside the chosen one -- both are
    saved by the main loop below (vessel_path_polyline_rejected/
    vessel_path_method_rejected in bathym_fixed.csv) so the road-not-taken is
    still available to inspect later, not just discarded.

    preferred_method overrides the automatic chosen/rejected pick above: if given and
    it names the OTHER candidate (not the one this function would have picked on its
    own), chosen and rejected are swapped before returning. Used by
    8_compute_width_by_stage.py to reproduce whichever path a human actually confirmed
    in 7b_review_navigable_path.py (which can swap to the rejected candidate) instead of
    always reproducing this function's own default pick -- 8 has no other way to learn
    about that swap, since it recomputes the path fresh from the raw gpkg every time
    rather than reading 7's stored result.
    """
    sections = filter_straggler_sections(merge_collinear_sections(build_sections(gdf_utm, fine_dissolved)))

    if len(sections) < 2:
        return {"connected": False, "through_width_ft": 0.0, "bottleneck_xy": None,
                "break_station_ft": None, "path_xy": None, "path_sections": [],
                "sections": sections, "reach_axis": None, "reach_origin": None,
                "path_method": None, "rejected_path_xy": None, "rejected_path_method": None,
                "rejected_through_width_ft": None, "rejected_connected": None,
                "rejected_bottleneck_xy": None, "rejected_break_station_ft": None}

    start_idx, end_idx, reach_axis, reach_origin = find_edge_sections(sections)

    method_a = find_optimal_path_edge_to_edge(sections, start_idx, end_idx, reach_axis, reach_origin)
    method_b = find_optimal_path_from_longest(sections, start_idx, end_idx, reach_axis, reach_origin)

    a_zero = method_a["through_width_ft"] <= 0
    b_zero = method_b["through_width_ft"] <= 0

    if a_zero != b_zero:
        if a_zero:
            chosen, chosen_name = method_b, "from_longest"
            rejected, rejected_name = method_a, "edge_to_edge"
        else:
            chosen, chosen_name = method_a, "edge_to_edge"
            rejected, rejected_name = method_b, "from_longest"
    else:
        if method_a["through_width_ft"] >= method_b["through_width_ft"]:
            wider, wider_name = method_a, "edge_to_edge"
            narrower, narrower_name = method_b, "from_longest"
        else:
            wider, wider_name = method_b, "from_longest"
            narrower, narrower_name = method_a, "edge_to_edge"

        if narrower["through_width_ft"] >= PATH_MIN_WIDTH_TOL_FRAC * wider["through_width_ft"]:
            if wider["path_length_m"] >= narrower["path_length_m"]:
                chosen, chosen_name = wider, wider_name
                rejected, rejected_name = narrower, narrower_name
            else:
                chosen, chosen_name = narrower, narrower_name
                rejected, rejected_name = wider, wider_name
        else:
            chosen, chosen_name = wider, wider_name
            rejected, rejected_name = narrower, narrower_name

    if preferred_method is not None and preferred_method == rejected_name:
        chosen, rejected = rejected, chosen
        chosen_name, rejected_name = rejected_name, chosen_name

    return {**chosen, "sections": sections, "reach_axis": reach_axis, "reach_origin": reach_origin,
            "path_method": chosen_name, "rejected_path_xy": rejected["path_xy"],
            "rejected_path_method": rejected_name,
            "rejected_through_width_ft": rejected["through_width_ft"],
            "rejected_connected": rejected["connected"],
            "rejected_bottleneck_xy": rejected["bottleneck_xy"],
            "rejected_break_station_ft": rejected["break_station_ft"]}


def lines_qa_rows(vessel):
    """Row-per-crossed-section QA rows, same shape compute_blob_vessel_path produces
    (station_id, station_ft, width_ft, geometry, lon_lat_source, flag) so
    main() can build the transects geojson identically for both pathways.
    geometry is the full cross-section line; width_ft is the width of the
    run the optimal path actually threads through it, not necessarily that
    section's widest run."""
    sections, path_xy, path_section_idx = vessel["sections"], vessel["path_xy"], vessel["path_sections"]
    if path_xy is None or len(path_xy) == 0:
        return []
    widths, _, _, _ = evaluate_path(path_xy, path_section_idx, sections)
    rows = []
    for station_id, (pt, sec_idx, width_ft) in enumerate(zip(path_xy, path_section_idx, widths)):
        section = sections[sec_idx]
        station_m = float((section["centroid"] - vessel["reach_origin"]) @ vessel["reach_axis"])
        rows.append({
            "station_id": station_id,
            "station_ft": round(station_m * FT_PER_M, 1),
            "width_ft": round(float(width_ft), 1),
            "geometry": LineString(section["coords"]),
            "lon_lat_source": Point(*pt),
            "flag": "ok",
        })
    return rows


# ── SURVEY-TYPE DISPATCH ──────────────────────────────────────────────────────

def compute_survey_vessel_path(gdf_utm, preferred_method=None, blob_mode="original"):
    """Classify a survey's shape and run whichever pathway applies, producing its
    navigable path. Factored out of main()'s loop so 8_compute_width_by_stage.py can
    call this identically to recompute width at other hypothetical river stages along
    the SAME path -- both stages run against the same still-present raw gpkg (must run
    before 9_make_depth_polygons.py deletes it), and every step here is deterministic
    (fixed-seed sampling, no randomness tied to run order), so a second call on the same
    gdf_utm reproduces the exact same path, not just an equivalent one -- UNLESS a human
    swapped to the rejected candidate in 7b_review_navigable_path.py, which
    preferred_method (lines pathway only; ignored for blob, which has no alternate
    candidate) exists to reproduce instead. See find_optimal_path's docstring.

    blob_mode (blob pathway only, ignored for lines): "original"/"horizontal"/
    "vertical" choice from 6_review_surveys.py's per-blob-survey orientation control,
    read off bathym_fixed.csv's blob_path_mode column -- see compute_blob_vessel_path
    and width_blob.get_navigable_path.

    Returns (method, vessel, rows, spacing_m) -- method is None (vessel/rows/spacing_m
    also None) for a sparse_lines survey, skip it. spacing_m is a leftover of
    survey-type classification (classify_survey's dissolve, used only when survey_type
    isn't already tagged on the gpkg) and isn't meaningful for either pathway's
    measurement step anymore -- the blob pathway (width_blob.py) uses its own fixed
    constants instead of the survey's point spacing."""
    survey_type = gdf_utm["survey_type"].iloc[0] if "survey_type" in gdf_utm.columns else None
    spacing_m = None
    if survey_type == "sparse_lines":
        return None, None, None, None
    elif survey_type == "full_coverage":
        method = "blob"
    elif survey_type == "cross_sections":
        method = "lines"
        fine_dissolved = dissolve_survey_lines(gdf_utm)
    else:
        method, fine_dissolved, spacing_m = classify_survey(gdf_utm)
        if method == "lines":
            fine_dissolved = dissolve_survey_lines(gdf_utm)

    if method == "blob":
        vessel, rows = compute_blob_vessel_path(gdf_utm, blob_mode=blob_mode)
    else:
        vessel = find_optimal_path(gdf_utm, fine_dissolved, preferred_method=preferred_method)
        rows = lines_qa_rows(vessel)
    return method, vessel, rows, spacing_m


# ── MAIN ──────────────────────────────────────────────────────────────────────

def already_done(survey_id):
    return (OUT_DIR / f"{survey_id}_transects.geojson").exists()


def main():
    files = sorted(NAVD88_DIR.glob("*_SurveyPoint.gpkg"))

    bathym_fixed = pd.read_csv(CLEAN_BATHYMETRY_FILE) if CLEAN_BATHYMETRY_FILE.exists() else None
    if bathym_fixed is not None:
        # written to _pending columns, not the live vessel_path_* columns app.py reads --
        # 7b_review_navigable_path.py copies chosen/swapped pending values into the live
        # columns (and sets path_confirmed="yes") once a human has looked at the path, so
        # an unreviewed path never affects the live map. See that script's module
        # docstring for the full gate.
        for col in ("vessel_path_connected_pending", "vessel_path_width_ft_pending",
                    "vessel_path_break_ft_pending", "vessel_path_bottleneck_lon_pending",
                    "vessel_path_bottleneck_lat_pending", "vessel_path_polyline_pending",
                    "vessel_path_method_pending", "vessel_path_polyline_rejected_pending",
                    "vessel_path_method_rejected_pending", "vessel_path_width_ft_rejected_pending",
                    "vessel_path_connected_rejected_pending", "vessel_path_bottleneck_lon_rejected_pending",
                    "vessel_path_bottleneck_lat_rejected_pending", "vessel_path_break_ft_rejected_pending",
                    "path_confirmed"):
            if col not in bathym_fixed.columns:
                bathym_fixed[col] = pd.NA
        # string-valued columns: force object dtype even if the column
        # already existed but happened to be all-NaN (read_csv then infers
        # float64, and a later string .loc assignment raises instead of
        # upcasting -- easy to hit right after a bathym_fixed.csv reset).
        for col in ("vessel_path_connected_pending", "vessel_path_polyline_pending",
                    "vessel_path_method_pending", "vessel_path_polyline_rejected_pending",
                    "vessel_path_method_rejected_pending", "vessel_path_connected_rejected_pending",
                    "path_confirmed"):
            if bathym_fixed[col].dtype != object:
                bathym_fixed[col] = bathym_fixed[col].astype(object)
    bathym_fixed_dirty = False

    # never trust depth_ft on a survey 6_review_surveys.py hasn't cleared for a
    # possible flipped sign yet -- a flip changes which points are "wet", so
    # width computed before that check is confirmed is meaningless.
    confirmed_files = set(bathym_fixed.loc[bathym_fixed["confirmed"] == "yes", "file"]) \
        if bathym_fixed is not None else set()
    todo = [f for f in files if not already_done(f.name.replace("_SurveyPoint.gpkg", ""))]
    not_yet_confirmed = [f for f in todo if f.name not in confirmed_files]
    todo = [f for f in todo if f.name in confirmed_files]
    if not_yet_confirmed:
        print(f"{len(not_yet_confirmed)} survey(s) skipped, not confirmed=yes in {CLEAN_BATHYMETRY_FILE} "
              f"(run 6_review_surveys.py first)")
    print(f"{len(todo)} survey(s) to process ({len(files)} total in NAVD88Files)")

    profile_rows = []
    for fpath in todo:
        survey_id = fpath.name.replace("_SurveyPoint.gpkg", "")
        is_lm = survey_id.upper().startswith("LM_")

        trim_path = TRIM_DIR / fpath.name
        read_path = trim_path if trim_path.exists() else fpath
        if read_path is trim_path:
            print(f"{survey_id}: using 6_review_surveys.py trim override ({trim_path.name})")
        gdf = gpd.read_file(read_path)  # EPSG:3857, written by 2_read_in_surveys.py (or 6_review_surveys.py's trimmed sidecar)
        gdf_utm = gdf.to_crs(UTM_CRS)

        if "depth_ft" not in gdf_utm.columns:
            print(f"{survey_id}: no depth_ft column (run 4_compute_thresh_depth.py on "
                  f"this survey first), skipping")
            continue

        midpoint_utm = Point(gdf_utm.geometry.x.mean(), gdf_utm.geometry.y.mean())
        mile = nearest_mile(midpoint_utm, is_lm)

        n_pts_orig = len(gdf_utm)
        if n_pts_orig > DOWNSAMPLE_THRESHOLD_PTS:
            gdf_utm = downsample_grid(gdf_utm)
            print(f"{survey_id}: downsampled {n_pts_orig} -> {len(gdf_utm)} pts before processing")

        # blob_path_mode is set per-survey by 6_review_surveys.py's orientation choice
        # (blob/full_coverage surveys only); missing/blank means a survey accepted
        # before this feature existed, or a non-blob survey -- both fall back to the
        # original auto-detected behavior.
        blob_mode = "original"
        if bathym_fixed is not None and "blob_path_mode" in bathym_fixed.columns:
            match_mode = bathym_fixed.loc[bathym_fixed["file"] == fpath.name, "blob_path_mode"]
            if not match_mode.empty and pd.notna(match_mode.iloc[0]) and str(match_mode.iloc[0]).strip():
                blob_mode = str(match_mode.iloc[0]).strip()

        method, vessel, rows, _ = compute_survey_vessel_path(gdf_utm, blob_mode=blob_mode)
        if method is None:
            print(f"{survey_id}: sparse_lines, skipping (not useful for measuring width)")
            continue

        if not rows:
            print(f"{survey_id}: {method}, no measurable transects, skipping")
            continue

        bottleneck_lon = bottleneck_lat = None
        if vessel["bottleneck_xy"] is not None:
            bottleneck_pt = gpd.GeoSeries([Point(*vessel["bottleneck_xy"])], crs=UTM_CRS).to_crs("EPSG:4326").iloc[0]
            bottleneck_lon, bottleneck_lat = round(bottleneck_pt.x, 6), round(bottleneck_pt.y, 6)

        # the vessel path itself (min-width polyline threaded across the reach), plus
        # the lines pathway's rejected candidate from the other method
        # (find_optimal_path picked between edge_to_edge and from_longest) -- both as
        # WGS84 WKT for storage in a CSV.
        path_wkt = None
        if vessel.get("path_xy") is not None and len(vessel["path_xy"]) > 1:
            path_line = LineString(vessel["path_xy"])
            path_wkt = gpd.GeoSeries([path_line], crs=UTM_CRS).to_crs("EPSG:4326").iloc[0].wkt

        rejected_path_wkt = None
        if method == "lines" and vessel.get("rejected_path_xy") is not None and len(vessel["rejected_path_xy"]) > 1:
            rejected_path_line = LineString(vessel["rejected_path_xy"])
            rejected_path_wkt = gpd.GeoSeries([rejected_path_line], crs=UTM_CRS).to_crs("EPSG:4326").iloc[0].wkt

        rejected_bottleneck_lon = rejected_bottleneck_lat = None
        if vessel.get("rejected_bottleneck_xy") is not None:
            rejected_bottleneck_pt = gpd.GeoSeries(
                [Point(*vessel["rejected_bottleneck_xy"])], crs=UTM_CRS).to_crs("EPSG:4326").iloc[0]
            rejected_bottleneck_lon = round(rejected_bottleneck_pt.x, 6)
            rejected_bottleneck_lat = round(rejected_bottleneck_pt.y, 6)

        if bathym_fixed is not None:
            match = bathym_fixed["file"] == fpath.name
            if match.any():
                bathym_fixed.loc[match, "vessel_path_connected_pending"] = "yes" if vessel["connected"] else "no"
                bathym_fixed.loc[match, "vessel_path_width_ft_pending"] = vessel["through_width_ft"]
                bathym_fixed.loc[match, "vessel_path_break_ft_pending"] = vessel["break_station_ft"]
                bathym_fixed.loc[match, "vessel_path_bottleneck_lon_pending"] = bottleneck_lon
                bathym_fixed.loc[match, "vessel_path_bottleneck_lat_pending"] = bottleneck_lat
                bathym_fixed.loc[match, "vessel_path_polyline_pending"] = path_wkt
                bathym_fixed.loc[match, "vessel_path_method_pending"] = vessel.get("path_method")
                bathym_fixed.loc[match, "vessel_path_polyline_rejected_pending"] = rejected_path_wkt
                bathym_fixed.loc[match, "vessel_path_method_rejected_pending"] = vessel.get("rejected_path_method")
                bathym_fixed.loc[match, "vessel_path_width_ft_rejected_pending"] = vessel.get("rejected_through_width_ft")
                bathym_fixed.loc[match, "vessel_path_connected_rejected_pending"] = (
                    "yes" if vessel.get("rejected_connected") else ("no" if vessel.get("rejected_connected") is not None else None)
                )
                bathym_fixed.loc[match, "vessel_path_bottleneck_lon_rejected_pending"] = rejected_bottleneck_lon
                bathym_fixed.loc[match, "vessel_path_bottleneck_lat_rejected_pending"] = rejected_bottleneck_lat
                bathym_fixed.loc[match, "vessel_path_break_ft_rejected_pending"] = vessel.get("rejected_break_station_ft")
                bathym_fixed.loc[match, "path_confirmed"] = "no"
                bathym_fixed_dirty = True
            else:
                print(f"{survey_id}: no matching row in {CLEAN_BATHYMETRY_FILE}, vessel-path result not saved")
        method_note = f" [{vessel['path_method']}]" if vessel.get("path_method") else ""
        if vessel["connected"]:
            print(f"{survey_id}: vessel path connected{method_note}, through-width {vessel['through_width_ft']:.0f} ft")
        else:
            where = f" (breaks at station {vessel['break_station_ft']:.0f} ft)" if vessel["break_station_ft"] is not None else ""
            print(f"{survey_id}: vessel path NOT connected{method_note}{where}")

        survey_date = str(gdf["SurveyDateStamp"].iloc[0])
        year = pd.to_datetime(survey_date, utc=True).year

        out_gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs=UTM_CRS)
        lon_lat = gpd.GeoSeries(out_gdf["lon_lat_source"], crs=UTM_CRS).to_crs("EPSG:4326")
        out_gdf["lon"] = lon_lat.x.round(6)
        out_gdf["lat"] = lon_lat.y.round(6)
        out_gdf = out_gdf.drop(columns=["lon_lat_source"])
        out_gdf["survey_id"] = survey_id
        out_gdf["date"] = survey_date
        out_gdf["method"] = method

        out_gdf.to_crs("EPSG:4326").to_file(OUT_DIR / f"{survey_id}_transects.geojson", driver="GeoJSON")

        # lines pathway reports one number (min width along the optimal
        # path, saved above into bathym_fixed's vessel_path_* columns), not
        # a per-station width profile -- only blob surveys feed the profile.
        if method == "blob":
            for _, r in out_gdf.iterrows():
                profile_rows.append({
                    "survey_id": survey_id,
                    "date": survey_date,
                    "year": year,
                    "mile": round(mile, 1),
                    "method": method,
                    "station_id": r["station_id"],
                    "station_ft": r["station_ft"],
                    "width_ft": r["width_ft"],
                    "flag": r["flag"],
                    "lon": r["lon"],
                    "lat": r["lat"],
                })

        widths = [r["width_ft"] for r in rows]
        print(f"{survey_id}: {method}, {len(rows)} station(s), "
              f"width {min(widths):.0f}-{max(widths):.0f} ft, mile {mile:.1f}")

    if profile_rows:
        new_profile = pd.DataFrame(profile_rows)
        if PROFILE_FILE.exists():
            existing = pd.read_csv(PROFILE_FILE)
            combined = pd.concat([existing, new_profile], ignore_index=True)
        else:
            combined = new_profile
        combined.to_csv(PROFILE_FILE, index=False)
        print(f"Added {len(new_profile)} row(s) to {PROFILE_FILE}")
    else:
        print("No new navigable-width rows to add.")

    if bathym_fixed_dirty:
        bathym_fixed.to_csv(CLEAN_BATHYMETRY_FILE, index=False)
        print(f"Updated vessel-path columns in {CLEAN_BATHYMETRY_FILE}")


if __name__ == "__main__":
    main()

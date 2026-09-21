"""
Stage 8: for each survey, recompute its navigable path's min width across a whole range
of hypothetical river stages at that survey's anchor gage -- not just the single value
at the gage's fixed low-water threshold (-10ft Memphis, -3ft St. Louis, +7ft Greenville)
that 7_compute_navigable_width.py already reports as vessel_path_width_ft.

Reuses the EXACT path 7_compute_navigable_width.py found (same crossing stations for
the lines pathway, same path chunks for the blob pathway) rather than re-threading a
new path per stage -- only the wet/dry depth cutoff changes. This is mathematically the
same as the more intuitive "add the stage offset to every point's depth, then re-check
>=9ft (WIDTH_TARGET_DEPTH_FT)" framing: adding an offset to every depth and comparing to
WIDTH_TARGET_DEPTH_FT is equivalent to leaving every point's depth_ft alone and
comparing it to (WIDTH_TARGET_DEPTH_FT - offset) instead, so this script only ever
shifts the threshold passed into width_at_crossing (lines pathway, in
7_compute_navigable_width.py) / calculate_width_at_chunk (blob pathway, in
width_blob.py) -- no depth array is ever rewritten.

Stage range tested is per anchor gage, from river_stage_history.csv:
  - low end: that gage's historical minimum stage, minus 3 ft
  - high end: that gage's 75th-percentile stage -- stages higher than that don't
    meaningfully widen the channel further, so there's no value testing all the way to
    the historical max
  - whole feet only, one row per integer stage in that range

Must run against the SAME raw gpkg 7_compute_navigable_width.py used, before
9_make_depth_polygons.py deletes it -- this reruns the same classify/dissolve/
pathfinding 7_compute_navigable_width.py does (compute_survey_vessel_path), rather than
reading back a persisted copy of it, so it can only work on a survey still sitting in
NAVD88Files/ (or, same as 7, TRIM_DIR -- if 6_review_surveys.py left a point-exclusion
sidecar for this survey_id there, it's read instead, so this stage measures the same
trimmed point set 7 did rather than re-including whatever a human excluded). Gated on
confirmed=="yes" for the same reason 7_compute_navigable_width.py
is (see its module docstring): a sign-flipped survey's depth_ft is meaningless until
6_review_surveys.py catches it. No longer gated on path_confirmed=="yes" --
7b_review_navigable_path.py's manual review step isn't part of the workflow anymore.

RETIRED (2026-09-21): this stage is no longer part of the active pipeline. Kept here as
a reference copy; renamed from 8_compute_width_by_stage.py.

    1_check_for_surveys.py -> 2_read_in_surveys.py -> 3_process_surveys.py ->
    4_compute_thresh_depth.py -> 6_review_surveys.py (manual) ->
    9_make_depth_polygons.py

Every step compute_survey_vessel_path runs is deterministic (fixed-seed sampling, no
randomness tied to run order or wall-clock time), so calling it again here on the same
gdf_utm reproduces the exact same path 7_compute_navigable_width.py found -- or the
human-swapped alternate, via preferred_method -- not just an equivalent one.

For each survey, writes to data/WidthByStage/:
  - {survey_id}_width_by_stage.csv -- one row per tested stage: stage_ft, width_ft,
    bottleneck_lon, bottleneck_lat. bottleneck_lon/lat is that stage's narrowest
    crossing STATION's coordinate (one of the path's existing, fixed stations -- not a
    newly derived pinch point within a cross-section), so "the coordinate of the
    smallest width" is just wherever that minimum row's bottleneck_lon/lat point to, and
    "the smallest width at the threshold value" is the row where stage_ft equals this
    survey's anchor-gage threshold (GAGE_THRESHOLDS) -- which should equal
    bathym_fixed.csv's vessel_path_width_ft for this survey; a mismatch beyond rounding
    is logged as a warning, since the two are computed by the same underlying functions
    and should agree exactly.
  - {survey_id}_navigable_path.geojson -- the path itself (LineString, WGS84), with
    method/connected/through_width_ft (the anchor-threshold value, matching
    bathym_fixed.csv) as properties. The whole-foot depth polygon for this survey
    already exists once 9_make_depth_polygons.py has run -- not duplicated here.

Re-runs are safe: already-processed surveys (their width_by_stage.csv already exists)
are skipped.
"""

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely import wkt
from shapely.geometry import LineString, Point

from make_combined_depth_polygons import GAGE_THRESHOLDS, _survey_gage
from importlib import import_module

import width_blob

_stage7 = import_module("OLD_compute_navigable_width")

# ── CONFIG ────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DATA_DIR = SCRIPT_DIR / "data"
NAVD88_DIR = DATA_DIR / "NAVD88Files"
# 6_review_surveys.py's point-exclusion sidecars -- same as 7_compute_navigable_width.py's
# TRIM_DIR, must be read the same way here to reproduce the SAME path (see that
# script's module docstring and this stage's own docstring above).
TRIM_DIR = DATA_DIR / "NavigablePathTrims"
OUT_DIR = DATA_DIR / "WidthByStage"
OUT_DIR.mkdir(parents=True, exist_ok=True)

STAGE_HISTORY_FILE = REPO_ROOT / "river_stage_history.csv"

UTM_CRS = _stage7.UTM_CRS
WIDTH_TARGET_DEPTH_FT = _stage7.WIDTH_TARGET_DEPTH_FT
DOWNSAMPLE_THRESHOLD_PTS = _stage7.DOWNSAMPLE_THRESHOLD_PTS

# how far below/above the historical range to test -- see module docstring
STAGE_MIN_PAD_FT = 3
STAGE_MAX_QUANTILE = 0.75


# ── STAGE RANGE PER GAGE ──────────────────────────────────────────────────────

def _build_stage_ranges():
    hist = pd.read_csv(STAGE_HISTORY_FILE)
    ranges = {}
    for gage, grp in hist.groupby("gage"):
        lo = int(np.floor(grp["stage"].min())) - STAGE_MIN_PAD_FT
        hi = int(np.floor(grp["stage"].quantile(STAGE_MAX_QUANTILE)))
        ranges[gage] = (lo, hi)
    return ranges


STAGE_RANGES = _build_stage_ranges()


# ── PER-STATION WIDTH AT A SHIFTED THRESHOLD ─────────────────────────────────

def _widths_lines(vessel, threshold):
    """Width at every station the path crosses, lines pathway -- reuses
    7_compute_navigable_width.py's width_at_crossing, which already takes threshold as
    a parameter, against the same sections/crossing points the path already found."""
    path_xy, path_sections, sections = vessel["path_xy"], vessel["path_sections"], vessel["sections"]
    if path_xy is None or len(path_xy) == 0:
        return np.array([])
    return np.array([
        _stage7.width_at_crossing(sections[idx], pt, threshold=threshold)
        for pt, idx in zip(path_xy, path_sections)
    ])


def _widths_lines_confirmed(vessel, threshold):
    """Width at every point along a CONFIRMED (possibly waypoint-corrected) lines-
    pathway path -- vessel["point_sections"] is precomputed once, outside the stage
    loop (see main()), one nearest_section per path_xy point, since section geometry
    doesn't change across stages, only the depth threshold does. Distinct from
    _widths_lines (the full-recompute case, which already has real path_sections/
    sections from compute_survey_vessel_path's own hill-climb) -- this is what makes a
    confirmed cross_sections survey's path usable here at all without re-running that
    full recompute just to get sections."""
    path_xy, point_sections = vessel["path_xy"], vessel["point_sections"]
    return np.array([
        _stage7.width_at_crossing(sec, pt, threshold=threshold) if sec is not None else 0.0
        for pt, sec in zip(path_xy, point_sections)
    ])


def _widths_blob(gdf_utm, vessel, threshold):
    """Width at every chunk along the path, blob pathway -- reuses width_blob.py's
    calculate_width_at_chunk against the SAME path chunks (consecutive pairs of points
    along vessel["path_xy"]) compute_blob_vessel_path already threaded, just with the
    threshold shifted."""
    path_xy = vessel.get("path_xy")
    if path_xy is None or len(path_xy) < 2:
        return np.array([])
    return np.array([
        width_blob.calculate_width_at_chunk(gdf_utm, path_xy[i], path_xy[i + 1], depth_threshold=threshold)["total_width"]
        for i in range(len(path_xy) - 1)
    ])


def already_done(survey_id):
    return (OUT_DIR / f"{survey_id}_width_by_stage.csv").exists()


def main():
    files = sorted(NAVD88_DIR.glob("*_SurveyPoint.gpkg"))

    # same dependency as 7_compute_navigable_width.py's own gate (see module docstring):
    # a sign-flipped survey's depth_ft is inverted until 6_review_surveys.py catches it,
    # so this can't run ahead of that check either. No longer gated on path_confirmed==
    # "yes" -- 7b_review_navigable_path.py's manual review step isn't part of the
    # workflow anymore.
    bathym_fixed = pd.read_csv(REPO_ROOT / "bathym_fixed.csv") if (REPO_ROOT / "bathym_fixed.csv").exists() else None
    if bathym_fixed is not None:
        confirmed_files = set(bathym_fixed.loc[bathym_fixed["confirmed"] == "yes", "file"])
        # the human-confirmed path's method (possibly swapped from 7's own default pick)
        # -- passed to compute_survey_vessel_path as preferred_method so this stage
        # reproduces the SAME path a person actually looked at, not whatever 7's
        # from-scratch pathfinding would pick on its own. Blob surveys have no
        # alternate candidate, so this is always None for them.
        preferred_method_by_file = dict(zip(bathym_fixed["file"], bathym_fixed.get("vessel_path_method")))
        # blob_path_mode reproduces 7_compute_navigable_width.py's per-blob-survey
        # orientation choice (6_review_surveys.py) so this stage's own
        # compute_survey_vessel_path call regenerates the SAME path, not just an
        # equivalent one -- see width_blob.get_navigable_path's mode docstring.
        blob_mode_by_file = dict(zip(bathym_fixed["file"], bathym_fixed.get("blob_path_mode")))
        # confirmed LIVE path geometry/connected/width -- used instead of a fresh
        # recompute whenever a human has manually corrected the path with waypoints in
        # 7b_review_navigable_path.py (see the uses_waypoints branch below). Read from
        # the live vessel_path_* columns, not the _pending ones, since those are what
        # 7b actually promoted after review.
        polyline_by_file = dict(zip(bathym_fixed["file"], bathym_fixed.get("vessel_path_polyline")))
        connected_by_file = dict(zip(bathym_fixed["file"], bathym_fixed.get("vessel_path_connected")))
        through_width_by_file = dict(zip(bathym_fixed["file"], bathym_fixed.get("vessel_path_width_ft")))
    else:
        confirmed_files = set()
        preferred_method_by_file = {}
        blob_mode_by_file = {}
        polyline_by_file = {}
        connected_by_file = {}
        through_width_by_file = {}
    todo = [f for f in files if not already_done(f.name.replace("_SurveyPoint.gpkg", ""))]
    not_yet_confirmed = [f for f in todo if f.name not in confirmed_files]
    todo = [f for f in todo if f.name in confirmed_files]
    if not_yet_confirmed:
        print(f"{len(not_yet_confirmed)} survey(s) skipped, not confirmed=yes in bathym_fixed.csv "
              f"(run 6_review_surveys.py first)")
    print(f"{len(todo)} survey(s) to process ({len(files)} total in NAVD88Files)")

    for fpath in todo:
        survey_id = fpath.name.replace("_SurveyPoint.gpkg", "")
        is_lm = survey_id.upper().startswith("LM_")

        trim_path = TRIM_DIR / fpath.name
        read_path = trim_path if trim_path.exists() else fpath
        gdf = gpd.read_file(read_path)  # EPSG:3857, written by 2_read_in_surveys.py (or 6_review_surveys.py's trimmed sidecar)
        gdf_utm = gdf.to_crs(UTM_CRS)

        if "depth_ft" not in gdf_utm.columns:
            print(f"{survey_id}: no depth_ft column (run 4_compute_thresh_depth.py on "
                  f"this survey first), skipping")
            continue

        midpoint_utm = Point(gdf_utm.geometry.x.mean(), gdf_utm.geometry.y.mean())
        mile = _stage7.nearest_mile(midpoint_utm, is_lm)

        n_pts_orig = len(gdf_utm)
        if n_pts_orig > DOWNSAMPLE_THRESHOLD_PTS:
            gdf_utm = _stage7.downsample_grid(gdf_utm)

        # same call 7_compute_navigable_width.py's main() makes -- deterministic, so this
        # reproduces the exact same path, except preferred_method steers it to reproduce
        # a human-swapped pick instead (see the preferred_method_by_file comment above)
        preferred_method = preferred_method_by_file.get(fpath.name)
        blob_mode = blob_mode_by_file.get(fpath.name)
        blob_mode = str(blob_mode).strip() if pd.notna(blob_mode) and str(blob_mode).strip() else "original"

        # A human may have manually corrected this path with waypoints in
        # 7b_review_navigable_path.py (apply_waypoints there sets vessel_path_method to
        # "<original>+waypoints" or "manual_waypoints"). preferred_method above only
        # steers compute_survey_vessel_path's automatic chosen/rejected pick for the
        # LINES pathway -- for blob-pathway surveys it's ignored entirely (see that
        # function's docstring), so recomputing from scratch here would silently throw
        # away the waypoint correction and reproduce the ORIGINAL, uncorrected
        # hill-climbed path instead (2026-08-31, caught when Molly asked for this
        # stage's output on a waypoint-corrected survey and the numbers didn't match
        # what she'd confirmed). Read the confirmed polyline straight off
        # bathym_fixed.csv's LIVE vessel_path_polyline instead whenever waypoints are
        # involved -- correct for ANY original pathway, since apply_waypoints always
        # re-measures width via width_blob.calculate_width_at_chunk regardless of
        # whether the path started as "lines" or "blob", so a waypoint-corrected path
        # is always chunk-based from that point on.
        #
        # 2026-08-31 (same day, broadened): also skip recompute for a PLAIN blob survey
        # (method=="blob", no waypoints at all) -- Molly flagged that re-threading the
        # hill-climbing path from scratch here is far too slow to be worth it when the
        # confirmed path is already sitting right there in bathym_fixed.csv, unchanged.
        # width_blob.calculate_width_at_chunk only needs path_xy + gdf_utm (see
        # _widths_blob above) -- no dependency on anything compute_blob_vessel_path's
        # own hill-climbing search produces beyond the path geometry itself, so reusing
        # the confirmed polyline is exactly as correct as recomputing for a blob survey,
        # just fast. Recompute is still needed for the LINES pathway ("edge_to_edge"/
        # "from_longest") -- _widths_lines needs the per-crossing `sections`/
        # `path_sections` objects compute_survey_vessel_path builds, which aren't
        # persisted anywhere, so there's no way to skip that recompute without also
        # persisting those (not done -- lines-pathway surveys aren't part of the current
        # 2022/2023-list batches this stage is being run against).
        use_confirmed_path = isinstance(preferred_method, str) and (
            preferred_method == "blob" or "waypoints" in preferred_method)
        if use_confirmed_path:
            polyline_wkt = polyline_by_file.get(fpath.name)
            if not isinstance(polyline_wkt, str) or not polyline_wkt.strip():
                print(f"{survey_id}: method={preferred_method!r} but no confirmed "
                      f"vessel_path_polyline in bathym_fixed.csv, skipping")
                continue
            line_utm = gpd.GeoSeries([wkt.loads(polyline_wkt)], crs="EPSG:4326").to_crs(UTM_CRS).iloc[0]
            path_xy = np.array(line_utm.coords)
            if len(path_xy) < 2:
                print(f"{survey_id}: confirmed path has <2 vertices, skipping")
                continue
            # Which pathway this survey actually is -- "waypoints" in preferred_method
            # doesn't imply blob (2026-09-01: a cross_sections survey can get waypoint-
            # corrected too, via 7b_review_navigable_path.py's lines-mode waypoint tool).
            # A blob-style calculate_width_at_chunk scan on a cross_sections survey's
            # path finds nothing off its sparse lines' own strips -- confirmed bug,
            # reported 0ft at every stage for LM_11_YBS_20230629/LM_05_VGG_20230817 the
            # first time this ran without the check below.
            survey_type = gdf_utm["survey_type"].iloc[0] if "survey_type" in gdf_utm.columns else None
            method = "lines" if survey_type == "cross_sections" else "blob"
            vessel = {
                "path_xy": path_xy,
                "connected": str(connected_by_file.get(fpath.name, "")).strip().lower() == "yes",
                "through_width_ft": through_width_by_file.get(fpath.name),
            }
            if method == "lines":
                # No persisted sections/path_sections for a confirmed path (those only
                # exist mid-hill-climb, in compute_survey_vessel_path) -- reconstruct
                # once here (cheap: section geometry doesn't depend on stage/threshold,
                # only the depth comparison does) rather than per-stage. See
                # _widths_lines_confirmed and 7_compute_navigable_width.py's
                # nearest_section (shared with 7b's own waypoint width tool).
                vessel["point_sections"] = [_stage7.nearest_section(gdf_utm, tuple(pt)) for pt in path_xy]
                n_missing_section = sum(1 for s in vessel["point_sections"] if s is None)
                if n_missing_section:
                    print(f"{survey_id}: {n_missing_section}/{len(path_xy)} path points found no "
                          f"nearby cross-section line (will read as 0ft width at every stage)")
            # one placeholder row per chunk (chunk midpoint), matching what
            # compute_blob_vessel_path's own rows look like -- the only field the code
            # below actually reads off `rows` is lon_lat_source (station coordinates).
            # Used for method=="blob" station points below; method=="lines" uses
            # vessel["path_xy"] directly instead (see the station_pts branch below).
            rows = [
                {"lon_lat_source": Point((path_xy[i][0] + path_xy[i + 1][0]) / 2,
                                          (path_xy[i][1] + path_xy[i + 1][1]) / 2)}
                for i in range(len(path_xy) - 1)
            ]
        else:
            method, vessel, rows, _ = _stage7.compute_survey_vessel_path(
                gdf_utm, preferred_method=preferred_method, blob_mode=blob_mode)
            if method is None:
                print(f"{survey_id}: sparse_lines, skipping")
                continue
            if not rows:
                print(f"{survey_id}: {method}, no measurable transects, skipping")
                continue

        gage = _survey_gage(survey_id, mile)
        anchor_stage = GAGE_THRESHOLDS[gage]
        if gage not in STAGE_RANGES:
            print(f"{survey_id}: no {gage} history in {STAGE_HISTORY_FILE}, skipping")
            continue
        lo, hi = STAGE_RANGES[gage]

        # station coordinates are fixed regardless of stage -- convert once, reused for
        # every tested stage's bottleneck lookup
        if method == "lines":
            path_xy = vessel["path_xy"]
            station_pts = [Point(*p) for p in path_xy] if path_xy is not None and len(path_xy) else []
        else:
            station_pts = [r["lon_lat_source"] for r in rows]
        if not station_pts:
            print(f"{survey_id}: {method}, no path stations, skipping")
            continue
        station_lonlat = gpd.GeoSeries(station_pts, crs=UTM_CRS).to_crs("EPSG:4326")

        table_rows = []
        anchor_row_width = None
        for stage in range(lo, hi + 1):
            offset = stage - anchor_stage
            threshold = WIDTH_TARGET_DEPTH_FT - offset  # see module docstring for the equivalence
            if method == "lines" and "point_sections" in vessel:
                widths = _widths_lines_confirmed(vessel, threshold)
            elif method == "lines":
                widths = _widths_lines(vessel, threshold)
            else:
                widths = _widths_blob(gdf_utm, vessel, threshold)
            if len(widths) == 0:
                continue
            min_idx = int(np.argmin(widths))
            width_ft = round(float(widths[min_idx]), 1)
            table_rows.append({
                "stage_ft": stage,
                "width_ft": width_ft,
                "bottleneck_lon": round(float(station_lonlat.x.iloc[min_idx]), 6),
                "bottleneck_lat": round(float(station_lonlat.y.iloc[min_idx]), 6),
            })
            if stage == anchor_stage:
                anchor_row_width = width_ft

        if not table_rows:
            print(f"{survey_id}: no stage rows produced, skipping")
            continue

        # sanity check: the anchor-threshold row should reproduce 7_compute_navigable_width.py's
        # own through_width_ft exactly (both derived from the same functions/inputs)
        through_width_ft = vessel.get("through_width_ft")
        if anchor_row_width is not None and through_width_ft is not None \
                and abs(anchor_row_width - through_width_ft) > 0.1:
            print(f"{survey_id}: WARNING anchor-stage width {anchor_row_width} ft != "
                  f"vessel_path_width_ft {through_width_ft} ft")

        pd.DataFrame(table_rows).to_csv(OUT_DIR / f"{survey_id}_width_by_stage.csv", index=False)

        path_line = LineString(station_pts) if len(station_pts) > 1 else None
        path_gdf = gpd.GeoDataFrame(
            [{
                "survey_id": survey_id,
                "method": method,
                "connected": "yes" if vessel.get("connected") else "no",
                "through_width_ft": through_width_ft,
                "gage": gage,
                "anchor_stage_ft": anchor_stage,
                "geometry": path_line if path_line is not None else Point(station_pts[0]),
            }],
            crs=UTM_CRS,
        ).to_crs("EPSG:4326")
        path_gdf.to_file(OUT_DIR / f"{survey_id}_navigable_path.geojson", driver="GeoJSON")

        min_row = min(table_rows, key=lambda r: r["width_ft"])
        print(f"{survey_id}: {method}, {gage} gage, stages {lo} to {hi} ft "
              f"({len(table_rows)} rows), worst width {min_row['width_ft']:.0f} ft "
              f"at stage {min_row['stage_ft']} ft")


if __name__ == "__main__":
    main()

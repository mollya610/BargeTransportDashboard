"""
Local QA tool: confirm the navigable path 7_compute_navigable_width.py picked before it
reaches the live map or 8_compute_width_by_stage.py/9_make_depth_polygons.py.

7_compute_navigable_width.py writes its result into _pending columns on bathym_fixed.csv
(vessel_path_connected_pending, vessel_path_width_ft_pending, vessel_path_polyline_pending,
etc. -- see that script's module docstring), NOT the live vessel_path_* columns app.py
reads for risk coloring. This tool is what promotes a pending result to live: for each
survey with confirmed=="yes" and path_confirmed=="no", it plots the survey's points
(colored by depth, same style as 6_review_surveys.py) with the computed path overlaid, so
a human can sanity-check that the path actually threads through the channel and the
bottleneck marker lands somewhere sensible -- an automatically-picked path can look
obviously wrong (e.g. cutting across a bend, or landing its narrowest point on a stray
outlier) in a way that's easy to catch by eye but hard to catch algorithmically.

For a lines-pathway survey, 7 already computed and stored a REJECTED alternate candidate
(find_optimal_path picked between edge_to_edge and from_longest) -- both are plotted
together here, and "Swap to rejected candidate" lets a reviewer promote the other one
instead if it actually looks better. Blob-pathway surveys have no alternate candidate
(width_blob.py's algorithm produces one path), so that control is simply hidden for them.

Confirm copies the selected candidate's _pending fields into the live vessel_path_*
columns (recording whichever one was NOT selected as the new vessel_path_*_rejected, so
the road-not-taken is still on record either way) and sets path_confirmed="yes" --
8_compute_width_by_stage.py reproduces this exact choice via compute_survey_vessel_path's
preferred_method argument (it recomputes the path fresh from the raw gpkg every stage 8
run, rather than reading back 7's stored result, so it has no other way to learn about a
swap). Save for Later moves the survey's raw gpkg to SURVEYS_TO_REVIEW_DIR (shared with
6_review_surveys.py's own "Save for Later", living outside NAVD88Files/ -- see that
script's module docstring) and sets path_confirmed="deferred" -- excluded from both 8/9
and this tool's own queue until someone looks at it by hand.

    1_check_for_surveys.py -> 2_read_in_surveys.py -> 3_process_surveys.py ->
    4_compute_thresh_depth.py -> 6_review_surveys.py (manual) ->
    7_compute_navigable_width.py -> 7b_review_navigable_path.py (manual, this file) ->
    8_compute_width_by_stage.py -> 9_make_depth_polygons.py

Local-only tool, never deployed -- runs on its own port, separate from app.py and from
6_review_surveys.py.
"""

from pathlib import Path
from importlib import import_module

import geopandas as gpd
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import shapely.wkt
from dash import Dash, dcc, html, Input, Output, State, no_update
from shapely.geometry import box, LineString, Point

import width_blob
_stage7 = import_module("OLD_compute_navigable_width")

# ---------------- CONFIG ----------------
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
NAVD88_DIR = SCRIPT_DIR / "data" / "NAVD88Files"
# deliberately NOT inside NAVD88_DIR -- shared with 6_review_surveys.py's own
# "Save for Later" (see this script's module docstring)
SURVEYS_TO_REVIEW_DIR = SCRIPT_DIR / "data" / "SurveysToReview"
BATHYM_FIXED_FILE = REPO_ROOT / "bathym_fixed.csv"

# Temporary: restrict the review queue to only survey_ids on a given list. Same idea as
# 6_review_surveys.py's own RESTRICT_TO_SURVEY_LIST_FILE. The 2023 recheck batch this was
# last pointed at is done (see project_2023_recheck_batch_resolved memory) and its
# scratch list file is gone, so this is back to None -- reviewing every pending path.
RESTRICT_TO_SURVEY_LIST_FILE = None

# Temporary, one-off recheck (2026-08-31): Molly wants to re-look at every 2022-list
# survey whose through-width came back 0 -- including ones she already clicked Confirm
# or Save for Later on, which the normal pending_mask below excludes (path_confirmed
# already "yes"/"deferred"). The _pending columns are never cleared by either action
# (see confirm_path/save_for_later), so these are still fully renderable -- this set
# just widens the queue to include them too, regardless of path_confirmed. Clear this
# set (back to []) once she's done rechecking; it's not meant to be a permanent queue
# addition.
# Empty as of 2026-08-31 -- every survey that was ever in this set has now been
# rechecked/confirmed (see the NOT-included notes below for each one's resolution).
# Kept as a set() rather than deleted so the same override pattern is ready to reuse
# next time Molly wants to force a batch of already-confirmed surveys back into the
# queue for a recheck.
# NOT included: LM_25_MADX_20220331_CS_917_904_SORT_LWRP_B_INTERP -- same story,
    # hit 2026-08-31: re-interpolated after excluding a 14.7km-distant stray point
    # (was ballooning the hull), stage 7 re-run, then confirmed (path_confirmed="yes",
    # live vessel_path_* columns match the corrected _pending values, width 1115.5ft).
    # Removed, same fix.
    # NOT included: LM_18_FRIX_20220719_CS_651_653_SORT_LWRP_INTERP -- Molly rechecked
    # it 2026-08-31, its live path/width data was already correct (0 width, genuinely
    # not connected -- matches vessel_path_method="blob+waypoints", meaning a prior
    # manual waypoint pass already tried to fix it and this is the real result), and
    # this force-include set was the ONLY reason it kept reappearing after Confirm --
    # NOT a real UI bug, just this override outliving her actual decision. Removed once
    # confirmed so it stops re-entering the queue.
    # NOT included: LM_23_OSCX_20220629_CS_796_791_SORT_LWRP_INTERP -- same story, hit
    # 2026-08-31: already confirmed (path_confirmed="yes", live vessel_path_* columns
    # match the corrected _pending values, width 918.6ft) but kept reappearing/"stuck"
    # in the app because this override doesn't check path_confirmed. Removed, same fix.
    # NOT included: LM_19_MEMX_20220804_CS_758_759_SORT_LWRP_INTERP -- same story, hit
    # 2026-08-31: already confirmed (path_confirmed="yes", live vessel_path_* columns
    # match the corrected _pending values, width 787.4ft) but kept reappearing/"stuck"
    # in the app for the same reason. Removed, same fix.
    # NOT included: LM_09_LPM_20220622_CS_4992_4966 also had width 0, but Molly had it
    # deleted entirely 2026-08-31 (bathym_fixed.csv row, survey list row, transects
    # output, raw gpkg all removed, and marked done in lm_ids_done.csv so it's never
    # auto-redownloaded) -- nothing left to review.
    # NOT included: LM_08_MVF_20220404_CS_MB_4950_4800 also has width 0, but that's
    # already a resolved, deliberately-confirmed real finding (see
    # project_lm08_merge_gaps memory -- a genuinely shallow ~8.8ft spot under the 9ft
    # target, not a bug) and its raw gpkg is gone (stage 9 already ran and cleaned it
    # up per normal behavior) -- nothing left in NAVD88Files/ for 7b to even render.
# NOT included: LM_03_KGC_20220628_CS_3800_3742 -- rechecked 2026-08-31 (still 0
# through-width, break at 551.2ft after a manual_waypoints pass), Molly opted to Save
# for Later rather than fix it now (path_confirmed="deferred", raw gpkg back in
# SurveysToReview/). Removed from this set so it stays deferred instead of forcing
# back into the queue on top of that -- re-add if she wants another look.
WIDTH_ZERO_RECHECK_FILES = {
    # LM_18_FRIX_20220719_CS_651_653_SORT_LWRP_INTERP: already confirmed
    # (path_confirmed="yes", method="blob+waypoints", width 0, break at 10238.4ft) from
    # an earlier recheck pass, but Molly wants another look (2026-08-31) to double check
    # whether 0 is really right.
    "LM_18_FRIX_20220719_CS_651_653_SORT_LWRP_INTERP_SurveyPoint.gpkg",
}

UTM_CRS = "EPSG:26915"

# same triage-not-precision-reading colorscale as 6_review_surveys.py
DEPTH_COLOR_MIN = 5
DEPTH_COLOR_MAX = 25
DEPTH_COLORSCALE = "RdYlBu"

CHOSEN_COLOR = "#39ff6a"
REJECTED_COLOR = "#ffa500"
WAYPOINT_COLOR = "#00e5ff"

FT_PER_M = _stage7.FT_PER_M
WIDTH_TARGET_DEPTH_FT = _stage7.WIDTH_TARGET_DEPTH_FT
OUT_DIR = _stage7.OUT_DIR
PROFILE_FILE = _stage7.PROFILE_FILE

# Manual path-correction control (module docstring addendum, added 2026-08-31): when a
# blob survey's computed path goes off course partway through, drawing a box/lasso near
# where it SHOULD go places a waypoint there -- see add_waypoint()/deepest_point_near()
# below for how the click gets snapped to an actual channel point, and apply_waypoints()
# for how the waypoint chain gets spliced into the chosen path and re-measured. Radius
# is deliberately small (just picks the right point, not an area to average over) --
# unrelated to calculate_width_at_chunk's own much larger up-to-500m width-measurement
# scan, which apply_waypoints separately gives a wide margin for (see
# WIDTH_SCAN_MARGIN_FT below).
WAYPOINT_RADIUS_FT = 150.0
# calculate_width_at_chunk (width_blob.py) scans outward up to 500m from each chunk
# midpoint -- the bbox read for local points around a new/replaced chunk needs to
# comfortably cover that scan distance in every direction, not just the 150ft used to
# place the waypoint itself.
WIDTH_SCAN_MARGIN_M = 500 + 50


# ---------------- DATA ----------------

def load_bathym_fixed():
    return pd.read_csv(BATHYM_FIXED_FILE)


def get_pending_files():
    """Surveys awaiting path review: confirmed=="yes", path_confirmed=="no" (or unset --
    an older row from before this column existed), pending path data actually present
    (7_compute_navigable_width.py has run on it), and its raw gpkg still on disk (not yet
    moved to SURVEYS_TO_REVIEW_DIR by a previous "Save for Later"). When
    RESTRICT_TO_SURVEY_LIST_FILE is set, further narrowed to survey_ids on that list."""
    df = load_bathym_fixed()
    if "path_confirmed" not in df.columns or "vessel_path_connected_pending" not in df.columns:
        return []
    pending_mask = (df["confirmed"] == "yes") & (df["path_confirmed"].fillna("no") == "no") \
        & df["vessel_path_connected_pending"].notna()
    pending_mask = pending_mask | (df["file"].isin(WIDTH_ZERO_RECHECK_FILES) & df["vessel_path_connected_pending"].notna())
    pending = df.loc[pending_mask, "file"].tolist()
    present = [f for f in pending if (NAVD88_DIR / f).exists()]
    if RESTRICT_TO_SURVEY_LIST_FILE is not None:
        allowed_ids = set(pd.read_csv(RESTRICT_TO_SURVEY_LIST_FILE)["survey_id"])
        present = [f for f in present if f.replace("_SurveyPoint.gpkg", "") in allowed_ids]
    return present


# full_coverage (multibeam) surveys can carry hundreds of thousands of points -- a
# sparse sample is still plenty dense to see how the path threads through the channel,
# but cuts both the read and the Plotly render proportionally. Pushed down into the
# gpkg read itself (a `where` clause on SQLite's built-in rowid) rather than reading
# everything and then subsampling in pandas -- same fix as 6_review_surveys.py's, see
# that script's comment for the read-time numbers. Was 300 (15x sparser than an
# earlier 20) for genuinely huge merged multi-survey blobs at 2M+ points; dropped to
# 6 (50x denser than 300) 2026-08-31 for the interpolated 2022-list batch -- those
# max out around 170k points (nowhere near the merged-blob scale that justified 300),
# and 300 was too sparse to actually see the real-vs-interpolated point distinction
# added below.
MULTIBEAM_DOWNSAMPLE_FACTOR = 6
MULTIBEAM_DOWNSAMPLE_WHERE = f"rowid % {MULTIBEAM_DOWNSAMPLE_FACTOR} = 0"


def load_survey_points(file):
    """Raw survey points for `file`, reprojected to UTM -- depth_ft is already on the
    gpkg (written by 4_compute_thresh_depth.py), unlike 6_review_surveys.py this tool
    never previews a sign flip, so no recompute needed here."""
    peek = gpd.read_file(NAVD88_DIR / file, rows=1)
    survey_type = peek["survey_type"].iloc[0] if "survey_type" in peek.columns else None
    where = MULTIBEAM_DOWNSAMPLE_WHERE if survey_type == "full_coverage" else None
    gdf = gpd.read_file(NAVD88_DIR / file, where=where)  # EPSG:3857
    return gdf.to_crs(UTM_CRS)


def _line_to_utm(wkt_str):
    if pd.isna(wkt_str) or not str(wkt_str).strip():
        return None
    line = shapely.wkt.loads(wkt_str)
    return gpd.GeoSeries([line], crs="EPSG:4326").to_crs(UTM_CRS).iloc[0]


def _point_to_utm(lon, lat):
    if pd.isna(lon) or pd.isna(lat):
        return None
    return gpd.GeoSeries([Point(lon, lat)], crs="EPSG:4326").to_crs(UTM_CRS).iloc[0]


def candidate_info(row, suffix):
    """Pull one candidate's fields off `row` -- suffix is "_pending" for the chosen
    candidate or "_rejected_pending" for the rejected one. Returns None if that
    candidate doesn't exist (e.g. no rejected candidate for a blob-pathway survey)."""
    polyline_col = "vessel_path_polyline" + suffix
    if polyline_col not in row or pd.isna(row.get(polyline_col)):
        return None
    return {
        "connected": str(row.get("vessel_path_connected" + suffix, "")).strip().lower() == "yes",
        "width_ft": row.get("vessel_path_width_ft" + suffix),
        "break_ft": row.get("vessel_path_break_ft" + suffix),
        "method": row.get("vessel_path_method" + suffix),
        "polyline": row.get(polyline_col),
        "bottleneck_lon": row.get("vessel_path_bottleneck_lon" + suffix),
        "bottleneck_lat": row.get("vessel_path_bottleneck_lat" + suffix),
    }


# ---------------- WAYPOINT CORRECTION ----------------
# See CONFIG's WAYPOINT_RADIUS_FT/WIDTH_SCAN_MARGIN_M comment for the two different
# search radii involved here.

def _bbox_3857_around_utm(center_xy, half_extent_m):
    """A (minx, miny, maxx, maxy) bbox in EPSG:3857 (the raw gpkg's own storage CRS)
    covering a `half_extent_m` square around a UTM point -- for a targeted
    gpd.read_file(bbox=...) that only pulls the local points near a waypoint,
    independent of the survey's overall file size (this is what makes waypoint
    correction tractable on a 786MB/2.37M-point merged blob: no full-file read)."""
    box_utm = box(center_xy[0] - half_extent_m, center_xy[1] - half_extent_m,
                   center_xy[0] + half_extent_m, center_xy[1] + half_extent_m)
    return gpd.GeoSeries([box_utm], crs=UTM_CRS).to_crs("EPSG:3857").iloc[0].bounds


def deepest_point_near(file, center_xy):
    """The actual survey point within WAYPOINT_RADIUS_FT of `center_xy` (UTM) with the
    greatest depth_ft -- what a drawn waypoint region snaps to. Returns (x, y, depth_ft)
    in UTM, or None if no survey points fall within the radius."""
    radius_m = WAYPOINT_RADIUS_FT / FT_PER_M
    bbox = _bbox_3857_around_utm(center_xy, radius_m + 30)
    gdf = gpd.read_file(NAVD88_DIR / file, bbox=bbox, columns=["depth_ft"])
    if gdf.empty:
        return None
    gdf_utm = gdf.to_crs(UTM_CRS)
    dist = np.hypot(gdf_utm.geometry.x.values - center_xy[0], gdf_utm.geometry.y.values - center_xy[1])
    within = dist <= radius_m
    if not within.any():
        return None
    sub = gdf_utm.loc[within]
    best = sub["depth_ft"].values.argmax()
    pt = sub.geometry.iloc[best]
    return (pt.x, pt.y, float(sub["depth_ft"].iloc[best]))


def width_at_waypoint_on_lines(file, point_xy, threshold=WIDTH_TARGET_DEPTH_FT, margin_m=WIDTH_SCAN_MARGIN_M):
    """Lines-pathway equivalent of width_blob.calculate_width_at_chunk for the waypoint
    tool. A cross_sections survey's points only exist along discrete physical lines --
    calculate_width_at_chunk's isotropic outward scan (built for a 2D blob's continuous
    coverage) would mostly find nothing off a narrow line's own strip, since there's no
    survey data between lines to scan through. Width at a waypoint here has to be the
    length of the >=threshold-deep run along whichever cross-section LINE the waypoint
    actually sits on -- the same measurement 7_compute_navigable_width.py's own
    width_at_crossing makes for the normal (non-waypoint) lines pathway, just evaluated
    at a hand-placed point instead of a hill-climbed path crossing.

    Reads a local bbox around the point (not the whole survey -- same reasoning as
    apply_waypoints' _local_points_near) and hands it to 7_compute_navigable_width.py's
    nearest_section (shared with 8_compute_width_by_stage.py's own confirmed-path
    width re-measurement, see that module) -- the click was already snapped to a real
    survey point by deepest_point_near, so "closest section" should be "the section
    that point is actually on" barring two lines running unusually close together.

    Returns (width_ft, section_line) -- section_line is the FULL cross-section line's
    LineString (matches lines_qa_rows' own geometry convention for a normal transects
    row, not a synthesized width segment). Returns (0.0, None) if no section is found
    at all near the point (margin_m too small for an unusually wide reach, or
    build_sections' linearity/min-point-count filters rejected everything local)."""
    bbox_3857 = _bbox_3857_around_utm(point_xy, margin_m)
    local = gpd.read_file(NAVD88_DIR / file, bbox=bbox_3857, columns=["depth_ft"])
    if local.empty:
        return 0.0, None
    local_utm = local.to_crs(UTM_CRS)
    nearest = _stage7.nearest_section(local_utm, point_xy)
    if nearest is None:
        return 0.0, None
    width_ft = _stage7.width_at_crossing(nearest, np.asarray(point_xy), threshold=threshold)
    return width_ft, LineString(nearest["coords"])


def splice_waypoints(path_xy, waypoints_xy, keep_side="head"):
    """Where a waypoint chain gets grafted onto (or replaces) an existing path.

    keep_side="head": finds the path vertex nearest the FIRST waypoint, keeps the path
    from its start up to and including that vertex, then appends the waypoints in
    order -- everything past that vertex in the original path is dropped. Use this
    when the path was fine at the start and goes wrong later on.

    keep_side="tail": finds the path vertex nearest the LAST waypoint, keeps the path
    from that vertex to its end, and PREPENDS the waypoints (in order) before it --
    everything before that vertex in the original path is dropped. Use this when the
    path goes wrong near the start and is fine again later.

    keep_side="full": ignores path_xy entirely -- the waypoint chain IS the new path,
    start to finish. Use this when the auto-computed path isn't worth correcting at
    all and the whole route should just be walked by hand (each waypoint already
    snapped to the deepest nearby point by deepest_point_near -- this just connects
    them in the order placed). Returns nearest_idx=None in this mode since there's no
    old-path vertex being matched.

    "head"/"tail" are one-sided corrections, not a reconnect-both-ends splice -- to
    rejoin more of the original route on the discarded side, draw waypoints the rest
    of the way there instead of relying on this function to detect it. Their "nearest
    vertex" match is straight-line distance, not distance-along-the-path -- if the
    discarded stretch loops back near itself, it can match a vertex other than the one
    you'd expect from just looking at where the path travels in sequence. Callers
    should show `nearest_idx`'s point on the plot so a person can catch a bad match
    before applying it.

    Returns (new_path_xy, nearest_idx) -- nearest_idx indexes into the ORIGINAL
    path_xy (the matched vertex) for "head"/"tail", or None for "full"."""
    waypoints_xy = np.asarray(waypoints_xy)
    if keep_side == "full":
        return waypoints_xy, None
    path_xy = np.asarray(path_xy)
    if keep_side == "tail":
        nearest_idx = int(np.argmin(np.linalg.norm(path_xy - waypoints_xy[-1], axis=1)))
        new_path_xy = np.vstack([waypoints_xy, path_xy[nearest_idx:]])
    else:
        nearest_idx = int(np.argmin(np.linalg.norm(path_xy - waypoints_xy[0], axis=1)))
        new_path_xy = np.vstack([path_xy[:nearest_idx + 1], waypoints_xy])
    return new_path_xy, nearest_idx


# ---------------- FIGURE ----------------

def build_path_review_figure(points_utm, chosen, rejected, waypoints=None, keep_side="head"):
    depth = points_utm["depth_ft"]
    minx, miny, maxx, maxy = points_utm.total_bounds

    fig = go.Figure(go.Scattergl(
        x=points_utm.geometry.x, y=points_utm.geometry.y, mode="markers",
        marker=dict(
            size=4, color=depth,
            colorscale=DEPTH_COLORSCALE, cmin=DEPTH_COLOR_MIN, cmax=DEPTH_COLOR_MAX,
            colorbar=dict(title="Depth (ft)", tickfont=dict(color="white"),
                           title_font=dict(color="white")),
        ),
        hovertext=[f"{d:.1f} ft" for d in depth],
        hoverinfo="text",
        name="survey points",
        showlegend=False,
    ))

    # interpolate_cross_section_to_blob.py tags every point "real" (an actual survey
    # measurement) or "interpolated" (synthetic grid infill) in a point_source column --
    # only present on _INTERP surveys, not real full_coverage/cross_sections ones. A
    # light, semi-transparent grey overlay on just the real points (added AFTER the
    # depth-colored scatter so it renders on top, same z-order rule as the path/
    # waypoint traces below) traces out the shape of the original cross-section lines
    # without hiding the depth coloring everywhere else -- added 2026-08-31 so a
    # reviewer can see at a glance which parts of a synthetic full_coverage survey are
    # real data vs. grid-interpolated infill.
    if "point_source" in points_utm.columns:
        real_pts = points_utm.loc[points_utm["point_source"] == "real"]
        if not real_pts.empty:
            fig.add_trace(go.Scattergl(
                x=real_pts.geometry.x, y=real_pts.geometry.y, mode="markers",
                marker=dict(size=5, color="rgba(230,230,230,0.35)"),
                name="original (real) points",
                hoverinfo="skip",
            ))

    # scattergl, not scatter: plotly renders WebGL traces (the points above, also
    # scattergl) in their own canvas layer stacked above the regular SVG layer
    # regardless of trace order -- a plain go.Scatter line here would sit BEHIND the
    # point cloud no matter how late it's added. Keeping the path/bottleneck traces in
    # the same WebGL layer makes trace order (added after the points) actually control
    # what's on top.
    # In "full" mode the computed path is being thrown out entirely (see
    # splice_waypoints) -- showing it (and its now-meaningless bottleneck marker) is
    # just noise once you're building the whole thing from waypoints instead.
    chosen_path_utm = None
    if keep_side != "full":
        for cand, color, label in ((chosen, CHOSEN_COLOR, "chosen path"), (rejected, REJECTED_COLOR, "rejected path")):
            if cand is None:
                continue
            line_utm = _line_to_utm(cand["polyline"])
            if line_utm is not None:
                if label == "chosen path":
                    chosen_path_utm = line_utm
                xs, ys = line_utm.xy
                fig.add_trace(go.Scattergl(
                    x=list(xs), y=list(ys), mode="lines",
                    line=dict(color=color, width=3, dash="solid" if label == "chosen path" else "dash"),
                    name=label,
                ))
            bottleneck_utm = _point_to_utm(cand["bottleneck_lon"], cand["bottleneck_lat"])
            if bottleneck_utm is not None:
                fig.add_trace(go.Scattergl(
                    x=[bottleneck_utm.x], y=[bottleneck_utm.y], mode="markers",
                    marker=dict(size=14, color=color, symbol="star", line=dict(color="black", width=1)),
                    name=f"{label} bottleneck",
                ))

    if waypoints:
        wp_arr = np.asarray(waypoints)
        if keep_side == "full":
            # the waypoint chain IS the path -- no old-path vertex to match/mark, and
            # no dependency on a chosen candidate existing at all.
            if len(wp_arr) >= 2:
                fig.add_trace(go.Scattergl(
                    x=wp_arr[:, 0], y=wp_arr[:, 1], mode="lines",
                    line=dict(color=WAYPOINT_COLOR, width=3, dash="dot"),
                    name="waypoint preview",
                ))
        elif chosen_path_utm is not None:
            orig_path_xy = np.array(chosen_path_utm.coords)
            preview_xy, cut_idx = splice_waypoints(orig_path_xy, wp_arr, keep_side)
            fig.add_trace(go.Scattergl(
                x=preview_xy[:, 0], y=preview_xy[:, 1], mode="lines",
                line=dict(color=WAYPOINT_COLOR, width=3, dash="dot"),
                name="waypoint preview",
            ))
            # exactly which old-path vertex the waypoint chain grafts onto -- shown
            # explicitly because the nearest-vertex match (see splice_waypoints) can
            # land somewhere unexpected on a path that loops back near itself, and
            # that's much easier to catch here than after Apply.
            cut_xy = orig_path_xy[cut_idx]
            fig.add_trace(go.Scattergl(
                x=[cut_xy[0]], y=[cut_xy[1]], mode="markers",
                marker=dict(size=16, color="#ff1744", symbol="x", line=dict(color="white", width=2)),
                name="cut point (old path)",
            ))
        fig.add_trace(go.Scattergl(
            x=wp_arr[:, 0], y=wp_arr[:, 1], mode="markers+text",
            marker=dict(size=10, color=WAYPOINT_COLOR, symbol="diamond", line=dict(color="black", width=1)),
            text=[str(i + 1) for i in range(len(wp_arr))], textposition="top center",
            textfont=dict(color=WAYPOINT_COLOR),
            name="waypoints",
        ))

    fig.update_layout(
        xaxis=dict(range=[minx, maxx], visible=False, fixedrange=True),
        yaxis=dict(range=[miny, maxy], visible=False, fixedrange=True,
                    scaleanchor="x", scaleratio=1),
        margin=dict(l=0, r=0, t=10, b=0),
        plot_bgcolor="#1a1a1a", paper_bgcolor="#1a1a1a",
        showlegend=True,
        legend=dict(font=dict(color="white"), bgcolor="rgba(0,0,0,0.4)"),
    )
    return fig


def _candidate_summary(cand, label):
    if cand is None:
        return None
    connected_txt = "connected" if cand["connected"] else "NOT connected"
    width_txt = f"{cand['width_ft']:.0f} ft" if pd.notna(cand.get("width_ft")) else "n/a"
    method_txt = f" [{cand['method']}]" if cand.get("method") else ""
    return f"{label}{method_txt}: {connected_txt}, through-width {width_txt}"


# ---------------- APP ----------------
app = Dash(__name__)

CANDIDATE_STYLE = {
    "display": "inline-block", "padding": "4px 14px", "border-radius": "12px",
    "margin-right": "14px", "font-weight": "bold",
}
BTN_STYLE = {"padding": "8px 16px", "color": "white", "border": "none", "margin-right": "10px"}

initial_pending = get_pending_files()

app.layout = html.Div(
    style={"font-family": "Arial, sans-serif", "padding": "12px",
           "background": "#1a1a1a", "color": "white", "min-height": "100vh"},
    children=[
        dcc.Store(id="pending-store", data=initial_pending),
        dcc.Store(id="index-store", data=0),
        dcc.Store(id="waypoints-store", data=[]),

        html.H3(id="queue-status"),
        html.Div(id="survey-header", style={"margin-bottom": "10px"}),
        html.Div("Check \"Add waypoints\", then click near where the path should actually go -- each click "
                  f"snaps to the deepest survey point within {WAYPOINT_RADIUS_FT:.0f}ft of where you clicked, "
                  "used in the order placed. \"Build entire path from waypoints\" throws out the computed "
                  "path completely -- your waypoint chain (in order, each already snapped to its local deep "
                  "point) becomes the whole path top to bottom, and width is measured fresh along it. "
                  "\"Keep before waypoints\" matches your FIRST waypoint to the nearest existing path point, "
                  "keeps everything before that, and re-routes through the waypoint chain from there (path is "
                  "fine at the start, wrong later). \"Keep after waypoints\" matches your LAST waypoint "
                  "instead, keeps everything after that point, and routes the waypoint chain INTO it (path is "
                  "wrong near the start, fine again later). For the before/after modes, the red X marks "
                  "exactly which existing point it matched -- check it before applying, since on a path that "
                  "loops back near itself the nearest match isn't always the one you'd expect. Apply Waypoints "
                  "re-measures width and replaces the chosen path; Confirm afterward promotes it live as usual. "
                  "To build off the REJECTED (orange) candidate instead, select \"Swap to rejected candidate\" "
                  "above and use \"Build entire path from waypoints\" -- head/tail splicing isn't supported for "
                  "the rejected candidate.",
                  style={"color": "#aaaaaa", "margin-bottom": "8px", "font-style": "italic"}),

        dcc.Graph(id="path-plot", style={"height": "650px", "width": "100%"},
                  config={"displayModeBar": False, "scrollZoom": False}),

        html.Div(
            style={"display": "flex", "gap": "20px", "align-items": "center", "margin-top": "14px", "flex-wrap": "wrap"},
            children=[
                dcc.RadioItems(
                    id="candidate-choice",
                    options=[{"label": " Use chosen path", "value": "chosen"},
                             {"label": " Swap to rejected candidate", "value": "rejected"}],
                    value="chosen",
                ),
                dcc.Checklist(
                    id="waypoint-mode-checklist",
                    options=[{"label": " Add waypoints (click the plot)", "value": "waypoint"}],
                    value=[],
                ),
                dcc.RadioItems(
                    id="keep-side-radio",
                    options=[{"label": " Keep before waypoints", "value": "head"},
                             {"label": " Keep after waypoints", "value": "tail"},
                             {"label": " Build entire path from waypoints", "value": "full"}],
                    value="head",
                    inline=True,
                    labelStyle={"margin-right": "10px"},
                ),
                html.Button("Undo Last Waypoint", id="undo-waypoint-btn", n_clicks=0,
                            style={**BTN_STYLE, "background": "#7a5b00"}),
                html.Button("Clear Waypoints", id="clear-waypoints-btn", n_clicks=0,
                            style={**BTN_STYLE, "background": "#7a5b00"}),
                html.Button("Apply Waypoints", id="apply-waypoints-btn", n_clicks=0,
                            style={**BTN_STYLE, "background": "#7b2d8e"}),
                html.Button("Save for Later", id="later-btn", n_clicks=0,
                            style={"padding": "8px 16px", "background": "#7a5b00", "color": "white", "border": "none"}),
                html.Button("Confirm", id="confirm-btn", n_clicks=0,
                            style={"padding": "8px 16px", "background": "#2166ac", "color": "white", "border": "none"}),
            ],
        ),

        html.Div(id="action-message", style={"margin-top": "10px", "color": "#7fb3e8"}),
    ],
)


@app.callback(
    Output("path-plot", "figure"),
    Output("survey-header", "children"),
    Output("queue-status", "children"),
    Output("candidate-choice", "options"),
    Output("candidate-choice", "style"),
    Input("pending-store", "data"),
    Input("index-store", "data"),
    Input("waypoints-store", "data"),
    Input("keep-side-radio", "value"),
)
def recompute(pending, index, waypoints, keep_side):
    if not pending:
        empty = go.Figure()
        empty.update_layout(margin=dict(l=0, r=0, t=0, b=0),
                             plot_bgcolor="#1a1a1a", paper_bgcolor="#1a1a1a")
        return empty, html.Div("No paths pending review."), "All caught up", [], {"display": "none"}

    index = index % len(pending)
    file = pending[index]

    df = load_bathym_fixed()
    row = df.set_index("file").loc[file]

    points_utm = load_survey_points(file)
    chosen = candidate_info(row, "_pending")
    rejected = candidate_info(row, "_rejected_pending")

    fig = build_path_review_figure(points_utm, chosen, rejected, waypoints, keep_side or "head")

    header_spans = [
        html.Span(f"{file}", style={"font-weight": "bold", "margin-right": "14px"}),
        html.Span(f"date: {row.get('date')}", style={"margin-right": "14px"}),
    ]
    if "survey_type" in points_utm.columns and points_utm["survey_type"].iloc[0] == "full_coverage":
        header_spans.append(html.Span(f"showing 1-in-{MULTIBEAM_DOWNSAMPLE_FACTOR} points (multibeam)",
                                       style={"margin-right": "14px", "color": "#aaaaaa", "font-style": "italic"}))
    if waypoints:
        header_spans.append(html.Span(f"{len(waypoints)} waypoint(s) placed (not yet applied)",
                                       style=dict(CANDIDATE_STYLE, background=WAYPOINT_COLOR, color="black")))
    chosen_summary = _candidate_summary(chosen, "Chosen")
    if chosen_summary:
        style = dict(CANDIDATE_STYLE, background=CHOSEN_COLOR, color="black")
        header_spans.append(html.Span(chosen_summary, style=style))
    rejected_summary = _candidate_summary(rejected, "Rejected")
    if rejected_summary:
        style = dict(CANDIDATE_STYLE, background=REJECTED_COLOR, color="black")
        header_spans.append(html.Span(rejected_summary, style=style))
    header = html.Div(header_spans)
    status = f"Path {index + 1} of {len(pending)} pending"

    if rejected is None:
        radio_options = [{"label": " Use chosen path", "value": "chosen", "disabled": True}]
        radio_style = {"display": "none"}
    else:
        radio_options = [{"label": " Use chosen path", "value": "chosen"},
                          {"label": " Swap to rejected candidate", "value": "rejected"}]
        radio_style = {"display": "block"}

    return fig, header, status, radio_options, radio_style


@app.callback(
    Output("pending-store", "data", allow_duplicate=True),
    Output("index-store", "data", allow_duplicate=True),
    Output("action-message", "children", allow_duplicate=True),
    Input("later-btn", "n_clicks"),
    State("pending-store", "data"),
    State("index-store", "data"),
    prevent_initial_call=True,
)
def save_for_later(n_clicks, pending, index):
    """Set this survey's path aside for manual handling: move its raw gpkg out of
    NAVD88_DIR (so 8/9's own glob() stops seeing it, and it drops out of this tool's
    queue) and mark path_confirmed="deferred"."""
    if not pending:
        return pending, index, "Nothing to save."

    index = index % len(pending)
    file = pending[index]

    SURVEYS_TO_REVIEW_DIR.mkdir(parents=True, exist_ok=True)
    src = NAVD88_DIR / file
    if src.exists():
        src.rename(SURVEYS_TO_REVIEW_DIR / file)

    df = load_bathym_fixed()
    df.loc[df["file"] == file, "path_confirmed"] = "deferred"
    df.to_csv(BATHYM_FIXED_FILE, index=False)

    new_pending = get_pending_files()
    new_index = min(index, max(len(new_pending) - 1, 0))
    message = f"Saved {file}'s path for later ({SURVEYS_TO_REVIEW_DIR.name}/)." if new_pending \
        else "Saved for later. No paths left pending."
    return new_pending, new_index, message


@app.callback(
    Output("pending-store", "data"),
    Output("index-store", "data", allow_duplicate=True),
    Output("action-message", "children"),
    Input("confirm-btn", "n_clicks"),
    State("pending-store", "data"),
    State("index-store", "data"),
    State("candidate-choice", "value"),
    prevent_initial_call=True,
)
def confirm_path(n_clicks, pending, index, choice):
    """Promote the selected candidate's _pending fields into the live vessel_path_*
    columns app.py reads, recording whichever candidate was NOT selected as the new
    vessel_path_*_rejected (so the road-not-taken stays on record), and set
    path_confirmed="yes" -- 8_compute_width_by_stage.py gates on this."""
    if not pending:
        return pending, index, "Nothing to confirm."

    index = index % len(pending)
    file = pending[index]

    df = load_bathym_fixed()
    mask = df["file"] == file
    row = df.loc[mask].iloc[0]

    chosen = candidate_info(row, "_pending")
    rejected = candidate_info(row, "_rejected_pending")
    use_rejected = choice == "rejected" and rejected is not None
    winner, loser = (rejected, chosen) if use_rejected else (chosen, rejected)

    # string-valued columns: force object dtype even if the column already existed but
    # happened to be all-NaN (read_csv then infers float64, and a later string
    # assignment raises instead of upcasting) -- same fix as
    # 7_compute_navigable_width.py's main(), needed here too since this is the only
    # place that ever writes these live columns now.
    for col in ("vessel_path_connected", "vessel_path_polyline", "vessel_path_method",
                "vessel_path_polyline_rejected", "vessel_path_method_rejected"):
        if col in df.columns and df[col].dtype != object:
            df[col] = df[col].astype(object)

    df.loc[mask, "vessel_path_connected"] = "yes" if winner["connected"] else "no"
    df.loc[mask, "vessel_path_width_ft"] = winner["width_ft"]
    df.loc[mask, "vessel_path_break_ft"] = winner["break_ft"]
    df.loc[mask, "vessel_path_bottleneck_lon"] = winner["bottleneck_lon"]
    df.loc[mask, "vessel_path_bottleneck_lat"] = winner["bottleneck_lat"]
    df.loc[mask, "vessel_path_polyline"] = winner["polyline"]
    df.loc[mask, "vessel_path_method"] = winner["method"]
    df.loc[mask, "vessel_path_polyline_rejected"] = loser["polyline"] if loser else None
    df.loc[mask, "vessel_path_method_rejected"] = loser["method"] if loser else None
    df.loc[mask, "path_confirmed"] = "yes"
    df.to_csv(BATHYM_FIXED_FILE, index=False)

    new_pending = get_pending_files()
    new_index = min(index, max(len(new_pending) - 1, 0))
    swap_note = " (swapped to rejected candidate)" if use_rejected else ""
    message = f"Confirmed {file}'s path{swap_note}." if new_pending else "Confirmed. No paths left pending."
    return new_pending, new_index, message


@app.callback(
    Output("waypoints-store", "data", allow_duplicate=True),
    Output("waypoint-mode-checklist", "value", allow_duplicate=True),
    Output("keep-side-radio", "value", allow_duplicate=True),
    Input("pending-store", "data"),
    Input("index-store", "data"),
    prevent_initial_call=True,
)
def reset_waypoints(pending, index):
    """Clear any in-progress waypoints (and drop out of waypoint mode, back to "keep
    before waypoints") whenever the current survey changes -- same reasoning as
    6_review_surveys.py resetting its own per-survey controls on navigation."""
    return [], [], "head"


@app.callback(
    Output("waypoints-store", "data", allow_duplicate=True),
    Output("action-message", "children", allow_duplicate=True),
    Input("path-plot", "clickData"),
    State("waypoint-mode-checklist", "value"),
    State("waypoints-store", "data"),
    State("pending-store", "data"),
    State("index-store", "data"),
    prevent_initial_call=True,
)
def add_waypoint(click_data, wp_mode_values, waypoints, pending, index):
    """A plain click, not a box/lasso drag -- drag-select on this plot glitched badly
    in practice (2026-08-31 feedback). A click always lands on/near an actual marker
    (the point cloud is dense enough that a click anywhere in the channel hits one), so
    its (x, y) -- already UTM, same coords the points are plotted in -- is used directly
    as the search center for deepest_point_near, same 150ft-radius snap as before."""
    if "waypoint" not in (wp_mode_values or []) or not pending:
        return no_update, no_update
    if not click_data or not click_data.get("points"):
        return no_update, no_update
    pt = click_data["points"][0]
    if "x" not in pt or "y" not in pt:
        return no_update, no_update
    center = (pt["x"], pt["y"])
    index = index % len(pending)
    file = pending[index]
    found = deepest_point_near(file, center)
    if found is None:
        return no_update, (f"No survey points within {WAYPOINT_RADIUS_FT:.0f}ft of that click -- "
                            f"try clicking closer to the channel.")
    x, y, depth = found
    new_waypoints = (waypoints or []) + [[x, y]]
    return new_waypoints, f"Waypoint {len(new_waypoints)} placed (depth {depth:.1f} ft there)."


@app.callback(
    Output("waypoints-store", "data", allow_duplicate=True),
    Input("undo-waypoint-btn", "n_clicks"),
    State("waypoints-store", "data"),
    prevent_initial_call=True,
)
def undo_waypoint(n_clicks, waypoints):
    return (waypoints or [])[:-1]


@app.callback(
    Output("waypoints-store", "data", allow_duplicate=True),
    Input("clear-waypoints-btn", "n_clicks"),
    prevent_initial_call=True,
)
def clear_waypoints(n_clicks):
    return []


@app.callback(
    Output("pending-store", "data", allow_duplicate=True),
    Output("index-store", "data", allow_duplicate=True),
    Output("waypoints-store", "data", allow_duplicate=True),
    Output("waypoint-mode-checklist", "value", allow_duplicate=True),
    Output("action-message", "children", allow_duplicate=True),
    Input("apply-waypoints-btn", "n_clicks"),
    State("pending-store", "data"),
    State("index-store", "data"),
    State("waypoints-store", "data"),
    State("keep-side-radio", "value"),
    State("candidate-choice", "value"),
    prevent_initial_call=True,
)
def apply_waypoints(n_clicks, pending, index, waypoints, keep_side, candidate_choice):
    """Grafts the placed waypoint chain onto the chosen candidate's path for "head"/
    "tail" keep_side, or (keep_side="full") replaces the path entirely with just the
    waypoint chain -- see splice_waypoints for what each keep_side mode does. If "Swap
    to rejected candidate" is selected in candidate-choice, ONLY keep_side="full" is
    allowed (added 2026-08-31 so the rejected/orange path can be walked by hand, since
    the tool previously only ever worked off the chosen/green path) -- head/tail against
    the rejected candidate is refused because the on-disk transects file's stations are
    measured along the CHOSEN path's vertices, so splicing against the rejected path's
    different geometry there would silently pair waypoints with the wrong path's station
    data. Re-measures width for the new/replaced chunks against a freshly-read local
    point set (NOT the downsampled preview -- see WIDTH_SCAN_MARGIN_M): for a blob
    (full_coverage) survey, width_blob.calculate_width_at_chunk per SEGMENT between
    consecutive path points (a 2D isotropic scan, same as the automatic blob pathway);
    for a cross_sections survey (added 2026-09-01, see width_at_waypoint_on_lines),
    that scan doesn't make sense -- survey points only exist along discrete physical
    lines, not a continuous 2D field -- so width is instead the deep-water run along
    whichever cross-section LINE each waypoint POINT actually sits on, one row per
    point rather than per segment (matches the automatic lines pathway's own
    width_at_crossing/lines_qa_rows convention). Which pathway a survey is gets read
    fresh off its raw gpkg's survey_type, not inferred from vessel_path_method_pending
    (a keep_side="full" rebuild overwrites that to "manual_waypoints" with no pathway
    info left in it). Rewrites this survey's _pending columns (the corrected result
    always lands in the CHOSEN slot regardless of source, so a normal Confirm afterward
    promotes it live without needing to also flip candidate-choice), its
    NavigableWidth/{id}_transects.geojson, and its navigable_width_profile.csv rows,
    then reloads the queue so the corrected result shows immediately as the new "chosen
    path". Does not touch path_confirmed -- still needs a normal Confirm afterward."""
    if not pending:
        return pending, index, [], [], "Nothing to correct."
    keep_side = keep_side or "head"
    min_waypoints = 2 if keep_side == "full" else 1
    if not waypoints or len(waypoints) < min_waypoints:
        msg = ("Draw at least 2 waypoints to build a full path." if min_waypoints == 2
               else "Draw at least one waypoint first.")
        return pending, index, waypoints, no_update, msg

    index = index % len(pending)
    file = pending[index]
    survey_id = file.replace("_SurveyPoint.gpkg", "")

    df = load_bathym_fixed()
    mask = df["file"] == file
    row = df.loc[mask].iloc[0]
    waypoints_xy = np.asarray(waypoints)
    transects_path = OUT_DIR / f"{survey_id}_transects.geojson"

    # Which pathway this survey actually is -- read straight off the raw gpkg's own
    # survey_type rather than inferring from vessel_path_method_pending, since a
    # keep_side="full" waypoint rebuild overwrites that to "manual_waypoints" with no
    # pathway info left in it (so a SECOND waypoint pass on an already-rebuilt survey
    # couldn't tell blob from lines that way). A cross_sections survey's points only
    # exist along discrete physical lines -- width_blob.calculate_width_at_chunk's
    # isotropic outward scan (built for a 2D blob's continuous coverage) finds nothing
    # off a narrow line's own strip, so those surveys need width_at_waypoint_on_lines
    # instead (measures the deep-water run along whichever line the point is on, same
    # as the normal automatic lines pathway's own width_at_crossing).
    type_sample = gpd.read_file(NAVD88_DIR / file, rows=1, columns=["survey_type"])
    is_lines = not type_sample.empty and type_sample["survey_type"].iloc[0] == "cross_sections"

    if keep_side == "full":
        # the waypoint chain IS the path -- no dependency on a computed candidate or
        # existing transects file at all (see splice_waypoints' "full" mode).
        new_path_xy, _ = splice_waypoints(None, waypoints_xy, "full")
        kept_rows = []
        existing_date = row.get("date")
        # lines mode: one row PER WAYPOINT POINT, matching the normal automatic lines
        # pathway's one-row-per-crossed-section convention (lines_qa_rows) -- every
        # waypoint is its own crossing, width isn't a between-two-points measurement.
        # blob mode (unchanged): one row per SEGMENT between consecutive waypoints,
        # matching calculate_width_at_chunk -- one fewer row than points.
        new_row_indices = range(0, len(waypoints_xy)) if is_lines else range(0, len(waypoints_xy) - 1)
    else:
        use_rejected = candidate_choice == "rejected"
        # "head"/"tail" splice onto the REJECTED candidate is deliberately NOT supported:
        # kept_rows below is sliced out of the on-disk transects file by nearest_idx, but
        # that file's stations were measured along the CHOSEN path's vertices -- if
        # path_xy here were the rejected path's (different geometry, different vertex
        # count/spacing), nearest_idx would index into the wrong path and kept_rows would
        # silently pair rejected-path waypoints with chosen-path station data. Only "full"
        # is safe for the rejected candidate, since it ignores kept_rows/existing
        # entirely -- caught above by min_waypoints=2 already being required for "full".
        if use_rejected:
            return pending, index, waypoints, no_update, (
                "Building off the rejected candidate only supports \"Build entire path "
                "from waypoints\" (head/tail splicing would mismatch station data from "
                "the chosen path's transects file) -- switch keep_side to \"full\" and "
                "place 2+ waypoints."
            )
        chosen = candidate_info(row, "_pending")
        if chosen is None or pd.isna(chosen.get("polyline")):
            return pending, index, waypoints, no_update, "No pending path to correct."

        if not transects_path.exists():
            return pending, index, waypoints, no_update, f"No transects file for {survey_id} -- run stage 7 first."
        existing = gpd.read_file(transects_path)
        existing_date = existing["date"].iloc[0] if "date" in existing.columns and len(existing) else row.get("date")

        path_line_utm = _line_to_utm(chosen["polyline"])
        path_xy = np.array(path_line_utm.coords)
        new_path_xy, nearest_idx = splice_waypoints(path_xy, waypoints_xy, keep_side)

        # which existing transect rows survive unchanged, which path-vertex range holds
        # the new/replaced segments (for the local width-rescan bbox below), and in what
        # order the kept + new rows end up -- mirrors splice_waypoints' two modes. Same
        # slice for both pathways in "tail" mode (kept rows are literally untouched
        # original points/segments either way); "head" mode differs by one index -- see
        # the comment below.
        if keep_side == "tail":
            kept_rows = existing.iloc[nearest_idx:].to_dict("records")
            new_row_indices = range(0, len(waypoints_xy))
        elif is_lines:
            # the graft vertex new_path_xy[nearest_idx] keeps its own unchanged row in
            # point-per-row mode (kept_rows INCLUDES index nearest_idx), so only the
            # waypoints strictly after it are new.
            kept_rows = existing.iloc[:nearest_idx + 1].to_dict("records")
            new_row_indices = range(nearest_idx + 1, len(new_path_xy))
        else:
            # blob's segment-per-row mode: the segment connecting that same vertex to
            # the first waypoint IS new (nothing measured it before), so kept_rows stops
            # one short (nearest_idx, exclusive) and new segments start AT nearest_idx.
            kept_rows = existing.iloc[:nearest_idx].to_dict("records")
            new_row_indices = range(nearest_idx, len(new_path_xy) - 1)

    # Blob-mode-only local read (see the rationale comment where it's used, below) --
    # PER SEGMENT, not once across the whole path, to keep each scan's point count cheap
    # regardless of how many segments/waypoints there are in total.
    def _local_points_near(point_a, point_b):
        seg_xy = np.array([point_a, point_b])
        lo = seg_xy.min(axis=0) - WIDTH_SCAN_MARGIN_M
        hi = seg_xy.max(axis=0) + WIDTH_SCAN_MARGIN_M
        seg_bbox_3857 = gpd.GeoSeries([box(lo[0], lo[1], hi[0], hi[1])], crs=UTM_CRS).to_crs("EPSG:3857").iloc[0].bounds
        seg_gdf = gpd.read_file(NAVD88_DIR / file, bbox=seg_bbox_3857, columns=["depth_ft"])
        return seg_gdf.to_crs(UTM_CRS)

    cum_dist_m = np.concatenate([[0.0], np.cumsum(np.linalg.norm(np.diff(new_path_xy, axis=0), axis=1))])
    new_rows_utm = []
    if is_lines:
        # One row PER WAYPOINT POINT -- width_at_waypoint_on_lines does its own local
        # bbox read (margin_m, centered on the point) rather than the segment-pair read
        # _local_points_near does, since a cross-section line's own extent is what
        # needs to fit in that box, not a path segment between two arbitrary waypoints.
        for i in new_row_indices:
            width_ft, section_line = width_at_waypoint_on_lines(file, tuple(new_path_xy[i]))
            new_rows_utm.append({
                "station_ft": round(cum_dist_m[i] * FT_PER_M, 1),
                "width_ft": round(width_ft, 1),
                "flag": "waypoint",
                "geometry": section_line if section_line is not None else Point(*new_path_xy[i]),
                "lon_lat_source": Point(*new_path_xy[i]),
            })
    else:
        # Re-measure width for the new/replacement chunks against a local, full-resolution
        # (not downsampled) read -- see WIDTH_SCAN_MARGIN_M. Read PER SEGMENT, not once
        # across all of scan_xy: calculate_width_at_chunk does a `.within(buffer)` scan
        # (no spatial index) at every one of its ~100 outward steps, so its cost is
        # steps x len(local_utm) FOR EACH SEGMENT -- fine against a few thousand local
        # points, but a single reach-spanning read (e.g. 68 waypoints covering all 15
        # miles) put 300k+ points through that scan on every step of every segment and
        # never finished in practice (2026-08-31). A small bbox per segment keeps each
        # scan cheap regardless of how many segments/waypoints there are in total.
        for i in new_row_indices:
            local_utm = _local_points_near(new_path_xy[i], new_path_xy[i + 1])
            w = width_blob.calculate_width_at_chunk(
                local_utm, tuple(new_path_xy[i]), tuple(new_path_xy[i + 1]), depth_threshold=WIDTH_TARGET_DEPTH_FT
            )
            mid_x, mid_y = w["midpoint"]
            angle = w["perpendicular_angle"]
            dx, dy = np.cos(angle), np.sin(angle)
            width_left_m = w["width_left"] / FT_PER_M
            width_right_m = w["width_right"] / FT_PER_M
            station_m = (cum_dist_m[i] + cum_dist_m[i + 1]) / 2
            new_rows_utm.append({
                "station_ft": round(station_m * FT_PER_M, 1),
                "width_ft": round(w["total_width"], 1),
                "flag": "waypoint",
                "geometry": LineString([
                    (mid_x - width_left_m * dx, mid_y - width_left_m * dy),
                    (mid_x + width_right_m * dx, mid_y + width_right_m * dy),
                ]),
                "lon_lat_source": Point(mid_x, mid_y),
            })

    new_rows_gdf = gpd.GeoDataFrame(new_rows_utm, geometry="geometry", crs=UTM_CRS)
    lon_lat = gpd.GeoSeries(new_rows_gdf["lon_lat_source"], crs=UTM_CRS).to_crs("EPSG:4326")
    new_rows_gdf["lon"] = lon_lat.x.round(6)
    new_rows_gdf["lat"] = lon_lat.y.round(6)
    new_rows_gdf = new_rows_gdf.drop(columns=["lon_lat_source"]).to_crs("EPSG:4326")
    new_rows_gdf["survey_id"] = survey_id
    new_rows_gdf["date"] = existing_date
    new_rows_gdf["method"] = "lines" if is_lines else "blob"
    new_rows = new_rows_gdf.to_dict("records")

    all_rows = (new_rows + kept_rows) if keep_side == "tail" else (kept_rows + new_rows)
    for i, r in enumerate(all_rows):
        r["station_id"] = i
        # station_ft is cumulative distance along the path. In lines mode (one row per
        # POINT), all_rows[i] corresponds exactly to new_path_xy[i] in every keep_side
        # mode (kept_rows and new_rows are built to sit at exactly the point-indices
        # they occupy in the combined path -- see the kept_rows/new_row_indices
        # comments above), so it's just that point's own cumulative distance. In blob
        # mode (one row per SEGMENT, len(all_rows) == len(new_path_xy)-1), it's the
        # midpoint between path vertices i and i+1 -- kept_rows' old station_ft values
        # (relative to the ORIGINAL path) get overwritten here regardless, so this is
        # correct for "tail" mode's shifted kept rows too, not just "head"'s unchanged
        # prefix.
        if is_lines:
            r["station_ft"] = round(cum_dist_m[i] * FT_PER_M, 1)
        else:
            station_m = (cum_dist_m[i] + cum_dist_m[i + 1]) / 2
            r["station_ft"] = round(station_m * FT_PER_M, 1)

    real_rows = [r for r in all_rows if r["flag"] != "gap_bridge"]
    connected = all(r["width_ft"] > 0 for r in real_rows)
    through_width_ft = round(min(r["width_ft"] for r in real_rows), 1)
    bottleneck_row = min(real_rows, key=lambda r: r["width_ft"])
    break_station_ft = None if connected else bottleneck_row["station_ft"]

    path_wkt = gpd.GeoSeries([LineString(new_path_xy)], crs=UTM_CRS).to_crs("EPSG:4326").iloc[0].wkt
    if keep_side == "full":
        new_method = "manual_waypoints"
    else:
        old_method = str(chosen.get("method") or "blob")
        new_method = old_method if old_method.endswith("+waypoints") else f"{old_method}+waypoints"

    if df["vessel_path_polyline_pending"].dtype != object:
        df["vessel_path_polyline_pending"] = df["vessel_path_polyline_pending"].astype(object)
    if df["vessel_path_method_pending"].dtype != object:
        df["vessel_path_method_pending"] = df["vessel_path_method_pending"].astype(object)
    if df["vessel_path_connected_pending"].dtype != object:
        df["vessel_path_connected_pending"] = df["vessel_path_connected_pending"].astype(object)

    df.loc[mask, "vessel_path_connected_pending"] = "yes" if connected else "no"
    df.loc[mask, "vessel_path_width_ft_pending"] = through_width_ft
    df.loc[mask, "vessel_path_break_ft_pending"] = break_station_ft
    df.loc[mask, "vessel_path_bottleneck_lon_pending"] = bottleneck_row["lon"]
    df.loc[mask, "vessel_path_bottleneck_lat_pending"] = bottleneck_row["lat"]
    df.loc[mask, "vessel_path_polyline_pending"] = path_wkt
    df.loc[mask, "vessel_path_method_pending"] = new_method
    df.to_csv(BATHYM_FIXED_FILE, index=False)

    new_gdf = gpd.GeoDataFrame(all_rows, geometry="geometry", crs="EPSG:4326")
    new_gdf.to_file(transects_path, driver="GeoJSON")

    profile_rows = [{
        "survey_id": survey_id, "date": r["date"], "year": pd.to_datetime(r["date"], utc=True).year,
        "mile": None, "method": "lines" if is_lines else "blob", "station_id": r["station_id"], "station_ft": r["station_ft"],
        "width_ft": r["width_ft"], "flag": r["flag"], "lon": r["lon"], "lat": r["lat"],
    } for r in all_rows]
    new_profile = pd.DataFrame(profile_rows)
    if PROFILE_FILE.exists():
        profile_existing = pd.read_csv(PROFILE_FILE)
        keep_mile = profile_existing.loc[profile_existing["survey_id"] == survey_id, "mile"]
        if len(keep_mile):
            new_profile["mile"] = keep_mile.iloc[0]
        profile_existing = profile_existing[profile_existing["survey_id"] != survey_id]
        combined = pd.concat([profile_existing, new_profile], ignore_index=True)
    else:
        combined = new_profile
    combined.to_csv(PROFILE_FILE, index=False)

    new_pending = get_pending_files()
    new_index = min(index, max(len(new_pending) - 1, 0)) if new_pending else 0
    message = (f"Applied {len(waypoints)} waypoint(s) to {survey_id}: "
               f"connected={connected}, through-width={through_width_ft:.0f} ft.")
    return new_pending, new_index, [], [], message


if __name__ == "__main__":
    # dev_tools_ui off: the devtools bar covers the confirm/save-for-later controls
    app.run(debug=True, dev_tools_ui=False, host="0.0.0.0", port=8061)

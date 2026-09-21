"""
Local QA tool: fast pre-flight review pass before the rest of the pipeline
(7_compute_navigable_width.py onward) runs on freshly-downloaded surveys.

Run after 4_compute_thresh_depth.py (stage 4) -- the first point depth_ft/water_elev
exist to plot by depth -- and before 7_compute_navigable_width.py, which (along with
8_compute_width_by_stage.py) gates on confirmed=="yes" for exactly the things this
tool checks:

  1. Sign check: USACE's eHydro source occasionally publishes a survey with Z_navd88's
     sign flipped relative to every neighboring survey -- points colored by depth should
     show a channel (deep/blue) with shallower banks (shallow/red); a flipped survey
     shows that pattern inverted. Toggle "Flip sign" to preview Z_navd88 negated; if that
     looks right, Accept while it's checked to patch Z_navd88 (and recompute depth_ft to
     match) in the gpkg permanently, and log the file to SIGN_FLIP_LOG_FILE (a running
     record of every survey that needed the fix).

  2. Classification fit: 3_process_surveys.py already tagged each survey's survey_type
     (full_coverage / cross_sections / sparse_lines), shown here for a human to
     sanity-check against the actual point pattern -- e.g. a "full_coverage" tag on a
     survey that's really a fan of separated line transects would route it into
     7_compute_navigable_width.py's blob pathway (width_blob.py), which assumes
     reasonably continuous coverage and won't measure a line-transect survey sensibly.
     A survey that doesn't look like a clean fit for either pathway (mislabeled, odd
     geometry, mixed coverage) should be set aside with "Save for Later" instead of
     Accept: its raw gpkg is moved to SURVEYS_TO_REVIEW_DIR (shared with
     7b_review_navigable_path.py's own "Save for Later" -- see that script's module
     docstring) and confirmed is set to "deferred" -- excluded from the live map (same
     as "no"/"rejected") and from get_pending_files(), so it stops cluttering the review
     queue and stops appearing to 7/8/9's own NAVD88_DIR.glob() without blocking the
     rest of the batch. Nothing downstream distinguishes "deferred" from "no"/"rejected"
     -- it's on bathym_fixed.csv purely so a person can filter for these later and
     decide by hand.

  3. Blob path orientation (full_coverage surveys only): 7_compute_navigable_width.py's
     width_blob.py algorithm normally auto-detects whether to run horizontal or vertical
     test lines off the survey's own bounding box, then hill-climbs the local
     cross-section angle at every step. For a survey where that guess is wrong -- or the
     hill-climbing wanders -- a reviewer can override it here with a 4-way choice,
     written to bathym_fixed.csv's blob_path_mode column and read by
     7_compute_navigable_width.py (and reproduced identically by
     8_compute_width_by_stage.py):
       - "Original" (default): unchanged auto-detected + hill-climbed behavior.
       - "Horizontal"/"Vertical": forces that starting line and travel direction, and
         fixes the cross-section angle to exactly horizontal/vertical for every step
         instead of hill-climbing it (see width_blob.get_navigable_path's docstring).
       - "Neither": neither line orientation fits this survey's shape well enough to
         path-find at all. Accept with this selected behaves like "Save for Later" but
         moves the raw gpkg to BLOB_TO_REVIEW_DIR instead, so these can be filtered and
         handled as their own batch later (e.g. a bespoke or manual path).
     This control only appears for full_coverage surveys; other survey_types are
     unaffected and their blob_path_mode stays blank.

  4. Path/width point exclusion/inclusion (any survey_type): "Region mode" picks what
     drawn box/lasso regions mean; both write to the same sidecar,
     data/NavigablePathTrims/{survey_id}_SurveyPoint.gpkg (same survey_id, just a
     different directory) -- 7 and 8 both check there first and read it instead of
     NAVD88Files/ when present. Every other stage (4_compute_thresh_depth.py's depth_ft,
     this tool's own sign check, 9_make_depth_polygons.py's depth polygons) always uses
     every point in the survey; neither mode below ever removes a point from the survey
     itself, only from what 7/8 (and interpolate_cross_section_to_blob.py's hull, which
     also reads this sidecar) see.
       - "Exclude selected" (default): drawn region(s) mark points to DROP from
         path/width finding -- typically two overlapping cross-section "lines" that
         dissolve into one blob and fail 3_process_surveys.py's build_sections()
         linearity check, dropping the whole cluster (see that function's
         check_linearity()). Sidecar = everything EXCEPT the drawn region(s).
       - "Keep ONLY selected" (added 2026-08-31): the inverse -- for a survey with a
         small stray piece of coverage in an unrelated location (e.g. a sliver of an
         adjacent reach caught in the same download) that stretches the convex hull and
         throws off interpolate_cross_section_to_blob.py's grid and width_blob.py's
         horizontal/vertical centerline hill-climbing. Draw region(s) around the part
         that IS the real survey; sidecar = ONLY the drawn region(s), everything else
         dropped. Same non-empty-result guard as exclude mode, just inverted (refuses if
         the drawn region(s) would keep zero points).
     This used to be a separate tool (10_fix_surveys.py, retired 2026-08-29) that only
     worked on already-deferred surveys sitting in SurveysToReview/ and (incorrectly)
     created a whole new survey_id for the trimmed copy -- folded in here so the
     exclusion can be drawn in the same pass as the sign check and classification call,
     on any pending survey, without a detour through Save for Later.

  5. Split into 2 chunks (any survey_type): for a survey that's really two disconnected
     clusters of points bundled into one file (so a single vessel path/width run across
     the whole thing is meaningless -- same underlying problem as the LM_08_MVF merge-gap
     work, handled here per-survey up front instead of by a one-off segment-and-bridge
     script after the fact). Set Region mode to "Split into 2 chunks", draw one or more
     box/lasso regions around ONE cluster (their union is chunk A; everything else is
     chunk B -- both must be non-empty), then Accept. This writes two brand new surveys,
     {survey_id}_A_SurveyPoint.gpkg and {survey_id}_B_SurveyPoint.gpkg, to NAVD88_DIR,
     each with its own bathym_fixed.csv row (confirmed="yes", milemarker and geometry
     recomputed from that chunk's own points -- same nearest_mile lookup
     7_compute_navigable_width.py itself uses, and same convex-hull construction
     4_compute_thresh_depth.py uses -- vessel_path_*/path_confirmed/blob_path_mode reset
     blank since those describe the old combined survey's now-meaningless path, not this
     chunk's). The original combined file is moved to SPLIT_ARCHIVE_DIR and its row's
     confirmed is set to "split" (excluded from the map/queue, same as "deferred"/
     "rejected"). From here on 7/8/9 treat _A and _B as two ordinary independent surveys.
     Not recomputed: depth_ft (still keyed to the ORIGINAL combined survey's single
     nearest-gauge water_elev/milemarker, per-point, from stage 4 -- fine unless the two
     clusters are far enough apart to actually warrant different gauges) and the
     vessel-weighted bathym_mean/depth aggregate stats (copied through unchanged from the
     original row, not re-run per chunk). If a curated survey-selection list (e.g.
     2022_channel_width_survey_list.csv) already named the original combined survey_id,
     swap it for the two new _A/_B ids by hand.

This replaces the old sign-flip-only version of this tool (see old_survey_review.py) --
that whole Scattermap/basemap review is gone in favor of a plain, non-interactive
depth-colored scatter (no tile dependency, no pan/zoom to fight with) since classifying
a point pattern by eye doesn't need a real-world map underneath it. It's also not the
older risk-classification review (AIS-density overlay, nav-path percentile, low/medium/
high tiers, problem-point marking) -- that workflow is retired entirely;
7_compute_navigable_width.py's objective vessel_path_connected/width_ft drives the map's
risk coloring now, no manual judgment call needed there.

Local-only tool, never deployed -- runs on its own port, separate from app.py.
"""

from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pyogrio
import shapely
from dash import Dash, dcc, html, Input, Output, State
from shapely.geometry import Point

# module name isn't a valid identifier -- reused the same import_module pattern
# 8_compute_width_by_stage.py already uses for the same reason. Only nearest_mile is
# used from it, for the Split feature's per-chunk milemarker recompute (item 5 above).
_stage7 = import_module("7_compute_navigable_width")

# ---------------- CONFIG ----------------
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
NAVD88_DIR = SCRIPT_DIR / "data" / "NAVD88Files"
# Temporary: while RESTRICT_TO_SURVEY_LIST_FILE has this app scoped to the 2022
# channel-width survey list, "Save for Later" here writes into its own dedicated
# folder (separate from 7b_review_navigable_path.py's SURVEYS_TO_REVIEW_DIR, which
# stays the shared bucket for every other survey) so Molly's 2022 set-asides don't mix
# in with unrelated deferrals. Living outside NAVD88_DIR means 7/8/9's own
# NAVD88_DIR.glob() can't accidentally pick it up even if that glob ever became
# recursive.
SURVEYS_TO_REVIEW_DIR = SCRIPT_DIR / "data" / "2022toFix"
# full_coverage surveys whose reviewer picked "Neither" for blob path orientation --
# separate from SURVEYS_TO_REVIEW_DIR since these were classified fine (full_coverage
# is the right pathway), they just don't fit either the horizontal or vertical
# starting-line approach; kept as their own batch to revisit later.
BLOB_TO_REVIEW_DIR = SCRIPT_DIR / "data" / "BlobToReview"
# path/width-only point-exclusion sidecars, written on Accept when regions are drawn
# (see module docstring item 4) -- same filename as the NAVD88_DIR gpkg, read by
# 7_compute_navigable_width.py/8_compute_width_by_stage.py in preference to it.
TRIM_DIR = SCRIPT_DIR / "data" / "NavigablePathTrims"
# Split feature (module docstring item 5): the original combined file, once split into
# _A/_B, is archived here rather than deleted -- kept in case the split needs redoing.
SPLIT_ARCHIVE_DIR = SCRIPT_DIR / "data" / "SplitOriginals"
BATHYM_FIXED_FILE = REPO_ROOT / "bathym_fixed.csv"
SIGN_FLIP_LOG_FILE = REPO_ROOT / "sign_flipped_surveys.csv"
NAVIGABLE_WIDTH_DIR = SCRIPT_DIR / "data" / "NavigableWidth"
WIDTH_BY_STAGE_DIR = SCRIPT_DIR / "data" / "WidthByStage"

# Temporary: restrict the review queue to only the surveys on a given list that are
# still missing stage 7 (NavigableWidth transects) or stage 8 (WidthByStage) output.
# The 2023 recheck batch this was last pointed at is done (see
# project_2023_recheck_batch_resolved memory) and its scratch list file is gone, so
# this is back to None -- reviewing every confirmed=="no" survey, i.e. the daily
# pipeline's actual pending queue.
RESTRICT_TO_SURVEY_LIST_FILE = None

# "Original" (bare width-vs-height auto-detect) replaced 2026-08-31 with two explicit
# variants -- same hill-climbing behavior, but the reviewer picks the starting line by
# eye instead of leaving it to the bbox-shape guess. See width_blob.get_navigable_path's
# mode docstring for exactly what each does.
BLOB_PATH_MODE_OPTIONS = [
    {"label": " Horizontal", "value": "horizontal"},
    {"label": " Vertical", "value": "vertical"},
    {"label": " Original / Horizontal", "value": "original_horizontal"},
    {"label": " Original / Vertical", "value": "original_vertical"},
    {"label": " Neither", "value": "neither"},
]
BLOB_PATH_MODE_DEFAULT = "original_horizontal"

UTM_CRS = "EPSG:26915"

# Depth colorscale range: this is a triage tool for spotting an inverted sign or an odd
# point pattern at a glance, not precise depth reading, so a fixed continuous range
# covering the typical channel/bank depth spread is enough -- values outside
# [DEPTH_COLOR_MIN, DEPTH_COLOR_MAX] just clip to the nearest end color. Plotly's
# "RdYlBu" colorscale runs red (low) -> yellow -> blue (high), matching the
# shallow=red/deep=blue convention app.py and the old review tool both use -- a flipped
# survey's channel reads red instead of blue, which is the whole point of this check.
DEPTH_COLOR_MIN = 5
DEPTH_COLOR_MAX = 25
DEPTH_COLORSCALE = "RdYlBu"

CLASSIFICATION_LABELS = {
    "full_coverage": "full_coverage (blob pathway)",
    "cross_sections": "cross_sections (lines pathway)",
    "sparse_lines": "sparse_lines (skipped by 7_compute_navigable_width.py)",
}


# ---------------- DATA ----------------

# Temporary: full_coverage (multibeam) surveys can carry hundreds of thousands of
# points, which makes both the queue scan and the per-survey plot painfully slow. Skip
# them here for now so the queue stays responsive for cross_sections/sparse_lines --
# set back to set() (or remove the filter in get_pending_files below) once that's fixed.
EXCLUDE_SURVEY_TYPES = set()

_survey_type_cache = {}


def load_bathym_fixed():
    return pd.read_csv(BATHYM_FIXED_FILE)


def _ensure_blob_path_mode_column(df):
    """Create blob_path_mode if missing and force object dtype: an all-blank column
    round-trips through CSV as float64 (pandas infers it from the all-NaN values), and a
    later string .loc assignment raises instead of upcasting."""
    if "blob_path_mode" not in df.columns:
        df["blob_path_mode"] = pd.NA
    if df["blob_path_mode"].dtype != object:
        df["blob_path_mode"] = df["blob_path_mode"].astype(object)
    return df


def _get_survey_type(file):
    """survey_type off `file`'s gpkg, without reading its points -- reads a single row
    (fast and independent of file size, unlike a full gpd.read_file) and caches the
    result, since get_pending_files() re-checks every candidate on every callback."""
    if file not in _survey_type_cache:
        try:
            gdf = gpd.read_file(NAVD88_DIR / file, rows=1)
            _survey_type_cache[file] = gdf["survey_type"].iloc[0] if "survey_type" in gdf.columns else None
        except Exception:
            _survey_type_cache[file] = None
    return _survey_type_cache[file]


def _needs_stage_7_or_8(survey_id):
    """True if `survey_id` is still missing stage 7's transects geojson or stage 8's
    width-by-stage csv -- used by RESTRICT_TO_SURVEY_LIST_FILE to only surface surveys
    that still need to move through this review before those stages can run."""
    has_transects = (NAVIGABLE_WIDTH_DIR / f"{survey_id}_transects.geojson").exists()
    has_width_by_stage = (WIDTH_BY_STAGE_DIR / f"{survey_id}_width_by_stage.csv").exists()
    return not (has_transects and has_width_by_stage)


def get_pending_files():
    """Surveys awaiting review: confirmed=="no", their raw points still on disk (not yet
    moved to SURVEYS_TO_REVIEW_DIR by a previous "Save for Later"), and not one of
    EXCLUDE_SURVEY_TYPES (see comment above). When RESTRICT_TO_SURVEY_LIST_FILE is set,
    further narrowed to survey_ids on that list that still need stage 7 or 8 output."""
    df = load_bathym_fixed()
    pending = df.loc[df["confirmed"] == "no", "file"].tolist()
    present = [f for f in pending if (NAVD88_DIR / f).exists()]
    if EXCLUDE_SURVEY_TYPES:
        present = [f for f in present if _get_survey_type(f) not in EXCLUDE_SURVEY_TYPES]
    if RESTRICT_TO_SURVEY_LIST_FILE is not None:
        allowed_ids = set(pd.read_csv(RESTRICT_TO_SURVEY_LIST_FILE)["survey_id"])
        present = [
            f for f in present
            if f.replace("_SurveyPoint.gpkg", "") in allowed_ids
            and _needs_stage_7_or_8(f.replace("_SurveyPoint.gpkg", ""))
        ]
    return present


# full_coverage (multibeam) surveys can carry hundreds of thousands of points -- a
# sparse sample is still plenty dense to eyeball the depth pattern/sign and doesn't
# change the overall point cloud shape, but cuts both the read and the Plotly render
# proportionally. Pushed down into the gpkg read itself (a `where` clause on SQLite's
# built-in rowid) rather than reading everything and then subsampling in pandas -- for
# a 280k-point survey that's the difference between a 15s read and well under a
# second, since the database skips the other rows entirely instead of just discarding
# them after.
#
# Scaled to the survey's OWN point count (not a fixed factor) -- a fixed 1-in-300 was
# fine for the 2M+-point merged multibeam blobs it was originally sized for, but left
# a ~155k-point interpolate_cross_section_to_blob.py output down to ~500 points,
# useless for actually seeing the interpolated grid (2026-08-31 feedback). Target a
# rendered count instead: pyogrio.read_info gives the real feature count almost
# instantly (no data read), so the factor is picked to land near
# MULTIBEAM_RENDER_TARGET_PTS regardless of how big or small the survey actually is --
# smaller full_coverage surveys now render at or near full resolution automatically.
MULTIBEAM_RENDER_TARGET_PTS = 100_000


def _multibeam_downsample_where(file):
    total = pyogrio.read_info(str(NAVD88_DIR / file))["features"]
    factor = max(1, total // MULTIBEAM_RENDER_TARGET_PTS)
    return f"rowid % {factor} = 0", factor


def load_survey_points(file, flip_sign):
    """Raw survey points for `file`, reprojected to UTM, with a depth_ft column --
    recomputed from water_elev/Z_navd88 here (not read off the gpkg's own depth_ft
    column) so the flip-sign checkbox can preview a flip without touching disk."""
    row = load_bathym_fixed().set_index("file").loc[file]
    where, _factor = _multibeam_downsample_where(file) if _get_survey_type(file) == "full_coverage" else (None, 1)
    gdf = gpd.read_file(NAVD88_DIR / file, where=where)  # EPSG:3857
    gdf_utm = gdf.to_crs(UTM_CRS)
    water_elev = float(row["water_elev"])
    z = -gdf_utm["Z_navd88"] if flip_sign else gdf_utm["Z_navd88"]
    gdf_utm["depth_ft"] = water_elev - z
    return row, gdf_utm


def build_split_row(orig_row, new_file, gdf_chunk_wgs84, mile):
    """Clone `orig_row` (a bathym_fixed.csv row, as a Series) into a new row for one
    split-off chunk (module docstring item 5): milemarker and geometry are recomputed
    from this chunk's OWN points -- same nearest_mile lookup 7_compute_navigable_width.py
    itself uses for gage assignment, and the same convex-hull construction
    4_compute_thresh_depth.py uses for its own `geometry` column -- since a chunk covers
    different ground than the original combined survey. Everything else (water_elev,
    bathym_mean, depth, weights_sum, etc.) is copied through unchanged -- see the
    docstring's "Not recomputed" note for why. vessel_path_*/path_confirmed/
    blob_path_mode/duplicate_of describe the OLD combined survey's path/duplicate-check,
    meaningless for this chunk, so they're reset blank for a clean run through 7/8."""
    new_row = orig_row.copy()
    new_row["file"] = new_file
    new_row["milemarker"] = round(mile, 1)
    coords = np.column_stack([gdf_chunk_wgs84.geometry.x.values, gdf_chunk_wgs84.geometry.y.values])
    new_row["geometry"] = shapely.convex_hull(shapely.multipoints(coords)).wkt
    new_row["confirmed"] = "yes"
    new_row["duplicate_of"] = ""
    for col in orig_row.index:
        if col == "blob_path_mode" or col == "path_confirmed" or col.startswith("vessel_path_"):
            new_row[col] = pd.NA
    return new_row


def log_sign_flip(file):
    """Append `file` to SIGN_FLIP_LOG_FILE, deduped on file (a re-flip of an
    already-logged file just refreshes its timestamp)."""
    entry = pd.DataFrame([{"file": file, "flipped_at": datetime.now(timezone.utc).isoformat()}])
    if SIGN_FLIP_LOG_FILE.exists():
        existing = pd.read_csv(SIGN_FLIP_LOG_FILE)
        existing = existing[existing["file"] != file]
        combined = pd.concat([existing, entry], ignore_index=True)
    else:
        combined = entry
    combined.to_csv(SIGN_FLIP_LOG_FILE, index=False)


# ---------------- PATH/WIDTH EXCLUSION REGIONS ----------------
# Box/lasso select mechanics ported from the retired 10_fix_surveys.py -- see this
# module's docstring item 4 for what an exclusion region actually does at Accept time.

def region_mask(x, y, regions):
    """Boolean array, True where (x[i], y[i]) falls inside ANY accumulated region (box or
    lasso, both stored in the same UTM coords the points are in). Vectorized via
    shapely.contains_xy -- fast enough to run against a full-resolution multi-hundred-
    thousand-point array at Accept time, not just the downsampled preview."""
    if not regions:
        return np.zeros(len(x), dtype=bool)
    hit = np.zeros(len(x), dtype=bool)
    for r in regions:
        if r["type"] == "box":
            poly = shapely.box(r["x0"], r["y0"], r["x1"], r["y1"])
        else:
            coords = list(zip(r["x"], r["y"]))
            if len(coords) < 3:
                continue
            poly = shapely.Polygon(coords)
        hit |= shapely.contains_xy(poly, x, y)
    return hit


def selection_to_region(selected_data):
    """selectedData from a dcc.Graph box- or lasso-select event -> a region dict, or None
    if selectedData is empty/not a real drag (e.g. a bare click)."""
    if not selected_data:
        return None
    if "range" in selected_data:  # box select
        rx = selected_data["range"]["x"]
        ry = selected_data["range"]["y"]
        return {"type": "box", "x0": min(rx), "x1": max(rx), "y0": min(ry), "y1": max(ry)}
    if "lassoPoints" in selected_data:  # lasso select
        lp = selected_data["lassoPoints"]
        if len(lp["x"]) < 3:
            return None
        return {"type": "lasso", "x": lp["x"], "y": lp["y"]}
    return None


# ---------------- FIGURE ----------------

def build_survey_figure(points_utm, regions):
    """Plain scatter of survey points colored by depth -- no basemap, fixed aspect
    ratio and axis range locked to the survey's own bounds: a static plot to eyeball the
    depth pattern and point layout, not a map to navigate. Box/lasso select stays on
    (dragmode="select") so a path/width exclusion region can be drawn immediately; points
    falling in any pending region are dropped from the display as a live preview of what
    Accept would exclude (see module docstring item 4) -- not from the survey itself."""
    x = points_utm.geometry.x.values
    y = points_utm.geometry.y.values
    depth = points_utm["depth_ft"].values
    keep = ~region_mask(x, y, regions)
    x, y, depth = x[keep], y[keep], depth[keep]
    minx, miny, maxx, maxy = points_utm.total_bounds

    fig = go.Figure(go.Scattergl(
        x=x, y=y, mode="markers",
        marker=dict(
            size=4, color=depth,
            colorscale=DEPTH_COLORSCALE, cmin=DEPTH_COLOR_MIN, cmax=DEPTH_COLOR_MAX,
            colorbar=dict(title="Depth (ft)", tickfont=dict(color="white"),
                           title_font=dict(color="white")),
        ),
        hovertext=[f"{d:.1f} ft" for d in depth],
        hoverinfo="text",
    ))
    fig.update_layout(
        xaxis=dict(range=[minx, maxx], visible=False),
        yaxis=dict(range=[miny, maxy], visible=False,
                    scaleanchor="x", scaleratio=1),
        margin=dict(l=0, r=0, t=10, b=0),
        plot_bgcolor="#1a1a1a", paper_bgcolor="#1a1a1a",
        showlegend=False, dragmode="select",
    )
    return fig, int(keep.sum()), int((~keep).sum())


# ---------------- APP ----------------
app = Dash(__name__)

DUPLICATE_WARNING_STYLE = {
    "display": "inline-block", "padding": "4px 14px", "border-radius": "12px",
    "background": "#fff3cd", "color": "#7a5b00", "font-weight": "bold",
    "border": "1px solid #a50026", "margin-left": "14px",
}
CLASSIFICATION_STYLE = {
    "display": "inline-block", "padding": "4px 14px", "border-radius": "12px",
    "background": "#2166ac", "color": "white", "font-weight": "bold",
    "margin-right": "14px",
}
BTN_STYLE = {"padding": "8px 16px", "color": "white", "border": "none", "margin-right": "10px"}


def _duplicate_warning(row, df_all):
    """Span describing a possible source-data duplicate flagged by
    4_compute_thresh_depth.py (same survey location + date as another row, which
    USACE's eHydro occasionally republishes under a second job ID), or None if this
    survey wasn't flagged."""
    dup_of = row.get("duplicate_of")
    if pd.isna(dup_of) or not str(dup_of).strip():
        return None
    match_files = [f.strip() for f in str(dup_of).split(",") if f.strip()]
    match_rows = df_all[df_all["file"].isin(match_files)]
    parts = [f"{m['file']} (confirmed={m['confirmed']})" for _, m in match_rows.iterrows()]
    desc = "; ".join(parts) if parts else ", ".join(match_files)
    return html.Span(f"⚠ Possible duplicate of: {desc}", style=DUPLICATE_WARNING_STYLE)


initial_pending = get_pending_files()

app.layout = html.Div(
    style={"font-family": "Arial, sans-serif", "padding": "12px",
           "background": "#1a1a1a", "color": "white", "min-height": "100vh"},
    children=[
        dcc.Store(id="pending-store", data=initial_pending),
        dcc.Store(id="index-store", data=0),
        dcc.Store(id="regions-store", data=[]),

        html.H3(id="queue-status"),
        html.Div(id="survey-header", style={"margin-bottom": "10px"}),
        html.Div("Drag a box, or switch to lasso in the plot's toolbar, to draw region(s), then pick what "
                 "they mean below before Accept. Every point still counts for depth_ft, this review, and "
                 "depth polygons (4/9) regardless of Region mode -- these only affect path/width finding "
                 "(7/8) and the interpolation hull. \"Exclude selected\": drop the drawn region(s), keep "
                 "everything else -- e.g. an overlapping cross-section line. \"Keep ONLY selected\": the "
                 "inverse -- keep only the drawn region(s), drop everything else -- e.g. a stray sliver of "
                 "unrelated coverage stretching the hull. \"Split into 2 chunks\": the drawn region(s)' "
                 "union becomes a brand new chunk A survey, everything else becomes chunk B.",
                 style={"color": "#aaaaaa", "margin-bottom": "8px", "font-style": "italic"}),

        dcc.Graph(id="survey-plot", style={"height": "600px", "width": "100%"},
                  config={"displayModeBar": True, "scrollZoom": False,
                          "modeBarButtonsToRemove": ["autoScale2d"]}),

        html.Div(
            style={"display": "flex", "gap": "20px", "align-items": "center", "margin-top": "14px", "flex-wrap": "wrap"},
            children=[
                dcc.Checklist(
                    id="flip-sign-checkbox",
                    options=[{"label": " Flip sign (preview)", "value": "flip"}],
                    value=[],
                ),
                html.Div(
                    style={"display": "flex", "align-items": "center", "gap": "8px"},
                    children=[
                        html.Span("Region mode:", style={"color": "#aaaaaa"}),
                        dcc.RadioItems(
                            id="region-mode",
                            options=[
                                {"label": " Exclude selected", "value": "exclude"},
                                {"label": " Keep ONLY selected", "value": "keep_only"},
                                {"label": " Split into 2 chunks", "value": "split"},
                            ],
                            value="exclude",
                            inline=True,
                            labelStyle={"margin-right": "10px"},
                        ),
                    ],
                ),
                html.Div(
                    id="blob-path-mode-wrapper",
                    style={"display": "none", "align-items": "center", "gap": "8px"},
                    children=[
                        html.Span("Blob path:", style={"color": "#aaaaaa"}),
                        dcc.RadioItems(
                            id="blob-path-mode",
                            options=BLOB_PATH_MODE_OPTIONS,
                            value=BLOB_PATH_MODE_DEFAULT,
                            inline=True,
                            labelStyle={"margin-right": "10px"},
                        ),
                    ],
                ),
                html.Button("Undo Last Region", id="undo-btn", n_clicks=0, style={**BTN_STYLE, "background": "#7a5b00"}),
                html.Button("Reset Regions", id="reset-btn", n_clicks=0, style={**BTN_STYLE, "background": "#7a5b00"}),
                html.Button("Save for Later", id="later-btn", n_clicks=0,
                            style={"padding": "8px 16px", "background": "#7a5b00", "color": "white", "border": "none"}),
                html.Button("Accept", id="accept-btn", n_clicks=0,
                            style={"padding": "8px 16px", "background": "#2166ac", "color": "white", "border": "none"}),
            ],
        ),

        html.Div(id="action-message", style={"margin-top": "10px", "color": "#7fb3e8"}),
    ],
)


@app.callback(
    Output("survey-plot", "figure"),
    Output("survey-header", "children"),
    Output("queue-status", "children"),
    Input("pending-store", "data"),
    Input("index-store", "data"),
    Input("flip-sign-checkbox", "value"),
    Input("regions-store", "data"),
)
def recompute(pending, index, flip_values, regions):
    if not pending:
        empty = go.Figure()
        empty.update_layout(margin=dict(l=0, r=0, t=0, b=0),
                             plot_bgcolor="#1a1a1a", paper_bgcolor="#1a1a1a")
        return empty, html.Div("No surveys pending review."), "All caught up"

    index = index % len(pending)
    file = pending[index]
    flip_sign = "flip" in (flip_values or [])

    row, points_utm = load_survey_points(file, flip_sign)
    survey_fig, _kept, hidden = build_survey_figure(points_utm, regions)

    survey_type = points_utm["survey_type"].iloc[0] if "survey_type" in points_utm.columns else None
    classification_label = CLASSIFICATION_LABELS.get(survey_type, survey_type or "untagged (pre-3_process_surveys.py tagging)")

    header_spans = [
        html.Span(f"{file}", style={"font-weight": "bold", "margin-right": "14px"}),
        html.Span(f"date: {row['date']}", style={"margin-right": "14px"}),
        html.Span(f"depth range: {points_utm['depth_ft'].min():.1f}–{points_utm['depth_ft'].max():.1f} ft",
                  style={"margin-right": "14px"}),
        html.Span(classification_label, style=CLASSIFICATION_STYLE),
    ]
    if survey_type == "full_coverage":
        _, factor = _multibeam_downsample_where(file)
        showing_txt = "showing every point (multibeam)" if factor == 1 \
            else f"showing 1-in-{factor} points (multibeam)"
        header_spans.append(html.Span(showing_txt,
                                       style={"margin-right": "14px", "color": "#aaaaaa", "font-style": "italic"}))
    if regions:
        header_spans.append(html.Span(f"{hidden:,} pt(s) marked for path/width exclusion",
                                       style={"margin-right": "14px", "color": "#ffa500"}))
    dup_warning = _duplicate_warning(row, load_bathym_fixed())
    if dup_warning is not None:
        header_spans.append(dup_warning)
    header = html.Div(header_spans)
    status = f"Survey {index + 1} of {len(pending)} pending"

    return survey_fig, header, status


@app.callback(
    Output("blob-path-mode", "value"),
    Output("blob-path-mode-wrapper", "style"),
    Input("pending-store", "data"),
    Input("index-store", "data"),
)
def update_blob_path_mode_control(pending, index):
    """Shows the orientation radio only for full_coverage surveys, and resets it to
    "Original" whenever the current survey changes -- deliberately NOT wired to
    flip-sign-checkbox (unlike the recompute callback below) so toggling that preview
    doesn't clobber an orientation choice already made for this survey."""
    hidden = {"display": "none"}
    shown = {"display": "flex", "align-items": "center", "gap": "8px"}
    if not pending:
        return BLOB_PATH_MODE_DEFAULT, hidden
    index = index % len(pending)
    file = pending[index]
    if _get_survey_type(file) == "full_coverage":
        return BLOB_PATH_MODE_DEFAULT, shown
    return BLOB_PATH_MODE_DEFAULT, hidden


@app.callback(
    Output("region-mode", "value"),
    Input("pending-store", "data"),
    Input("index-store", "data"),
)
def reset_region_mode(pending, index):
    """Reset Region mode to "Exclude selected" whenever the current survey changes, same
    reasoning as update_blob_path_mode_control's reset -- a leftover Split/Keep-only
    choice shouldn't silently apply to the next survey in the queue."""
    return "exclude"


@app.callback(
    Output("regions-store", "data", allow_duplicate=True),
    Input("survey-plot", "selectedData"),
    State("regions-store", "data"),
    prevent_initial_call=True,
)
def add_region(selected_data, regions):
    region = selection_to_region(selected_data)
    if region is None:
        return regions
    return (regions or []) + [region]


@app.callback(
    Output("regions-store", "data", allow_duplicate=True),
    Input("undo-btn", "n_clicks"),
    State("regions-store", "data"),
    prevent_initial_call=True,
)
def undo_region(n_clicks, regions):
    return (regions or [])[:-1]


@app.callback(
    Output("regions-store", "data", allow_duplicate=True),
    Input("reset-btn", "n_clicks"),
    prevent_initial_call=True,
)
def reset_regions(n_clicks):
    return []


@app.callback(
    Output("pending-store", "data", allow_duplicate=True),
    Output("index-store", "data", allow_duplicate=True),
    Output("action-message", "children", allow_duplicate=True),
    Output("regions-store", "data", allow_duplicate=True),
    Input("later-btn", "n_clicks"),
    State("pending-store", "data"),
    State("index-store", "data"),
    prevent_initial_call=True,
)
def save_for_later(n_clicks, pending, index):
    """Set this survey aside for manual handling: move its raw gpkg out of NAVD88_DIR
    (so 7/8/9's own glob() stops seeing it, and it drops out of the review queue) and
    mark confirmed="deferred" (excluded from the live map same as "no"/"rejected"). Any
    path/width exclusion regions drawn for it are discarded -- undrawable once it's back
    in the queue, same as they never applied to anything on disk until Accept."""
    if not pending:
        return pending, index, "Nothing to save.", []

    index = index % len(pending)
    file = pending[index]

    SURVEYS_TO_REVIEW_DIR.mkdir(parents=True, exist_ok=True)
    src = NAVD88_DIR / file
    if src.exists():
        src.rename(SURVEYS_TO_REVIEW_DIR / file)

    df = load_bathym_fixed()
    df.loc[df["file"] == file, "confirmed"] = "deferred"
    df.to_csv(BATHYM_FIXED_FILE, index=False)

    new_pending = get_pending_files()
    new_index = min(index, max(len(new_pending) - 1, 0))
    message = f"Saved {file} for later ({SURVEYS_TO_REVIEW_DIR.name}/)." if new_pending else "Saved for later. No surveys left pending."
    return new_pending, new_index, message, []


@app.callback(
    Output("pending-store", "data"),
    Output("index-store", "data", allow_duplicate=True),
    Output("action-message", "children"),
    Output("regions-store", "data", allow_duplicate=True),
    Input("accept-btn", "n_clicks"),
    State("pending-store", "data"),
    State("index-store", "data"),
    State("flip-sign-checkbox", "value"),
    State("blob-path-mode", "value"),
    State("regions-store", "data"),
    State("region-mode", "value"),
    prevent_initial_call=True,
)
def accept_survey(n_clicks, pending, index, flip_values, blob_path_mode, regions, region_mode):
    if not pending:
        return pending, index, "Nothing to accept.", regions

    index = index % len(pending)
    file = pending[index]
    flip_sign = "flip" in (flip_values or [])
    is_blob = _get_survey_type(file) == "full_coverage"

    df = load_bathym_fixed()
    mask = df["file"] == file
    row = df.loc[mask].iloc[0]
    water_elev = float(row["water_elev"])

    if region_mode == "split":
        if not regions:
            return pending, index, (
                "Draw a region around one cluster first (its contents become chunk A), "
                "or switch Region mode back to Exclude selected."
            ), regions

        gpkg_path = NAVD88_DIR / file
        gdf_full = gpd.read_file(gpkg_path)  # EPSG:3857
        gdf_full_utm = gdf_full.to_crs(UTM_CRS)
        mask_a = region_mask(gdf_full_utm.geometry.x.values, gdf_full_utm.geometry.y.values, regions)
        n_a, n_b = int(mask_a.sum()), int((~mask_a).sum())
        if n_a == 0 or n_b == 0:
            return pending, index, (
                "Refusing to split: the drawn region(s) must contain some but not all "
                "points (everything landed on one side). Adjust and try again."
            ), regions

        if flip_sign:
            gdf_full["Z_navd88"] = -gdf_full["Z_navd88"]
            if "depth_ft" in gdf_full.columns:
                gdf_full["depth_ft"] = water_elev - gdf_full["Z_navd88"]
            log_sign_flip(file)

        survey_id = file.replace("_SurveyPoint.gpkg", "")
        is_lm = survey_id.upper().startswith("LM_")
        new_rows = []
        chunk_summary = []
        for suffix, chunk_mask in (("A", mask_a), ("B", ~mask_a)):
            chunk_file = f"{survey_id}_{suffix}_SurveyPoint.gpkg"
            gdf_chunk = gdf_full.loc[chunk_mask].reset_index(drop=True)
            gdf_chunk.to_file(NAVD88_DIR / chunk_file, driver="GPKG")

            chunk_wgs84 = gdf_chunk.to_crs("EPSG:4326")
            minx, miny, maxx, maxy = chunk_wgs84.total_bounds
            midpoint_utm = gpd.GeoSeries(
                [Point((minx + maxx) / 2, (miny + maxy) / 2)], crs="EPSG:4326").to_crs(UTM_CRS).iloc[0]
            mile = _stage7.nearest_mile(midpoint_utm, is_lm)

            new_rows.append(build_split_row(row, chunk_file, chunk_wgs84, mile))
            chunk_summary.append(f"{survey_id}_{suffix} ({len(gdf_chunk):,} pts)")

        SPLIT_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        gpkg_path.rename(SPLIT_ARCHIVE_DIR / file)

        df.loc[mask, "confirmed"] = "split"
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
        df.to_csv(BATHYM_FIXED_FILE, index=False)

        new_pending = get_pending_files()
        new_index = min(index, max(len(new_pending) - 1, 0))
        message = (f"Split {file} into {' and '.join(chunk_summary)}. Original archived to "
                   f"{SPLIT_ARCHIVE_DIR.name}/; both chunks confirmed=yes, ready for stage 7.")
        if not new_pending:
            message += " No surveys left pending."
        return new_pending, new_index, message, []

    if is_blob and blob_path_mode == "neither":
        # Neither starting-line orientation fits this survey -- same "set aside"
        # shape as Save for Later, but its own folder (BLOB_TO_REVIEW_DIR) since the
        # full_coverage classification itself was fine, just not path-findable yet.
        # Any exclusion regions drawn are discarded along with everything else here --
        # this survey isn't being accepted, so there's nothing for them to apply to.
        df = _ensure_blob_path_mode_column(df)
        BLOB_TO_REVIEW_DIR.mkdir(parents=True, exist_ok=True)
        src = NAVD88_DIR / file
        if src.exists():
            src.rename(BLOB_TO_REVIEW_DIR / file)
        df.loc[mask, "confirmed"] = "deferred"
        df.loc[mask, "blob_path_mode"] = "neither"
        df.to_csv(BATHYM_FIXED_FILE, index=False)

        new_pending = get_pending_files()
        new_index = min(index, max(len(new_pending) - 1, 0))
        message = (f"Sent {file} to {BLOB_TO_REVIEW_DIR.name}/ (neither orientation fit)."
                   if new_pending else f"Sent to {BLOB_TO_REVIEW_DIR.name}/. No surveys left pending.")
        return new_pending, new_index, message, []

    # Path/width exclusion/inclusion regions (module docstring item 4): read once, up
    # front, and validated BEFORE any sign-flip write below -- a selection that would
    # keep zero points aborts the whole Accept cleanly, with nothing on disk touched yet.
    keep_only = region_mode == "keep_only"
    gpkg_path = NAVD88_DIR / file
    need_full_read = flip_sign or bool(regions)
    gdf_kept = None
    trim_note = ""
    gdf_full = None
    if need_full_read:
        gdf_full = gpd.read_file(gpkg_path)  # EPSG:3857

        if regions:
            gdf_full_utm = gdf_full.to_crs(UTM_CRS)
            in_region = region_mask(gdf_full_utm.geometry.x.values, gdf_full_utm.geometry.y.values, regions)
            n_total = len(gdf_full)
            keep_mask = in_region if keep_only else ~in_region
            n_dropped = int((~keep_mask).sum())
            gdf_kept = gdf_full.loc[keep_mask].reset_index(drop=True)
            if len(gdf_kept) == 0:
                verb = "keep only the drawn region(s), dropping" if keep_only else "exclude"
                return pending, index, (
                    f"Refusing to accept: this would {verb} all {n_total:,} points from "
                    f"path/width finding. Undo/Reset the drawn region(s) and try again."
                ), regions
            trim_note = (f", {n_dropped:,} pt(s) dropped from path/width finding only "
                         f"({'kept only drawn region(s)' if keep_only else 'excluded drawn region(s)'})")

        if flip_sign:
            gdf_full["Z_navd88"] = -gdf_full["Z_navd88"]
            # depth_ft was written by 4_compute_thresh_depth.py off the un-flipped Z --
            # recompute it here so 7_compute_navigable_width.py reads a value consistent
            # with the corrected sign, not a stale inverted one
            if "depth_ft" in gdf_full.columns:
                gdf_full["depth_ft"] = water_elev - gdf_full["Z_navd88"]
            gdf_full.to_file(gpkg_path, driver="GPKG")
            log_sign_flip(file)
            if gdf_kept is not None:
                # keep the trim sidecar's Z/depth consistent with the flip just
                # applied to the full survey, without a second disk read
                gdf_kept["Z_navd88"] = -gdf_kept["Z_navd88"]
                if "depth_ft" in gdf_kept.columns:
                    gdf_kept["depth_ft"] = water_elev - gdf_kept["Z_navd88"]

        if gdf_kept is not None:
            TRIM_DIR.mkdir(parents=True, exist_ok=True)
            gdf_kept.to_file(TRIM_DIR / file, driver="GPKG")

    df.loc[mask, "confirmed"] = "yes"
    if is_blob:
        df = _ensure_blob_path_mode_column(df)
        df.loc[mask, "blob_path_mode"] = blob_path_mode or BLOB_PATH_MODE_DEFAULT
    df.to_csv(BATHYM_FIXED_FILE, index=False)

    new_pending = get_pending_files()
    new_index = min(index, max(len(new_pending) - 1, 0))
    message = f"Accepted {file}{trim_note}." if new_pending else f"Accepted{trim_note}. No surveys left pending."
    return new_pending, new_index, message, []


if __name__ == "__main__":
    # dev_tools_ui off: the devtools bar (error badge + "update available" check)
    # pins itself to the bottom of the page and covers the accept/save-for-later controls
    app.run(debug=True, dev_tools_ui=False, host="0.0.0.0", port=8060)

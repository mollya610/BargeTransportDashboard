"""
Local QA tool: fast pre-flight review pass before 6_make_depth_polygons.py runs on
freshly-downloaded surveys.

Run after 4_compute_thresh_depth.py (stage 4) -- the first point depth_ft/water_elev
exist to plot by depth -- and before 6_make_depth_polygons.py, which gates on
confirmed=="yes" for exactly the thing this tool checks:

  Sign check: USACE's eHydro source occasionally publishes a survey with Z_navd88's
  sign flipped relative to every neighboring survey -- points colored by depth should
  show a channel (deep/blue) with shallower banks (shallow/red); a flipped survey
  shows that pattern inverted. Toggle "Flip sign" to preview Z_navd88 negated; if that
  looks right, Accept while it's checked to patch Z_navd88 (and recompute depth_ft to
  match) in the gpkg permanently, and log the file to SIGN_FLIP_LOG_FILE (a running
  record of every survey that needed the fix).

A survey that doesn't look right for any other reason should be set aside with "Save
for Later" instead of Accept: its raw gpkg is moved to SURVEYS_TO_REVIEW_DIR and
confirmed is set to "deferred" -- excluded from the live map (same as "no"/"rejected")
and from get_pending_files(), so it stops cluttering the review queue without blocking
the rest of the batch. Nothing downstream distinguishes "deferred" from "no"/"rejected"
-- it's on bathym_fixed.csv purely so a person can filter for these later and decide by
hand.

RETIRED (2026-09-21): this tool used to also cover classification-fit sanity-checking,
blob-path-orientation overrides, path/width point exclusion regions, and splitting a
survey into two chunks -- all removed along with the navigable-width/width-by-stage
stages that consumed them. Sign check + Accept/Save for Later is now this tool's whole
job. See git history for the removed code if any of it needs reviving.

This replaces the old sign-flip-only version of this tool (see old_survey_review.py,
since deleted) -- that whole Scattermap/basemap review is gone in favor of a plain,
non-interactive depth-colored scatter (no tile dependency, no pan/zoom to fight with)
since checking a point pattern by eye doesn't need a real-world map underneath it.

Local-only tool, never deployed -- runs on its own port, separate from app.py.
"""

from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import pandas as pd
import plotly.graph_objects as go
import pyogrio
from dash import Dash, dcc, html, Input, Output, State

# ---------------- CONFIG ----------------
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
NAVD88_DIR = SCRIPT_DIR / "data" / "NAVD88Files"
# Temporary: while this app was scoped to the 2022 channel-width survey list, "Save for
# Later" wrote into its own dedicated folder so Molly's 2022 set-asides didn't mix in
# with unrelated deferrals. Kept as the default set-aside location.
SURVEYS_TO_REVIEW_DIR = SCRIPT_DIR / "data" / "2022toFix"
BATHYM_FIXED_FILE = REPO_ROOT / "bathym_fixed.csv"
SIGN_FLIP_LOG_FILE = REPO_ROOT / "sign_flipped_surveys.csv"

UTM_CRS = "EPSG:26915"

# Depth colorscale range: this is a triage tool for spotting an inverted sign at a
# glance, not precise depth reading, so a fixed continuous range covering the typical
# channel/bank depth spread is enough -- values outside [DEPTH_COLOR_MIN,
# DEPTH_COLOR_MAX] just clip to the nearest end color. Plotly's "RdYlBu" colorscale
# runs red (low) -> yellow -> blue (high), matching the shallow=red/deep=blue
# convention app.py and the old review tool both use -- a flipped survey's channel
# reads red instead of blue, which is the whole point of this check.
DEPTH_COLOR_MIN = 5
DEPTH_COLOR_MAX = 25
DEPTH_COLORSCALE = "RdYlBu"


# ---------------- DATA ----------------

# Temporary: full_coverage (multibeam) surveys can carry hundreds of thousands of
# points, which makes both the queue scan and the per-survey plot painfully slow. Skip
# them here for now so the queue stays responsive for cross_sections/sparse_lines --
# set back to set() (or remove the filter in get_pending_files below) once that's fixed.
EXCLUDE_SURVEY_TYPES = set()

_survey_type_cache = {}


def load_bathym_fixed():
    return pd.read_csv(BATHYM_FIXED_FILE)


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


def get_pending_files():
    """Surveys awaiting review: confirmed=="no", their raw points still on disk (not yet
    moved to SURVEYS_TO_REVIEW_DIR by a previous "Save for Later"), and not one of
    EXCLUDE_SURVEY_TYPES (see comment above)."""
    df = load_bathym_fixed()
    pending = df.loc[df["confirmed"] == "no", "file"].tolist()
    present = [f for f in pending if (NAVD88_DIR / f).exists()]
    if EXCLUDE_SURVEY_TYPES:
        present = [f for f in present if _get_survey_type(f) not in EXCLUDE_SURVEY_TYPES]
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
# Scaled to the survey's OWN point count (not a fixed factor): pyogrio.read_info gives
# the real feature count almost instantly (no data read), so the factor is picked to
# land near MULTIBEAM_RENDER_TARGET_PTS regardless of how big or small the survey
# actually is -- smaller full_coverage surveys render at or near full resolution
# automatically.
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


# ---------------- FIGURE ----------------

def build_survey_figure(points_utm):
    """Plain scatter of survey points colored by depth -- no basemap, fixed aspect
    ratio and axis range locked to the survey's own bounds: a static plot to eyeball the
    depth pattern, not a map to navigate."""
    x = points_utm.geometry.x.values
    y = points_utm.geometry.y.values
    depth = points_utm["depth_ft"].values
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
        showlegend=False,
    )
    return fig


# ---------------- APP ----------------
app = Dash(__name__)

DUPLICATE_WARNING_STYLE = {
    "display": "inline-block", "padding": "4px 14px", "border-radius": "12px",
    "background": "#fff3cd", "color": "#7a5b00", "font-weight": "bold",
    "border": "1px solid #a50026", "margin-left": "14px",
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

        html.H3(id="queue-status"),
        html.Div(id="survey-header", style={"margin-bottom": "10px"}),

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
)
def recompute(pending, index, flip_values):
    if not pending:
        empty = go.Figure()
        empty.update_layout(margin=dict(l=0, r=0, t=0, b=0),
                             plot_bgcolor="#1a1a1a", paper_bgcolor="#1a1a1a")
        return empty, html.Div("No surveys pending review."), "All caught up"

    index = index % len(pending)
    file = pending[index]
    flip_sign = "flip" in (flip_values or [])

    row, points_utm = load_survey_points(file, flip_sign)
    survey_fig = build_survey_figure(points_utm)

    survey_type = points_utm["survey_type"].iloc[0] if "survey_type" in points_utm.columns else None

    header_spans = [
        html.Span(f"{file}", style={"font-weight": "bold", "margin-right": "14px"}),
        html.Span(f"date: {row['date']}", style={"margin-right": "14px"}),
        html.Span(f"depth range: {points_utm['depth_ft'].min():.1f}–{points_utm['depth_ft'].max():.1f} ft",
                  style={"margin-right": "14px"}),
    ]
    if survey_type == "full_coverage":
        _, factor = _multibeam_downsample_where(file)
        showing_txt = "showing every point (multibeam)" if factor == 1 \
            else f"showing 1-in-{factor} points (multibeam)"
        header_spans.append(html.Span(showing_txt,
                                       style={"margin-right": "14px", "color": "#aaaaaa", "font-style": "italic"}))
    dup_warning = _duplicate_warning(row, load_bathym_fixed())
    if dup_warning is not None:
        header_spans.append(dup_warning)
    header = html.Div(header_spans)
    status = f"Survey {index + 1} of {len(pending)} pending"

    return survey_fig, header, status


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
    """Set this survey aside for manual handling: move its raw gpkg out of NAVD88_DIR
    (so it drops out of the review queue) and mark confirmed="deferred" (excluded from
    the live map same as "no"/"rejected")."""
    if not pending:
        return pending, index, "Nothing to save."

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
    return new_pending, new_index, message


@app.callback(
    Output("pending-store", "data"),
    Output("index-store", "data", allow_duplicate=True),
    Output("action-message", "children"),
    Input("accept-btn", "n_clicks"),
    State("pending-store", "data"),
    State("index-store", "data"),
    State("flip-sign-checkbox", "value"),
    prevent_initial_call=True,
)
def accept_survey(n_clicks, pending, index, flip_values):
    if not pending:
        return pending, index, "Nothing to accept."

    index = index % len(pending)
    file = pending[index]
    flip_sign = "flip" in (flip_values or [])

    df = load_bathym_fixed()
    mask = df["file"] == file
    row = df.loc[mask].iloc[0]
    water_elev = float(row["water_elev"])

    if flip_sign:
        gpkg_path = NAVD88_DIR / file
        gdf_full = gpd.read_file(gpkg_path)  # EPSG:3857
        gdf_full["Z_navd88"] = -gdf_full["Z_navd88"]
        # depth_ft was written by 4_compute_thresh_depth.py off the un-flipped Z --
        # recompute it here so downstream stages read a value consistent with the
        # corrected sign, not a stale inverted one
        if "depth_ft" in gdf_full.columns:
            gdf_full["depth_ft"] = water_elev - gdf_full["Z_navd88"]
        gdf_full.to_file(gpkg_path, driver="GPKG")
        log_sign_flip(file)

    df.loc[mask, "confirmed"] = "yes"
    df.to_csv(BATHYM_FIXED_FILE, index=False)

    new_pending = get_pending_files()
    new_index = min(index, max(len(new_pending) - 1, 0))
    message = f"Accepted {file}." if new_pending else "Accepted. No surveys left pending."
    return new_pending, new_index, message


if __name__ == "__main__":
    # dev_tools_ui off: the devtools bar (error badge + "update available" check)
    # pins itself to the bottom of the page and covers the accept/save-for-later controls
    app.run(debug=True, dev_tools_ui=False, host="0.0.0.0", port=8060)

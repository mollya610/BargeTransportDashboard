"""
Local QA tool: sign-flip review pass for the 2015-2020 historic surveys in
HistoricDepthPolygons/ (see build_historic_survey_data.py).

Unlike 5_review_surveys.py, these surveys have no raw NAVD88Files point data and no
confirmed/pending column in bathym_fixed.csv to check against -- only the
already-dissolved depth-bin polygons build_historic_survey_data.py wrote to
data/DepthPolygons/. So the check here is done against those polygons directly, colored
with the same per-survey depth gradient app_survey_map.py's click-through view uses (see
app.py's _bin_colors_for_survey): a real survey should show a deep/blue channel with
shallow/red banks -- a flipped one shows that pattern inverted. "Flip sign" previews the
inversion by negating each bin's range before recomputing the gradient/hover labels --
there's no raw Z_navd88 to patch here (unlike 5_review_surveys.py), so Confirm never
touches the polygon file itself, only the two logs below.

Confirm logs the decision to historic_sign_review_progress.csv (this tool's own
progress tracker, kept in update_bathym/ rather than inside HistoricDepthPolygons/ since
Molly periodically deletes and replaces that whole folder wholesale -- a tracker living
inside it would lose all review progress the next time that happens) and, only if "Flip
sign" was checked, appends the survey_id to sign_flipped_surveys.csv, the same running
record 5_review_surveys.py's Accept writes to (keyed there by raw gpkg filename; keyed
here by survey_id, since that's all a historic survey has). Nothing else happens either
way -- no raw gpkg exists here to patch, and historic survey data isn't gated by a
confirmed column anywhere.

Skip Survey is for one that still looks questionable and needs a second look later
rather than a flip/no-flip decision now: it copies the survey's depth-polygon geojson
into HistoricSurveysToReview/ (a copy, not a move -- the original stays in
data/DepthPolygons/ so app_survey_map.py's click-through view is unaffected) and logs it
to the same progress tracker with status "skipped" instead of "confirmed", so it drops
out of this tool's queue without ever being logged as a sign-flip decision.

Local-only tool, never deployed -- runs on its own port.
"""
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, State

# app.py lives at the repo root, one level up from this script (unlike
# app_survey_map.py, which is colocated with it) -- add it to sys.path so `import app`
# resolves regardless of cwd.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import app as base  # reuse the per-survey gradient logic app_survey_map.py's click-through view uses

# ---------------- CONFIG ----------------
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
HIST_CSV = SCRIPT_DIR / "HistoricDepthPolygons" / "historic_surveys_combined.csv"
POLY_DIR = SCRIPT_DIR / "data" / "DepthPolygons"
PROGRESS_FILE = SCRIPT_DIR / "historic_sign_review_progress.csv"
SIGN_FLIP_LOG_FILE = REPO_ROOT / "sign_flipped_surveys.csv"
SKIP_DIR = SCRIPT_DIR / "HistoricSurveysToReview"

UTM_CRS = "EPSG:26915"


# ---------------- DATA ----------------

def load_survey_list():
    return pd.read_csv(HIST_CSV)


def load_progress():
    if not PROGRESS_FILE.exists():
        return pd.DataFrame(columns=["survey_id", "reviewed_at", "status", "flipped"])
    progress = pd.read_csv(PROGRESS_FILE)
    if "status" not in progress.columns:
        # migrate rows written before Skip Survey existed -- they were all Confirm
        progress["status"] = "confirmed"
    return progress


def get_pending_survey_ids():
    """Historic survey_ids not yet reviewed by this tool (confirmed OR skipped),
    restricted to ones that actually have a depth-polygon file (build_historic_survey_data.py
    skips any survey missing its source geojson, so the combined CSV can list a few that
    never got one)."""
    surveys = load_survey_list()
    reviewed = set(load_progress()["survey_id"])
    return [
        sid for sid in surveys["survey_id"]
        if sid not in reviewed and (POLY_DIR / f"{sid}_depth_polygons.geojson").exists()
    ]


def log_progress(survey_id, status, flipped=None):
    entry = pd.DataFrame([{
        "survey_id": survey_id,
        "reviewed_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "flipped": flipped,
    }])
    progress = load_progress()
    progress = progress[progress["survey_id"] != survey_id]
    pd.concat([progress, entry], ignore_index=True).to_csv(PROGRESS_FILE, index=False)


def skip_for_later(survey_id):
    """Copy (not move) this survey's depth-polygon geojson into SKIP_DIR -- see module
    docstring."""
    SKIP_DIR.mkdir(parents=True, exist_ok=True)
    src = POLY_DIR / f"{survey_id}_depth_polygons.geojson"
    if src.exists():
        shutil.copy2(src, SKIP_DIR / src.name)


def log_sign_flip(survey_id):
    """Same running record 5_review_surveys.py's Accept writes to -- dedup on `file`."""
    entry = pd.DataFrame([{"file": survey_id, "flipped_at": datetime.now(timezone.utc).isoformat()}])
    if SIGN_FLIP_LOG_FILE.exists():
        existing = pd.read_csv(SIGN_FLIP_LOG_FILE)
        existing = existing[existing["file"] != survey_id]
        combined = pd.concat([existing, entry], ignore_index=True)
    else:
        combined = entry
    combined.to_csv(SIGN_FLIP_LOG_FILE, index=False)


def _flip_label(label, flip):
    """'5-10 ft' -> '-10--5 ft' when flip is checked -- negating the range is the
    polygon-only equivalent of 5_review_surveys.py negating raw Z_navd88, since there's
    no point data here to actually renegate."""
    if not flip:
        return label
    m = base._HISTORIC_BIN_RANGE_RE.match(str(label))
    if not m:
        return label
    lo, hi = float(m.group(1)), float(m.group(2))
    new_lo, new_hi = -hi, -lo
    return f"{new_lo:g}-{new_hi:g} ft"


# ---------------- FIGURE ----------------

def _geom_to_xy(geom):
    xs, ys = [], []
    polys = list(geom.geoms) if geom.geom_type == "MultiPolygon" else [geom]
    for poly in polys:
        for coord in poly.exterior.coords:
            xs.append(coord[0])
            ys.append(coord[1])
        xs.append(None)
        ys.append(None)
    return xs, ys


def build_survey_figure(survey_id, flip_sign):
    """Plain filled-polygon plot of a survey's depth bins, colored by
    app._bin_colors_for_survey -- same no-basemap, fixed-aspect-ratio style as
    5_review_surveys.py's point scatter, just polygons instead of points."""
    gdf = gpd.read_file(POLY_DIR / f"{survey_id}_depth_polygons.geojson").to_crs(UTM_CRS)
    gdf = gdf.sort_values("bin_order")  # z-order only -- unaffected by the flip preview

    labels = [_flip_label(lbl, flip_sign) for lbl in gdf["depth_bin"]]
    color_map = base._bin_colors_for_survey([(lbl, None, None) for lbl in labels]) or {}

    fig = go.Figure()
    for lbl, geom in zip(labels, gdf.geometry):
        xs, ys = _geom_to_xy(geom)
        color = color_map.get(lbl, "#888888")
        fig.add_trace(go.Scatter(
            x=xs, y=ys, mode="lines", fill="toself",
            fillcolor=color, line=dict(width=1, color=color), opacity=0.85,
            name=lbl, hoverinfo="text", hovertext=f"<b>{lbl}</b>",
            showlegend=False,
        ))

    minx, miny, maxx, maxy = gdf.total_bounds
    fig.update_layout(
        xaxis=dict(range=[minx, maxx], visible=False),
        yaxis=dict(range=[miny, maxy], visible=False, scaleanchor="x", scaleratio=1),
        margin=dict(l=0, r=0, t=10, b=0),
        plot_bgcolor="#1a1a1a", paper_bgcolor="#1a1a1a",
        showlegend=False,
    )
    legend_pairs = [(lbl, color_map.get(lbl, "#888888")) for lbl in labels]
    return fig, legend_pairs


# ---------------- APP ----------------
app = Dash(__name__)

BTN_STYLE = {"padding": "8px 16px", "color": "white", "border": "none", "margin-right": "10px"}

initial_pending = get_pending_survey_ids()

app.layout = html.Div(
    style={"font-family": "Arial, sans-serif", "padding": "12px",
           "background": "#1a1a1a", "color": "white", "min-height": "100vh"},
    children=[
        dcc.Store(id="pending-store", data=initial_pending),
        dcc.Store(id="index-store", data=0),

        html.H3(id="queue-status"),
        html.Div(id="survey-header", style={"margin-bottom": "10px"}),

        html.Div(
            style={"display": "flex", "gap": "16px"},
            children=[
                dcc.Graph(id="survey-plot", style={"height": "600px", "flex": "1"},
                          config={"displayModeBar": True, "scrollZoom": False,
                                  "modeBarButtonsToRemove": ["autoScale2d"]}),
                html.Div(id="survey-legend", style={"width": "160px", "padding-top": "10px"}),
            ],
        ),

        html.Div(
            style={"display": "flex", "gap": "20px", "align-items": "center", "margin-top": "14px", "flex-wrap": "wrap"},
            children=[
                dcc.Checklist(
                    id="flip-sign-checkbox",
                    options=[{"label": " Flip sign (preview)", "value": "flip"}],
                    value=[],
                ),
                html.Button("Skip Survey", id="skip-btn", n_clicks=0,
                            style={**BTN_STYLE, "background": "#7a5b00"}),
                html.Button("Confirm", id="confirm-btn", n_clicks=0,
                            style={**BTN_STYLE, "background": "#2166ac"}),
            ],
        ),

        html.Div(id="action-message", style={"margin-top": "10px", "color": "#7fb3e8"}),
    ],
)


@app.callback(
    Output("survey-plot", "figure"),
    Output("survey-legend", "children"),
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
        return empty, [], html.Div("No historic surveys pending review."), "All caught up"

    index = index % len(pending)
    survey_id = pending[index]
    flip_sign = "flip" in (flip_values or [])

    row = load_survey_list().set_index("survey_id").loc[survey_id]
    fig, legend_pairs = build_survey_figure(survey_id, flip_sign)

    depths = []
    for lbl, _ in legend_pairs:
        m = base._HISTORIC_BIN_RANGE_RE.match(lbl)
        if m:
            depths.extend([float(m.group(1)), float(m.group(2))])
    depth_range_txt = f"{min(depths):.0f}–{max(depths):.0f} ft" if depths else "n/a"

    header = html.Div([
        html.Span(f"{survey_id}", style={"font-weight": "bold", "margin-right": "14px"}),
        html.Span(f"date: {row['date']}", style={"margin-right": "14px"}),
        html.Span(f"depth range: {depth_range_txt}", style={"margin-right": "14px"}),
    ])
    status = f"Survey {index + 1} of {len(pending)} pending"

    legend = [
        html.Div(
            style={"display": "flex", "align-items": "center", "margin-bottom": "4px"},
            children=[
                html.Span(style={
                    "display": "inline-block", "width": "16px", "height": "12px",
                    "background": color, "border-radius": "2px", "flex-shrink": "0",
                }),
                html.Span(lbl, style={"font-size": "11px", "margin-left": "6px"}),
            ],
        )
        for lbl, color in legend_pairs
    ]

    return fig, legend, header, status


@app.callback(
    Output("pending-store", "data"),
    Output("index-store", "data", allow_duplicate=True),
    Output("action-message", "children"),
    Output("flip-sign-checkbox", "value"),
    Input("confirm-btn", "n_clicks"),
    State("pending-store", "data"),
    State("index-store", "data"),
    State("flip-sign-checkbox", "value"),
    prevent_initial_call=True,
)
def confirm_survey(n_clicks, pending, index, flip_values):
    if not pending:
        return pending, index, "Nothing to confirm.", []

    index = index % len(pending)
    survey_id = pending[index]
    flip_sign = "flip" in (flip_values or [])

    log_progress(survey_id, status="confirmed", flipped=flip_sign)
    if flip_sign:
        log_sign_flip(survey_id)

    new_pending = get_pending_survey_ids()
    new_index = min(index, max(len(new_pending) - 1, 0))
    if flip_sign:
        message = f"Confirmed {survey_id} -- added to sign flip list."
    else:
        message = f"Confirmed {survey_id}."
    if not new_pending:
        message += " No surveys left pending."
    return new_pending, new_index, message, []


@app.callback(
    Output("pending-store", "data", allow_duplicate=True),
    Output("index-store", "data", allow_duplicate=True),
    Output("action-message", "children", allow_duplicate=True),
    Output("flip-sign-checkbox", "value", allow_duplicate=True),
    Input("skip-btn", "n_clicks"),
    State("pending-store", "data"),
    State("index-store", "data"),
    prevent_initial_call=True,
)
def skip_survey(n_clicks, pending, index):
    if not pending:
        return pending, index, "Nothing to skip.", []

    index = index % len(pending)
    survey_id = pending[index]

    skip_for_later(survey_id)
    log_progress(survey_id, status="skipped")

    new_pending = get_pending_survey_ids()
    new_index = min(index, max(len(new_pending) - 1, 0))
    message = f"Skipped {survey_id} -- copied to {SKIP_DIR.name}/ for later."
    if not new_pending:
        message += " No surveys left pending."
    return new_pending, new_index, message, []


if __name__ == "__main__":
    # dev_tools_ui off: the devtools bar (error badge + "update available" check)
    # pins itself to the bottom of the page and covers the confirm controls
    app.run(debug=True, dev_tools_ui=False, host="0.0.0.0", port=8063)

"""
RETIRED -- superseded by 6_review_surveys.py, which covers this tool's sign-flip check
plus a survey-classification-fit check (accept vs. save-for-later), replacing this
tool's Scattermap/basemap review with a plain static depth-colored scatter. Kept here
for reference only; not part of the pipeline anymore.

Local QA tool: catch a survey with an inverted Z_navd88 sign before it's confirmed.

Run after 4_compute_thresh_depth.py (stage 4) and before 7_compute_navigable_width.py
-- that stage (and 8_compute_width_by_stage.py, which reruns its path-finding) gate on
confirmed=="yes" specifically because of this: a sign-flipped survey's depth_ft is
inverted (wet reads as dry and vice versa), so anything computed from it before the flip
is caught and fixed would be meaningless. Must also run before 9_make_depth_polygons.py,
which deletes its own raw inputs once it processes them -- unreviewed surveys need to be
looked at here first.

This is NOT the old risk-classification review (that whole workflow -- AIS-density
overlay, nav-path percentile, low/medium/high tiers, problem-point marking -- is retired;
7_compute_navigable_width.py's objective vessel_path_connected/width_ft drives the map's
risk coloring now, no manual judgment call needed). The only thing left to eyeball by
hand is something a script can't safely infer: whether a survey's depth reads backwards.
USACE's eHydro source occasionally publishes a survey with Z_navd88's sign flipped
relative to every neighboring survey -- points colored by depth should show a channel
(deep, blue-ish here) with shallower banks; a flipped survey shows that pattern
inverted, which is usually obvious once you're looking at it.

For each survey with confirmed=="no" in bathym_fixed.csv, shows its raw points colored
by depth (same bins/colors app.py uses). Toggle "Flip sign" to preview Z_navd88 negated
-- if that looks right instead, Approve while it's checked to patch Z_navd88 (and
recompute depth_ft to match) in the gpkg permanently, then set confirmed="yes". Reject
excludes a survey for good (confirmed="rejected", raw gpkg deleted -- nothing downstream
needs it). Skip leaves it pending for a later pass.

Local-only tool, never deployed -- runs on its own port, separate from app.py.
"""

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, State, ctx, no_update
from shapely.geometry import Point

# ---------------- CONFIG ----------------
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
NAVD88_DIR = SCRIPT_DIR / "data" / "NAVD88Files"
BATHYM_FIXED_FILE = REPO_ROOT / "bathym_fixed.csv"

UTM_CRS = "EPSG:26915"

# Same depth bins/colors as 5_resolve_duplicates.py / app.py's DEPTH_POLY_COLORS,
# duplicated here rather than imported since this runs its own top-level Dash-app setup
# on import. Keep in sync by hand if the bins/colors ever change. Coarser named bands on
# purpose (not 9_make_depth_polygons.py's whole-foot bins) -- this is for spotting an
# inverted pattern at a glance, not precise depth reading.
DEPTH_BINS = [
    (20,   None,  ">20 ft"),
    (17.5, 20,    "17.5-20 ft"),
    (15,   17.5,  "15-17.5 ft"),
    (14,   15,    "14-15 ft"),
    (13,   14,    "13-14 ft"),
    (12,   13,    "12-13 ft"),
    (11,   12,    "11-12 ft"),
    (10,   11,    "10-11 ft"),
    (9,    10,    "9-10 ft"),
    (8,    9,     "8-9 ft"),
    (7,    8,     "7-8 ft"),
    (6,    7,     "6-7 ft"),
    (5,    6,     "5-6 ft"),
    (None, 5,     "<5 ft"),
]
BIN_ORDER = {label: i for i, (_, _, label) in enumerate(DEPTH_BINS)}
DEPTH_POLY_COLORS = {
    ">20 ft":      "#084594",
    "17.5-20 ft":  "#2171b5",
    "15-17.5 ft":  "#4292c6",
    "14-15 ft":    "#74c476",
    "13-14 ft":    "#a1d99b",
    "12-13 ft":    "#fee08b",
    "11-12 ft":    "#fdae61",
    "10-11 ft":    "#f46d43",
    "9-10 ft":     "#d73027",
    "8-9 ft":      "#a50026",
    "7-8 ft":      "#7b0000",
    "6-7 ft":      "#9e0142",
    "5-6 ft":      "#6a0136",
    "<5 ft":       "#3d0026",
}


def assign_depth_bin(depth):
    for lo, hi, label in DEPTH_BINS:
        if lo is not None and depth < lo:
            continue
        if hi is not None and depth >= hi:
            continue
        return label
    return None


# ---------------- DATA ----------------

def load_bathym_fixed():
    return pd.read_csv(BATHYM_FIXED_FILE)


def get_pending_files():
    """Surveys awaiting review: confirmed=="no" and their raw points still on disk."""
    df = load_bathym_fixed()
    pending = df.loc[df["confirmed"] == "no", "file"].tolist()
    return [f for f in pending if (NAVD88_DIR / f).exists()]


def load_survey_points(file, flip_sign):
    """Raw survey points for `file`, reprojected to UTM, with a depth_ft column --
    recomputed from water_elev/Z_navd88 here (not read off the gpkg's own depth_ft
    column) so the flip-sign checkbox can preview a flip without touching disk."""
    row = load_bathym_fixed().set_index("file").loc[file]
    gdf = gpd.read_file(NAVD88_DIR / file)  # EPSG:3857
    gdf_utm = gdf.to_crs(UTM_CRS)
    water_elev = float(row["water_elev"])
    z = -gdf_utm["Z_navd88"] if flip_sign else gdf_utm["Z_navd88"]
    gdf_utm["depth_ft"] = water_elev - z
    return row, gdf_utm


def _geom_to_lonlat(geom):
    if geom is None or geom.is_empty:
        return [], []
    lons, lats = [], []
    polys = list(geom.geoms) if geom.geom_type == "MultiPolygon" else [geom]
    for poly in polys:
        for coord in poly.exterior.coords:
            lons.append(coord[0])
            lats.append(coord[1])
        lons.append(None)
        lats.append(None)
    return lons, lats


# ---------------- FIGURE ----------------

def build_survey_figure(points_utm, center, uirevision=None):
    points_4326 = points_utm.to_crs(4326)
    lons = points_4326.geometry.x
    lats = points_4326.geometry.y
    depth = points_utm["depth_ft"]
    bins = depth.apply(assign_depth_bin)

    fig = go.Figure()
    for label in sorted(bins.dropna().unique(), key=lambda l: BIN_ORDER.get(l, 999)):
        mask = bins == label
        fig.add_trace(go.Scattermap(
            lon=lons[mask], lat=lats[mask], mode="markers",
            marker=dict(size=6, color=DEPTH_POLY_COLORS.get(label, "#888888")),
            hovertext=[f"{d:.1f} ft" for d in depth[mask]],
            hoverinfo="text",
            name=label,
        ))
    fig.update_layout(
        map=dict(style="carto-darkmatter", zoom=13, center=center, uirevision=uirevision),
        margin=dict(l=0, r=0, t=0, b=0),
        showlegend=True,
        legend=dict(
            x=0.02, y=0.98, xanchor="left", yanchor="top",
            bgcolor="rgba(0,0,0,0.55)", font=dict(color="white", size=10),
            title=dict(text="Depth", font=dict(size=11, color="white")),
        ),
    )
    return fig


# ---------------- APP ----------------
app = Dash(__name__)

DUPLICATE_WARNING_STYLE = {
    "display": "inline-block", "padding": "4px 14px", "border-radius": "12px",
    "background": "#fff3cd", "color": "#7a5b00", "font-weight": "bold",
    "border": "1px solid #a50026", "margin-left": "14px",
}


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
    style={"font-family": "Arial, sans-serif", "padding": "12px"},
    children=[
        dcc.Store(id="pending-store", data=initial_pending),
        dcc.Store(id="index-store", data=0),

        html.H3(id="queue-status"),
        html.Div(id="survey-header", style={"margin-bottom": "10px"}),

        dcc.Graph(id="survey-map", style={"height": "600px", "width": "100%"}),

        html.Div(
            style={"display": "flex", "gap": "20px", "align-items": "center", "margin-top": "14px"},
            children=[
                dcc.Checklist(
                    id="flip-sign-checkbox",
                    options=[{"label": " Flip sign (preview)", "value": "flip"}],
                    value=[],
                ),
                html.Button("Skip", id="skip-btn", n_clicks=0, style={"padding": "8px 16px"}),
                html.Button("Reject & Next", id="reject-btn", n_clicks=0,
                            style={"padding": "8px 16px", "background": "#a50026", "color": "white", "border": "none"}),
                html.Button("Approve & Next", id="approve-btn", n_clicks=0,
                            style={"padding": "8px 16px", "background": "#2166ac", "color": "white", "border": "none"}),
            ],
        ),

        html.Div(id="action-message", style={"margin-top": "10px", "color": "#2166ac"}),
    ],
)


@app.callback(
    Output("survey-map", "figure"),
    Output("survey-header", "children"),
    Output("queue-status", "children"),
    Input("pending-store", "data"),
    Input("index-store", "data"),
    Input("flip-sign-checkbox", "value"),
)
def recompute(pending, index, flip_values):
    if not pending:
        empty = go.Figure()
        empty.update_layout(map=dict(style="carto-darkmatter"), margin=dict(l=0, r=0, t=0, b=0))
        return empty, html.Div("No surveys pending review."), "All caught up"

    index = index % len(pending)
    file = pending[index]
    flip_sign = "flip" in (flip_values or [])

    row, points_utm = load_survey_points(file, flip_sign)

    # centroid of a point-union is just the mean of the points -- computing it this way
    # instead of union_all().centroid avoids GEOS dissolving potentially millions of
    # points into one geometry, which is impractically slow for the largest surveys
    center_utm = Point(points_utm.geometry.x.mean(), points_utm.geometry.y.mean())
    center_pt = gpd.GeoSeries([center_utm], crs=UTM_CRS).to_crs(4326).iloc[0]
    center = dict(lat=center_pt.y, lon=center_pt.x)

    # uirevision keyed to the file: keeps the operator's current pan/zoom when the
    # figure is just rebuilt for a flip-sign toggle, but resets when moving to a
    # different survey
    survey_fig = build_survey_figure(points_utm, center, uirevision=file)

    header_spans = [
        html.Span(f"{file}", style={"font-weight": "bold", "margin-right": "14px"}),
        html.Span(f"date: {row['date']}", style={"margin-right": "14px"}),
        html.Span(f"depth range: {points_utm['depth_ft'].min():.1f}–{points_utm['depth_ft'].max():.1f} ft",
                  style={"margin-right": "14px"}),
    ]
    dup_warning = _duplicate_warning(row, load_bathym_fixed())
    if dup_warning is not None:
        header_spans.append(dup_warning)
    header = html.Div(header_spans)
    status = f"Survey {index + 1} of {len(pending)} pending"

    return survey_fig, header, status


@app.callback(
    Output("index-store", "data", allow_duplicate=True),
    Input("skip-btn", "n_clicks"),
    State("pending-store", "data"),
    State("index-store", "data"),
    prevent_initial_call=True,
)
def skip_survey(n_clicks, pending, index):
    if not pending:
        return 0
    return (index + 1) % len(pending)


@app.callback(
    Output("pending-store", "data", allow_duplicate=True),
    Output("index-store", "data", allow_duplicate=True),
    Output("action-message", "children", allow_duplicate=True),
    Input("reject-btn", "n_clicks"),
    State("pending-store", "data"),
    State("index-store", "data"),
    prevent_initial_call=True,
)
def reject_survey(n_clicks, pending, index):
    """Permanently exclude a survey: mark it "rejected" (never shown on the live
    map, never re-queued for review) and delete its raw gpkg, since nothing
    downstream needs it once it's rejected. The row stays in bathym_fixed.csv
    as a record of surveys that were read in but deliberately not posted."""
    if not pending:
        return pending, index, "Nothing to reject."

    index = index % len(pending)
    file = pending[index]

    df = load_bathym_fixed()
    df.loc[df["file"] == file, "confirmed"] = "rejected"
    df.to_csv(BATHYM_FIXED_FILE, index=False)

    (NAVD88_DIR / file).unlink(missing_ok=True)

    new_pending = get_pending_files()
    new_index = min(index, max(len(new_pending) - 1, 0))
    message = f"Rejected {file}." if new_pending else "Rejected. No surveys left pending."
    return new_pending, new_index, message


@app.callback(
    Output("pending-store", "data"),
    Output("index-store", "data", allow_duplicate=True),
    Output("action-message", "children"),
    Input("approve-btn", "n_clicks"),
    State("pending-store", "data"),
    State("index-store", "data"),
    State("flip-sign-checkbox", "value"),
    prevent_initial_call=True,
)
def approve_survey(n_clicks, pending, index, flip_values):
    if not pending:
        return pending, index, "Nothing to approve."

    index = index % len(pending)
    file = pending[index]
    flip_sign = "flip" in (flip_values or [])

    df = load_bathym_fixed()
    mask = df["file"] == file
    row = df.loc[mask].iloc[0]
    water_elev = float(row["water_elev"])

    if flip_sign:
        gpkg_path = NAVD88_DIR / file
        gdf = gpd.read_file(gpkg_path)  # EPSG:3857
        gdf["Z_navd88"] = -gdf["Z_navd88"]
        # depth_ft was written by 4_compute_thresh_depth.py off the un-flipped Z --
        # recompute it here so 7_compute_navigable_width.py reads a value consistent
        # with the corrected sign, not a stale inverted one
        if "depth_ft" in gdf.columns:
            gdf["depth_ft"] = water_elev - gdf["Z_navd88"]
        gdf.to_file(gpkg_path, driver="GPKG")

    df.loc[mask, "confirmed"] = "yes"
    df.to_csv(BATHYM_FIXED_FILE, index=False)

    new_pending = get_pending_files()
    new_index = min(index, max(len(new_pending) - 1, 0))
    message = f"Approved {file}." if new_pending else "Approved. No surveys left pending."
    return new_pending, new_index, message


if __name__ == "__main__":
    # dev_tools_ui off: the devtools bar (error badge + "update available" check)
    # pins itself to the bottom of the page and covers the approve/reject controls
    app.run(debug=True, dev_tools_ui=False, host="0.0.0.0", port=8060)

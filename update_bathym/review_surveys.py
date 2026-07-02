"""
Local QA tool: review newly-ingested bathymetry surveys before they're confirmed
for the live map.

Run after compute_bathym_stats.py (stage 4) and before make_depth_polygons.py
(stage 5) -- stage 5 deletes its own raw inputs once it runs, so unreviewed
surveys must be looked at here first. (make_depth_polygons.py also gates on
confirmed=="yes" as a safety net in case it's ever run out of order.)

For each survey with confirmed=="no" in bathym_fixed.csv, shows:
  - left panel: local AIS vessel-density cells with an adjustable-percentile
    "nav-path" outline (the top cells that account for P% of local traffic)
  - right panel: the survey's raw points colored by depth, same outline overlaid
  - an auto "at risk" flag: yes if any point inside the nav-path outline is
    shallower than a threshold

Molly can tweak the percentile/threshold, override the risk flag, preview a
sign-convention fix, then Approve (writes confirmed="yes" + at_risk back to
bathym_fixed.csv, and patches Z_navd88 in the gpkg if the sign was flipped) or
Skip (leaves it pending for a later pass).

Local-only tool, never deployed -- runs on its own port, separate from app.py.
"""

import math
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, State, ctx, no_update

# ---------------- CONFIG ----------------
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DATA_DIR = SCRIPT_DIR / "data"
NAVD88_DIR = DATA_DIR / "NAVD88Files"

CORRIDOR_FILE = SCRIPT_DIR / "ais_grid_counts_clean5.csv"
BATHYM_FIXED_FILE = REPO_ROOT / "bathym_fixed.csv"

UTM_CRS = "EPSG:26915"
GRID_CELL_M = 75  # ais_grid_counts_clean5.csv cell spacing
GRID_MATCH_DIST_M = (GRID_CELL_M / 2) * math.sqrt(2)  # matches compute_bathym_stats.py's sjoin_nearest
BBOX_BUFFER_M = 500  # local AIS subset = survey bbox + this margin
NAV_BUFFER_M = 60  # bridges adjacent (incl. diagonal) 75m grid cells before dissolve
SIMPLIFY_M = 5  # same idiom as make_depth_polygons.py

DEFAULT_PERCENTILE = 80
DEFAULT_DEPTH_THRESHOLD = 13

# ---------------- LOAD SUPPORT DATA (once, at module load) ----------------
corridor_df = pd.read_csv(CORRIDOR_FILE)
corridor = gpd.GeoDataFrame(
    corridor_df[["vessel_count"]],
    geometry=gpd.points_from_xy(corridor_df["x"], corridor_df["y"]),
    crs=UTM_CRS,
)


def load_bathym_fixed():
    return pd.read_csv(BATHYM_FIXED_FILE)


def get_pending_files():
    """Surveys awaiting review: confirmed=="no" and their raw points still on disk."""
    df = load_bathym_fixed()
    pending = df.loc[df["confirmed"] == "no", "file"].tolist()
    return [f for f in pending if (NAVD88_DIR / f).exists()]


# ---------------- COMPUTATION ----------------

def _geom_to_lonlat(geom):
    """Convert a shapely Polygon/MultiPolygon (exterior only) to parallel lon/lat lists."""
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


def load_survey_points(file, flip_sign):
    """Raw survey points for `file`, reprojected to UTM, with a depth_ft column."""
    row = load_bathym_fixed().set_index("file").loc[file]
    gdf = gpd.read_file(NAVD88_DIR / file)  # EPSG:3857
    gdf_utm = gdf.to_crs(UTM_CRS)
    water_elev = float(row["water_elev"])
    if flip_sign:
        gdf_utm["depth_ft"] = water_elev + gdf_utm["Z_navd88"]
    else:
        gdf_utm["depth_ft"] = water_elev - gdf_utm["Z_navd88"]
    return row, gdf_utm


def local_corridor_subset(gdf_utm):
    minx, miny, maxx, maxy = gdf_utm.total_bounds
    return corridor.cx[minx - BBOX_BUFFER_M:maxx + BBOX_BUFFER_M, miny - BBOX_BUFFER_M:maxy + BBOX_BUFFER_M]


def compute_nav_outline(local_ais, percentile):
    """Top local AIS cells cumulative-summing to `percentile`% of local traffic,
    buffered + dissolved + simplified into one polygon (UTM). None if no local traffic."""
    if local_ais.empty or local_ais["vessel_count"].sum() <= 0:
        return None
    ranked = local_ais.sort_values("vessel_count", ascending=False)
    cum = ranked["vessel_count"].cumsum().values
    total = cum[-1]
    cutoff = (percentile / 100.0) * total
    cross_idx = min(int(np.searchsorted(cum, cutoff)), len(cum) - 1)
    selected = ranked.iloc[:cross_idx + 1]
    dissolved = selected.geometry.buffer(NAV_BUFFER_M).union_all()
    return dissolved.simplify(SIMPLIFY_M)


def compute_at_risk(nav_outline_utm, points_utm, threshold_ft):
    if nav_outline_utm is None or nav_outline_utm.is_empty:
        return "no"
    inside = points_utm[points_utm.geometry.within(nav_outline_utm)]
    if inside.empty:
        return "no"
    return "yes" if (inside["depth_ft"] <= threshold_ft).any() else "no"


def weighted_bathym_mean(gdf_utm, flip_sign):
    """Vessel-weighted mean NAVD88 elevation -- same pattern as compute_bathym_stats.py."""
    z = -gdf_utm["Z_navd88"] if flip_sign else gdf_utm["Z_navd88"]
    joined = gpd.sjoin_nearest(
        gdf_utm.assign(_z=z), corridor, max_distance=GRID_MATCH_DIST_M, distance_col="dist_to_cell"
    )
    depths = joined["_z"].values
    weights = joined["vessel_count"].values
    if len(depths) == 0 or weights.sum() <= 0:
        return np.nan
    return float(np.average(depths, weights=weights))


# ---------------- FIGURES ----------------

def build_ais_figure(local_ais, nav_outline_utm, center):
    local_ais_4326 = local_ais.to_crs(4326)
    lons = local_ais_4326.geometry.x
    lats = local_ais_4326.geometry.y
    fig = go.Figure()
    fig.add_trace(go.Scattermap(
        lon=lons, lat=lats, mode="markers",
        marker=dict(
            size=8, color=local_ais["vessel_count"], colorscale="Viridis",
            showscale=True, colorbar=dict(title="Vessels", len=0.6),
        ),
        hovertext=[f"vessel_count: {v}" for v in local_ais["vessel_count"]],
        hoverinfo="text",
        name="AIS density",
    ))
    if nav_outline_utm is not None and not nav_outline_utm.is_empty:
        outline_4326 = gpd.GeoSeries([nav_outline_utm], crs=UTM_CRS).to_crs(4326).iloc[0]
        lons_o, lats_o = _geom_to_lonlat(outline_4326)
        fig.add_trace(go.Scattermap(
            lon=lons_o, lat=lats_o, mode="lines",
            line=dict(width=2, color="#ffffff"),
            name="nav-path outline", hoverinfo="skip",
        ))
    fig.update_layout(
        map=dict(style="carto-darkmatter", zoom=13, center=center),
        margin=dict(l=0, r=0, t=0, b=0), showlegend=False,
    )
    return fig


def build_survey_figure(points_utm, nav_outline_utm, threshold_ft, center):
    points_4326 = points_utm.to_crs(4326)
    lons = points_4326.geometry.x
    lats = points_4326.geometry.y
    depth = points_utm["depth_ft"]
    sizes = np.where(depth <= threshold_ft, 9, 5)
    fig = go.Figure()
    fig.add_trace(go.Scattermap(
        lon=lons, lat=lats, mode="markers",
        marker=dict(
            size=sizes, color=depth, colorscale="RdYlBu",
            cmin=float(depth.min()), cmax=float(depth.max()),
            showscale=True, colorbar=dict(title="Depth (ft)", len=0.6),
        ),
        hovertext=[f"{d:.1f} ft" for d in depth],
        hoverinfo="text",
        name="survey points",
    ))
    if nav_outline_utm is not None and not nav_outline_utm.is_empty:
        outline_4326 = gpd.GeoSeries([nav_outline_utm], crs=UTM_CRS).to_crs(4326).iloc[0]
        lons_o, lats_o = _geom_to_lonlat(outline_4326)
        fig.add_trace(go.Scattermap(
            lon=lons_o, lat=lats_o, mode="lines",
            line=dict(width=2, color="#ffffff"),
            name="nav-path outline", hoverinfo="skip",
        ))
    fig.update_layout(
        map=dict(style="carto-darkmatter", zoom=13, center=center),
        margin=dict(l=0, r=0, t=0, b=0), showlegend=False,
    )
    return fig


# ---------------- APP ----------------
app = Dash(__name__)

BADGE_YES = {
    "display": "inline-block", "padding": "4px 14px", "border-radius": "12px",
    "background": "#a50026", "color": "white", "font-weight": "bold",
}
BADGE_NO = {
    "display": "inline-block", "padding": "4px 14px", "border-radius": "12px",
    "background": "#1a9850", "color": "white", "font-weight": "bold",
}

initial_pending = get_pending_files()

app.layout = html.Div(
    style={"font-family": "Arial, sans-serif", "padding": "12px"},
    children=[
        dcc.Store(id="pending-store", data=initial_pending),
        dcc.Store(id="index-store", data=0),

        html.H3(id="queue-status"),
        html.Div(id="survey-header", style={"margin-bottom": "10px"}),

        html.Div(
            style={"display": "flex", "gap": "10px"},
            children=[
                dcc.Graph(id="ais-map", style={"height": "480px", "width": "50%"}),
                dcc.Graph(id="survey-map", style={"height": "480px", "width": "50%"}),
            ],
        ),

        html.Div(
            style={"display": "flex", "gap": "30px", "align-items": "center", "margin-top": "14px", "flex-wrap": "wrap"},
            children=[
                html.Div([
                    html.Label("Nav-path percentile"),
                    dcc.Slider(id="percentile-slider", min=50, max=99, step=1, value=DEFAULT_PERCENTILE,
                               marks={p: str(p) for p in range(50, 100, 10)},
                               tooltip={"placement": "bottom"}),
                ], style={"width": "260px"}),

                html.Div([
                    html.Label("Depth threshold (ft)"),
                    dcc.Input(id="threshold-input", type="number", value=DEFAULT_DEPTH_THRESHOLD, step=0.5),
                ]),

                html.Div([
                    html.Label("Risk override"),
                    dcc.RadioItems(
                        id="risk-override",
                        options=[{"label": "At risk", "value": "yes"}, {"label": "Not at risk", "value": "no"}],
                        value="no", inline=True,
                    ),
                ]),

                dcc.Checklist(
                    id="flip-sign-checkbox",
                    options=[{"label": " Flip sign (preview)", "value": "flip"}],
                    value=[],
                ),

                html.Button("Skip", id="skip-btn", n_clicks=0, style={"padding": "8px 16px"}),
                html.Button("Approve & Next", id="approve-btn", n_clicks=0,
                            style={"padding": "8px 16px", "background": "#2166ac", "color": "white", "border": "none"}),
            ],
        ),

        html.Div(id="action-message", style={"margin-top": "10px", "color": "#2166ac"}),
    ],
)


@app.callback(
    Output("ais-map", "figure"),
    Output("survey-map", "figure"),
    Output("survey-header", "children"),
    Output("queue-status", "children"),
    Output("risk-override", "value"),
    Input("pending-store", "data"),
    Input("index-store", "data"),
    Input("percentile-slider", "value"),
    Input("threshold-input", "value"),
    Input("flip-sign-checkbox", "value"),
)
def recompute(pending, index, percentile, threshold, flip_values):
    if not pending:
        empty = go.Figure()
        empty.update_layout(map=dict(style="carto-darkmatter"), margin=dict(l=0, r=0, t=0, b=0))
        return empty, empty, html.Div("No surveys pending review."), "All caught up", "no"

    index = index % len(pending)
    file = pending[index]
    flip_sign = "flip" in (flip_values or [])
    threshold = DEFAULT_DEPTH_THRESHOLD if threshold is None else float(threshold)

    row, points_utm = load_survey_points(file, flip_sign)
    local_ais = local_corridor_subset(points_utm)
    nav_outline_utm = compute_nav_outline(local_ais, percentile)
    auto_flag = compute_at_risk(nav_outline_utm, points_utm, threshold)

    center_pt = gpd.GeoSeries([points_utm.union_all().centroid], crs=UTM_CRS).to_crs(4326).iloc[0]
    center = dict(lat=center_pt.y, lon=center_pt.x)

    ais_fig = build_ais_figure(local_ais, nav_outline_utm, center)
    survey_fig = build_survey_figure(points_utm, nav_outline_utm, threshold, center)

    badge_style = BADGE_YES if auto_flag == "yes" else BADGE_NO
    header = html.Div([
        html.Span(f"{file}", style={"font-weight": "bold", "margin-right": "14px"}),
        html.Span(f"date: {row['date']}", style={"margin-right": "14px"}),
        html.Span(f"stored bathym_mean: {row['bathym_mean']:.2f}, depth: {row['depth']:.2f} ft", style={"margin-right": "14px"}),
        html.Span(f"local depth range: {points_utm['depth_ft'].min():.1f}–{points_utm['depth_ft'].max():.1f} ft", style={"margin-right": "14px"}),
        html.Span(f"auto flag: {auto_flag}", style=badge_style),
    ])
    status = f"Survey {index + 1} of {len(pending)} pending"

    triggered = ctx.triggered_id
    override_value = auto_flag if triggered in ("pending-store", "index-store", None) else no_update
    return ais_fig, survey_fig, header, status, override_value


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
    Output("pending-store", "data"),
    Output("index-store", "data", allow_duplicate=True),
    Output("action-message", "children"),
    Input("approve-btn", "n_clicks"),
    State("pending-store", "data"),
    State("index-store", "data"),
    State("percentile-slider", "value"),
    State("threshold-input", "value"),
    State("flip-sign-checkbox", "value"),
    State("risk-override", "value"),
    prevent_initial_call=True,
)
def approve_survey(n_clicks, pending, index, percentile, threshold, flip_values, risk_value):
    if not pending:
        return pending, index, "Nothing to approve."

    index = index % len(pending)
    file = pending[index]
    flip_sign = "flip" in (flip_values or [])

    df = load_bathym_fixed()
    mask = df["file"] == file
    row = df.loc[mask].iloc[0]
    water_elev = float(row["water_elev"])

    # an all-blank at_risk column round-trips through CSV as NaN/float64 (compute_bathym_stats.py
    # writes "" for new rows), which then rejects a "yes"/"no" string assignment -- force object dtype
    if "at_risk" not in df.columns:
        df["at_risk"] = None
    df["at_risk"] = df["at_risk"].astype(object)

    if flip_sign:
        gpkg_path = NAVD88_DIR / file
        gdf = gpd.read_file(gpkg_path)  # EPSG:3857
        gdf["Z_navd88"] = -gdf["Z_navd88"]
        gdf.to_file(gpkg_path, driver="GPKG")

        gdf_utm = gdf.to_crs(UTM_CRS)
        new_bathym_mean = weighted_bathym_mean(gdf_utm, flip_sign=False)  # Z already flipped on disk
        df.loc[mask, "bathym_mean"] = new_bathym_mean
        df.loc[mask, "depth"] = water_elev - new_bathym_mean

    df.loc[mask, "confirmed"] = "yes"
    df.loc[mask, "at_risk"] = risk_value
    df.to_csv(BATHYM_FIXED_FILE, index=False)

    new_pending = get_pending_files()
    new_index = min(index, max(len(new_pending) - 1, 0))
    message = f"Approved {file}." if new_pending else "Approved. No surveys left pending."
    return new_pending, new_index, message


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8060)

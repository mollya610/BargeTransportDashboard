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
  - an auto risk suggestion: "medium" if any point inside the nav-path outline
    is shallower than a threshold, "low" otherwise. "high" is manual-only --
    the auto check can't tell an impassable channel from a merely narrow one,
    so Molly bumps it up herself by eye. Three tiers, judged visually:
      - high:   nav channel looks almost completely impassable at low water
      - medium: a spot in the channel looks risky but still crossable, or the
                channel looks like it's gotten narrower
      - low:    channel looks fine at low water

Clicking a point on the right-hand survey map marks the exact problem spot
within the surveyed area. If the survey is approved as medium or high risk,
that lon/lat is saved to bathym_fixed.csv (problem_lon/problem_lat) and app.py
plots the survey's dot there instead of at the survey's overall center -- low
risk surveys are unaffected and still center on the whole surveyed area.

Molly can tweak the percentile/threshold, override the risk level, preview a
sign-convention fix, then Approve (writes confirmed="yes" + at_risk back to
bathym_fixed.csv, and patches Z_navd88 in the gpkg if the sign was flipped),
Reject (writes confirmed="rejected" -- excluded from the live map and from
future review, but the row stays in bathym_fixed.csv as a record of surveys
read in but not posted -- and deletes the raw gpkg), or Skip (leaves it
pending for a later pass).

Local-only tool, never deployed -- runs on its own port, separate from app.py.
"""

import argparse
import math
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, State, ctx, no_update
from shapely.geometry import Point

# ---------------- CLI ----------------
# Two modes:
#   points   (default) -- normal review of confirmed=="no" surveys, reading raw points
#             straight out of NAVD88Files/*.gpkg. This is the original workflow.
#   polygons -- re-review of surveys that are already confirmed=="yes" for a given
#             year, re-classifying at_risk (e.g. after the 3-tier system replaced an
#             older yes/no flag) WITHOUT the raw gpkg, which stage 5 already deleted.
#             Reads the already-generated DepthPolygons/*.geojson instead -- coarser
#             (dissolved/buffered/simplified bins, not individual points) but enough
#             to eyeball nav-channel risk, and it's the same data app.py itself shows.
_parser = argparse.ArgumentParser()
_parser.add_argument("--mode", choices=["points", "polygons"], default="points")
_parser.add_argument("--year", type=int, default=None, help="required with --mode polygons")
_cli_args = _parser.parse_args()
MODE = _cli_args.mode
YEAR = _cli_args.year
if MODE == "polygons" and YEAR is None:
    raise SystemExit("--mode polygons requires --year YYYY")

# ---------------- CONFIG ----------------
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DATA_DIR = SCRIPT_DIR / "data"
NAVD88_DIR = DATA_DIR / "NAVD88Files"
DEPTH_POLY_DIR = DATA_DIR / "DepthPolygons"

CORRIDOR_FILE = SCRIPT_DIR / "ais_grid_counts_clean5.csv"
DATUM_INFO_FILE = SCRIPT_DIR / "datum_info.csv"
MILEMARKERS_FILE = SCRIPT_DIR / "usace_river_mile_markers.csv"
BATHYM_FIXED_FILE = REPO_ROOT / "bathym_fixed.csv"

UTM_CRS = "EPSG:26915"
GRID_CELL_M = 75  # ais_grid_counts_clean5.csv cell spacing
GRID_MATCH_DIST_M = (GRID_CELL_M / 2) * math.sqrt(2)  # matches compute_bathym_stats.py's sjoin_nearest
BBOX_BUFFER_M = 500  # local AIS subset = survey bbox + this margin
NAV_BUFFER_M = 60  # bridges adjacent (incl. diagonal) 75m grid cells before dissolve
POLY_BUFFER_M = 40  # per-point buffer before dissolve, matches make_depth_polygons.py's BUFFER_M
SIMPLIFY_M = 5  # same idiom as make_depth_polygons.py

DEFAULT_PERCENTILE = 80
DEFAULT_DEPTH_THRESHOLD = 13

# Same depth bins/colors as make_depth_polygons.py (stage 5) and app.py's
# DEPTH_POLY_COLORS, duplicated here rather than imported since both of those
# modules run their processing as top-level script code on import. Keep in sync
# by hand if the bins/colors ever change.
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
BIN_LOWER = {label: lo for lo, hi, label in DEPTH_BINS}
BIN_UPPER = {label: hi for lo, hi, label in DEPTH_BINS}


def bin_range_text(labels):
    """Display string for the union of depth bins present in a polygon set, e.g.
    '<5-13 ft' -- used in --mode polygons where there's no exact point min/max."""
    labels = list(labels)
    if not labels:
        return "n/a"
    # "<5 ft" is the only bin with lo=None, ">20 ft" the only one with hi=None
    if "<5 ft" in labels:
        lo_text = "<5"
    else:
        lo_text = f"{min(BIN_LOWER[l] for l in labels):.1f}"
    if ">20 ft" in labels:
        hi_text = ">20"
    else:
        hi_text = f"{max(BIN_UPPER[l] for l in labels):.1f}"
    return f"{lo_text}-{hi_text} ft"
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


def get_requeue_files(year):
    """Surveys to re-review in --mode polygons: already confirmed=="yes" for `year`
    and their depth-polygon geojson exists (stage 5 already ran and deleted the raw gpkg)."""
    df = load_bathym_fixed()
    files = df.loc[(df["confirmed"] == "yes") & (df["year"] == year), "file"].tolist()
    return [f for f in files if (DEPTH_POLY_DIR / f"{f.replace('_SurveyPoint.gpkg', '')}_depth_polygons.geojson").exists()]


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


def load_survey_polygons(file):
    """Depth-bin polygons for `file` (--mode polygons), reprojected to UTM. Used
    instead of load_survey_points once the raw gpkg has already been deleted by
    stage 5 -- coarser (dissolved/buffered bins, not individual points) but this
    is the same geojson app.py itself draws for a clicked survey."""
    row = load_bathym_fixed().set_index("file").loc[file]
    survey_id = file.replace("_SurveyPoint.gpkg", "")
    poly_path = DEPTH_POLY_DIR / f"{survey_id}_depth_polygons.geojson"
    gdf = gpd.read_file(poly_path)  # EPSG:4326
    gdf_utm = gdf.to_crs(UTM_CRS)
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


def compute_auto_risk_level(nav_outline_utm, points_utm, threshold_ft):
    """Auto-suggested risk level: "medium" if any point inside the nav-path
    outline is shallower than threshold_ft, else "low". Never suggests "high"
    -- that tier means the channel looks essentially impassable, which is a
    visual judgment call left to the reviewer."""
    if nav_outline_utm is None or nav_outline_utm.is_empty:
        return "low"
    inside = points_utm[points_utm.geometry.within(nav_outline_utm)]
    if inside.empty:
        return "low"
    return "medium" if (inside["depth_ft"] <= threshold_ft).any() else "low"


def compute_auto_risk_level_polygons(nav_outline_utm, polys_utm, threshold_ft):
    """Same idea as compute_auto_risk_level but for depth-bin polygons instead of
    raw points: "medium" if any depth bin overlapping the nav-path outline could
    contain water shallower than threshold_ft (its lower bound is below threshold,
    or it's the unbounded "<5 ft" bin), else "low". Approximate since a bin is a
    range, not an exact depth -- same "never suggests high" caveat as the point version."""
    if nav_outline_utm is None or nav_outline_utm.is_empty:
        return "low"
    inside = polys_utm[polys_utm.geometry.intersects(nav_outline_utm)]
    if inside.empty:
        return "low"
    shallow = inside["depth_bin"].map(lambda label: BIN_LOWER.get(label) is None or BIN_LOWER[label] < threshold_ft)
    return "medium" if shallow.any() else "low"


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

def build_ais_figure(local_ais, nav_outline_utm, center, uirevision=None):
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
        map=dict(style="carto-darkmatter", zoom=13, center=center, uirevision=uirevision),
        margin=dict(l=0, r=0, t=0, b=0), showlegend=False,
    )
    return fig


def build_survey_figure(points_utm, nav_outline_utm, threshold_ft, center, problem_point=None, uirevision=None):
    points_4326 = points_utm.to_crs(4326)
    lons = points_4326.geometry.x
    lats = points_4326.geometry.y
    depth = points_utm["depth_ft"]
    # larger dots below threshold (kept), color by the same discrete depth bins/palette
    # as the depth-polygon overlay on the main app (app.py's DEPTH_POLY_COLORS) instead
    # of a continuous scale, so a survey looks the same way here as it will once posted
    sizes = pd.Series(np.where(depth <= threshold_ft, 9, 5), index=depth.index)
    bins = depth.apply(assign_depth_bin)

    fig = go.Figure()
    for label in sorted(bins.dropna().unique(), key=lambda l: BIN_ORDER.get(l, 999)):
        mask = bins == label
        fig.add_trace(go.Scattermap(
            lon=lons[mask], lat=lats[mask], mode="markers",
            marker=dict(size=sizes[mask], color=DEPTH_POLY_COLORS.get(label, "#888888")),
            hovertext=[f"{d:.1f} ft" for d in depth[mask]],
            hoverinfo="text",
            name=label,
        ))
    if nav_outline_utm is not None and not nav_outline_utm.is_empty:
        outline_4326 = gpd.GeoSeries([nav_outline_utm], crs=UTM_CRS).to_crs(4326).iloc[0]
        lons_o, lats_o = _geom_to_lonlat(outline_4326)
        fig.add_trace(go.Scattermap(
            lon=lons_o, lat=lats_o, mode="lines",
            line=dict(width=2, color="#ffffff"),
            name="nav-path outline", hoverinfo="skip", showlegend=False,
        ))
    if problem_point is not None:
        fig.add_trace(go.Scattermap(
            lon=[problem_point["lon"]], lat=[problem_point["lat"]], mode="markers",
            marker=dict(size=20, color="#000000", symbol="star"),
            name="problem point", hoverinfo="skip", showlegend=False,
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


def build_survey_figure_polygons(polys_utm, nav_outline_utm, center, problem_point=None, uirevision=None):
    """Same idea as build_survey_figure, but draws the already-generated depth-bin
    polygons (filled, same style as app.py's click-through view) instead of raw
    points -- used in --mode polygons where the raw gpkg no longer exists."""
    polys_4326 = polys_utm.to_crs(4326)
    order = polys_4326["depth_bin"].map(lambda l: BIN_ORDER.get(l, 999))
    polys_4326 = polys_4326.iloc[order.sort_values().index]

    fig = go.Figure()
    for _, r in polys_4326.iterrows():
        label = r["depth_bin"]
        color = DEPTH_POLY_COLORS.get(label, "#888888")
        lons, lats = _geom_to_lonlat(r.geometry)
        fig.add_trace(go.Scattermap(
            lon=lons, lat=lats, mode="lines", fill="toself",
            fillcolor=color, line=dict(width=0), opacity=0.75,
            hoverinfo="text", hovertext=label, name=label,
        ))
    if nav_outline_utm is not None and not nav_outline_utm.is_empty:
        outline_4326 = gpd.GeoSeries([nav_outline_utm], crs=UTM_CRS).to_crs(4326).iloc[0]
        lons_o, lats_o = _geom_to_lonlat(outline_4326)
        fig.add_trace(go.Scattermap(
            lon=lons_o, lat=lats_o, mode="lines",
            line=dict(width=2, color="#ffffff"),
            name="nav-path outline", hoverinfo="skip", showlegend=False,
        ))
    if problem_point is not None:
        fig.add_trace(go.Scattermap(
            lon=[problem_point["lon"]], lat=[problem_point["lat"]], mode="markers",
            marker=dict(size=20, color="#000000", symbol="star"),
            name="problem point", hoverinfo="skip", showlegend=False,
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

BADGE_LOW = {
    "display": "inline-block", "padding": "4px 14px", "border-radius": "12px",
    "background": "#1a9850", "color": "white", "font-weight": "bold",
}
BADGE_MEDIUM = {
    "display": "inline-block", "padding": "4px 14px", "border-radius": "12px",
    "background": "#fb8c00", "color": "white", "font-weight": "bold",
}
BADGE_HIGH = {
    "display": "inline-block", "padding": "4px 14px", "border-radius": "12px",
    "background": "#a50026", "color": "white", "font-weight": "bold",
}
BADGE_BY_LEVEL = {"low": BADGE_LOW, "medium": BADGE_MEDIUM, "high": BADGE_HIGH}
DUPLICATE_WARNING_STYLE = {
    "display": "inline-block", "padding": "4px 14px", "border-radius": "12px",
    "background": "#fff3cd", "color": "#7a5b00", "font-weight": "bold",
    "border": "1px solid #a50026", "margin-left": "14px",
}


def _duplicate_warning(row, df_all):
    """Span describing a possible source-data duplicate flagged by
    compute_bathym_stats.py (same survey location + date as another row,
    which USACE's eHydro occasionally republishes under a second job ID),
    or None if this survey wasn't flagged."""
    dup_of = row.get("duplicate_of")
    if pd.isna(dup_of) or not str(dup_of).strip():
        return None
    match_files = [f.strip() for f in str(dup_of).split(",") if f.strip()]
    match_rows = df_all[df_all["file"].isin(match_files)]
    parts = []
    for _, m in match_rows.iterrows():
        depth_str = f"{m['depth']:.1f}ft" if pd.notna(m["depth"]) else "n/a"
        parts.append(f"{m['file']} (confirmed={m['confirmed']}, depth={depth_str})")
    desc = "; ".join(parts) if parts else ", ".join(match_files)
    return html.Span(f"⚠ Possible duplicate of: {desc}", style=DUPLICATE_WARNING_STYLE)

initial_pending = get_pending_files() if MODE == "points" else get_requeue_files(YEAR)
_HIDDEN = {"display": "none"}

app.layout = html.Div(
    style={"font-family": "Arial, sans-serif", "padding": "12px"},
    children=[
        dcc.Store(id="pending-store", data=initial_pending),
        dcc.Store(id="index-store", data=0),
        dcc.Store(id="problem-point-store", data=None),

        html.H3(
            f"Re-reviewing {YEAR} surveys (from DepthPolygons, no raw gpkg) -- risk only, "
            "no reject/flip-sign in this mode" if MODE == "polygons" else "",
            style={"color": "#7a5b00"} if MODE == "polygons" else {"display": "none"},
        ),
        html.H3(id="queue-status"),
        html.Div(id="survey-header", style={"margin-bottom": "10px"}),
        html.Div(
            "Click on the survey map (right) to mark roughly where the problem is -- it'll "
            "snap to the nearest depth-bin polygon vertex, not an exact survey point -- if "
            "approved as medium or high risk, that point (instead of the survey's overall "
            "center) is what shows up as the dot on the live map."
            if MODE == "polygons" else
            "Click a point on the survey map (right) to mark exactly where the problem "
            "is -- if the survey is approved as medium or high risk, that point (instead "
            "of the survey's overall center) is what shows up as the dot on the live map.",
            id="problem-point-hint",
            style={"margin-bottom": "6px", "font-size": "12px", "color": "#555"},
        ),

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
                    html.Label("Risk level"),
                    dcc.RadioItems(
                        id="risk-override",
                        options=[
                            {"label": "Low", "value": "low"},
                            {"label": "Medium", "value": "medium"},
                            {"label": "High", "value": "high"},
                        ],
                        value="low", inline=True,
                    ),
                ]),

                dcc.Checklist(
                    id="flip-sign-checkbox",
                    options=[{"label": " Flip sign (preview)", "value": "flip"}],
                    value=[],
                    style=_HIDDEN if MODE == "polygons" else {},
                ),

                html.Button("Clear problem point", id="clear-point-btn", n_clicks=0, style={"padding": "8px 16px"}),
                html.Button("Skip", id="skip-btn", n_clicks=0, style={"padding": "8px 16px"}),
                html.Button("Reject & Next", id="reject-btn", n_clicks=0,
                            style={**{"padding": "8px 16px", "background": "#a50026", "color": "white", "border": "none"},
                                   **(_HIDDEN if MODE == "polygons" else {})}),
                html.Button("Save risk & Next" if MODE == "polygons" else "Approve & Next", id="approve-btn", n_clicks=0,
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
    Input("problem-point-store", "data"),
)
def recompute(pending, index, percentile, threshold, flip_values, problem_point):
    if not pending:
        empty = go.Figure()
        empty.update_layout(map=dict(style="carto-darkmatter"), margin=dict(l=0, r=0, t=0, b=0))
        return empty, empty, html.Div("No surveys pending review."), "All caught up", "low"

    index = index % len(pending)
    file = pending[index]
    flip_sign = "flip" in (flip_values or [])
    threshold = DEFAULT_DEPTH_THRESHOLD if threshold is None else float(threshold)

    if MODE == "polygons":
        row, polys_utm = load_survey_polygons(file)
        local_ais = local_corridor_subset(polys_utm)
        nav_outline_utm = compute_nav_outline(local_ais, percentile)
        auto_level = compute_auto_risk_level_polygons(nav_outline_utm, polys_utm, threshold)

        minx, miny, maxx, maxy = polys_utm.total_bounds
        center_utm = Point((minx + maxx) / 2, (miny + maxy) / 2)
        center_pt = gpd.GeoSeries([center_utm], crs=UTM_CRS).to_crs(4326).iloc[0]
        center = dict(lat=center_pt.y, lon=center_pt.x)

        ais_fig = build_ais_figure(local_ais, nav_outline_utm, center, uirevision=file)
        survey_fig = build_survey_figure_polygons(polys_utm, nav_outline_utm, center, problem_point, uirevision=file)

        badge_style = BADGE_BY_LEVEL[auto_level]
        current_risk = row.get("at_risk")
        header_spans = [
            html.Span(f"{file}", style={"font-weight": "bold", "margin-right": "14px"}),
            html.Span(f"date: {row['date']}", style={"margin-right": "14px"}),
            html.Span(f"stored bathym_mean: {row['bathym_mean']:.2f}, depth: {row['depth']:.2f} ft", style={"margin-right": "14px"}),
            html.Span(f"depth bins present: {bin_range_text(polys_utm['depth_bin'].unique())}", style={"margin-right": "14px"}),
            html.Span(f"current at_risk: {current_risk}", style={"margin-right": "14px", "font-style": "italic"}),
            html.Span(f"auto suggestion: {auto_level}", style=badge_style),
        ]
    else:
        row, points_utm = load_survey_points(file, flip_sign)
        local_ais = local_corridor_subset(points_utm)
        nav_outline_utm = compute_nav_outline(local_ais, percentile)
        auto_level = compute_auto_risk_level(nav_outline_utm, points_utm, threshold)

        # centroid of a point-union is just the mean of the points -- computing it this way
        # instead of union_all().centroid avoids GEOS dissolving potentially millions of
        # points into one geometry, which is impractically slow for the largest surveys
        center_utm = Point(points_utm.geometry.x.mean(), points_utm.geometry.y.mean())
        center_pt = gpd.GeoSeries([center_utm], crs=UTM_CRS).to_crs(4326).iloc[0]
        center = dict(lat=center_pt.y, lon=center_pt.x)

        # uirevision keyed to the file: keeps the operator's current pan/zoom when the
        # figure is just rebuilt to add the problem-point marker or reflect a slider
        # tweak, but resets the view when moving on to a different survey
        ais_fig = build_ais_figure(local_ais, nav_outline_utm, center, uirevision=file)
        survey_fig = build_survey_figure(points_utm, nav_outline_utm, threshold, center, problem_point, uirevision=file)

        badge_style = BADGE_BY_LEVEL[auto_level]
        header_spans = [
            html.Span(f"{file}", style={"font-weight": "bold", "margin-right": "14px"}),
            html.Span(f"date: {row['date']}", style={"margin-right": "14px"}),
            html.Span(f"stored bathym_mean: {row['bathym_mean']:.2f}, depth: {row['depth']:.2f} ft", style={"margin-right": "14px"}),
            html.Span(f"local depth range: {points_utm['depth_ft'].min():.1f}–{points_utm['depth_ft'].max():.1f} ft", style={"margin-right": "14px"}),
            html.Span(f"auto suggestion: {auto_level}", style=badge_style),
        ]
    dup_warning = _duplicate_warning(row, load_bathym_fixed())
    if dup_warning is not None:
        header_spans.append(dup_warning)
    if problem_point is not None:
        header_spans.append(html.Span(
            f"problem point set: {problem_point['lat']:.5f}, {problem_point['lon']:.5f}",
            style={"margin-left": "14px", "font-style": "italic", "color": "#a50026"},
        ))
    header = html.Div(header_spans)
    status = (f"Survey {index + 1} of {len(pending)} to re-review ({YEAR})" if MODE == "polygons"
              else f"Survey {index + 1} of {len(pending)} pending")

    triggered = ctx.triggered_id
    # in polygon re-review mode, start from the survey's existing at_risk value rather
    # than the freshly-computed auto suggestion, since it's already been reviewed once
    current_risk = row.get("at_risk")
    default_risk = current_risk if MODE == "polygons" and current_risk in ("low", "medium", "high") else auto_level
    override_value = default_risk if triggered in ("pending-store", "index-store", None) else no_update
    return ais_fig, survey_fig, header, status, override_value


@app.callback(
    Output("problem-point-store", "data"),
    Input("survey-map", "clickData"),
    Input("clear-point-btn", "n_clicks"),
    Input("index-store", "data"),
    Input("pending-store", "data"),
    prevent_initial_call=True,
)
def update_problem_point(click_data, n_clear, index, pending):
    """Track the operator-marked problem spot on the survey map. Reset whenever
    the survey changes (index/pending) or the operator clears it explicitly;
    otherwise take the lon/lat of whatever survey point was just clicked."""
    triggered = ctx.triggered_id
    if triggered in ("index-store", "pending-store", "clear-point-btn"):
        return None
    if click_data and click_data.get("points"):
        pt = click_data["points"][0]
        if "lon" in pt and "lat" in pt:
            return {"lon": pt["lon"], "lat": pt["lat"]}
    return no_update


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
    if MODE == "polygons":
        return no_update, no_update, "Reject is disabled in --mode polygons re-review (survey is already live)."

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
    State("percentile-slider", "value"),
    State("threshold-input", "value"),
    State("flip-sign-checkbox", "value"),
    State("risk-override", "value"),
    State("problem-point-store", "data"),
    prevent_initial_call=True,
)
def approve_survey(n_clicks, pending, index, percentile, threshold, flip_values, risk_value, problem_point):
    if not pending:
        return pending, index, "Nothing to approve."

    index = index % len(pending)
    file = pending[index]

    df = load_bathym_fixed()
    mask = df["file"] == file

    # an all-blank at_risk/problem_lon/problem_lat column round-trips through CSV as
    # NaN/float64 (compute_bathym_stats.py writes "" for new rows), which then rejects
    # a string/float assignment later -- force object dtype (and create if missing)
    for col in ("at_risk", "problem_lon", "problem_lat"):
        if col not in df.columns:
            df[col] = None
        df[col] = df[col].astype(object)

    if MODE == "polygons":
        # no raw gpkg left to flip/repatch and confirmed is already "yes" -- just
        # update the risk classification (and problem point) for this survey
        df.loc[mask, "at_risk"] = risk_value
        if risk_value != "low" and problem_point is not None:
            df.loc[mask, "problem_lon"] = problem_point["lon"]
            df.loc[mask, "problem_lat"] = problem_point["lat"]
        else:
            df.loc[mask, "problem_lon"] = None
            df.loc[mask, "problem_lat"] = None
        df.to_csv(BATHYM_FIXED_FILE, index=False)

        # confirmed stays "yes" for this file even after saving, so re-deriving the
        # queue from the CSV (like the points-mode branch does) wouldn't drop it --
        # just remove it from the in-memory queue for this session instead
        new_pending = [f for f in pending if f != file]
        new_index = min(index, max(len(new_pending) - 1, 0))
        message = f"Saved {file} -> {risk_value}." if new_pending else "Saved. No surveys left to re-review."
        return new_pending, new_index, message

    row = df.loc[mask].iloc[0]
    water_elev = float(row["water_elev"])
    flip_sign = "flip" in (flip_values or [])

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
    # the problem point only means anything for a survey flagged medium or high risk --
    # otherwise clear it so a stale mark can't linger from an earlier pass
    if risk_value != "low" and problem_point is not None:
        df.loc[mask, "problem_lon"] = problem_point["lon"]
        df.loc[mask, "problem_lat"] = problem_point["lat"]
    else:
        df.loc[mask, "problem_lon"] = None
        df.loc[mask, "problem_lat"] = None
    df.to_csv(BATHYM_FIXED_FILE, index=False)

    new_pending = get_pending_files()
    new_index = min(index, max(len(new_pending) - 1, 0))
    message = f"Approved {file}." if new_pending else "Approved. No surveys left pending."
    return new_pending, new_index, message


if __name__ == "__main__":
    # dev_tools_ui off: the devtools bar (error badge + "update available" check)
    # pins itself to the bottom of the page and covers the risk-level/approve controls
    app.run(debug=True, dev_tools_ui=False, host="0.0.0.0", port=8060)

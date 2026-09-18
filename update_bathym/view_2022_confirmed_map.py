"""
Local QA viewer (read-only): one map showing every 2022-list survey that has fully
completed stage 8 (width-by-stage) and stage 9 (depth polygons) -- answers "does every
2022 survey I picked actually have a navigable path" at a glance, all surveys on screen
at once, rather than view_width_by_stage.py's one-dot-per-survey-you-click-to-reveal
approach (this is adapted from that script, reusing its depth-polygon color/binning
code exactly).

Restricted to update_bathym/2022_channel_width_survey_list.csv's survey_ids. For each
one that currently has BOTH a DepthPolygons/{id}_depth_polygons.geojson (stage 9) and a
WidthByStage/{id}_width_by_stage.csv (stage 8), AND is still confirmed=="yes"/
path_confirmed=="yes" in bathym_fixed.csv right now (same staleness filter
view_width_by_stage.py's load_survey_index() uses -- a survey's old stage 8/9 output
files don't delete themselves if it later gets sent back for re-review), plots:

  - that survey's whole-foot depth-bin polygons, regrouped into the same coarse
    display bands + colors app.py's live map and view_width_by_stage.py use (red
    shallow -> blue deep), at its real location.
  - its navigable path (WidthByStage/{id}_navigable_path.geojson) drawn on top in
    green.

No dots, no click interaction -- every ready survey's polygons+path are just on the map
already, since seeing the full set together (gaps included) is the whole point.

Local-only tool, never deployed -- runs on its own port.
"""

import functools
from pathlib import Path

import geopandas as gpd
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html

# ---------------- CONFIG ----------------
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DEPTH_POLY_DIR = SCRIPT_DIR / "data" / "DepthPolygons"
WIDTH_BY_STAGE_DIR = SCRIPT_DIR / "data" / "WidthByStage"
BATHYM_FIXED_FILE = REPO_ROOT / "bathym_fixed.csv"
SURVEY_LIST_FILE = SCRIPT_DIR / "2022_channel_width_survey_list.csv"

GEO_CRS = "EPSG:4326"

# same display bands + colors as view_width_by_stage.py/app.py, so a survey's polygons
# read the same here as everywhere else
DEPTH_POLY_COLORS = {
    "20+ ft":   "#084594",
    "15-20 ft": "#4292c6",
    "12-15 ft": "#74c476",
    "9-12 ft":  "#fee08b",
    "5-9 ft":   "#f46d43",
    "<5 ft":    "#a50026",
}
_DISPLAY_BINS = [
    (20,   None, "20+ ft"),
    (15,   20,   "15-20 ft"),
    (12,   15,   "12-15 ft"),
    (9,    12,   "9-12 ft"),
    (5,    9,    "5-9 ft"),
    (None, 5,    "<5 ft"),
]


def _assign_display_bin(depth):
    for lo, hi, label in _DISPLAY_BINS:
        if lo is not None and depth < lo:
            continue
        if hi is not None and depth >= hi:
            continue
        return label
    return None


def _geom_to_lonlat(geom):
    """Shapely Polygon/MultiPolygon -> parallel lon/lat lists (None-separated rings),
    so a MultiPolygon collapses to one trace instead of one per ring."""
    lons, lats = [], []
    polys = list(geom.geoms) if geom.geom_type == "MultiPolygon" else [geom]
    for poly in polys:
        for coord in poly.exterior.coords:
            lons.append(coord[0])
            lats.append(coord[1])
        lons.append(None)
        lats.append(None)
    return lons, lats


@functools.lru_cache(maxsize=256)
def _load_depth_polygon_bins(poly_path_str):
    poly_gdf = gpd.read_file(poly_path_str)
    if pd.api.types.is_numeric_dtype(poly_gdf["depth_bin"]):
        poly_gdf["depth_bin"] = poly_gdf["depth_bin"].apply(_assign_display_bin)
        poly_gdf = poly_gdf.dissolve(by="depth_bin", as_index=False)
        bin_order_map = {label: i for i, (_, _, label) in enumerate(_DISPLAY_BINS)}
        poly_gdf["bin_order"] = poly_gdf["depth_bin"].map(bin_order_map)
    poly_gdf = poly_gdf.sort_values("bin_order")
    bins = []
    for _, row in poly_gdf.iterrows():
        lons, lats = _geom_to_lonlat(row.geometry)
        bins.append((row["depth_bin"], lons, lats))
    return bins


def _depth_polygon_traces(poly_path, survey_id):
    traces = []
    for bin_label, lons, lats in _load_depth_polygon_bins(str(poly_path)):
        color = DEPTH_POLY_COLORS.get(bin_label, "#888888")
        traces.append(go.Scattermap(
            lon=lons, lat=lats, mode="lines", fill="toself",
            line=dict(width=1, color=color), fillcolor=color, opacity=0.75,
            hoverinfo="text", hovertext=f"{survey_id}<br><b>{bin_label}</b>",
            hoverlabel=dict(bgcolor=color, bordercolor=color, font=dict(color="white")),
            showlegend=False,
        ))
    return traces


def _path_trace(path_gdf, survey_id):
    line = path_gdf.to_crs(GEO_CRS).geometry.iloc[0]
    lons, lats = line.xy
    return go.Scattermap(
        lon=list(lons), lat=list(lats), mode="lines",
        line=dict(width=3, color="#39ff6a"),
        hoverinfo="text", hovertext=survey_id, showlegend=False,
    )


def load_ready_survey_ids():
    """2022-list survey_ids with both stage-8 and stage-9 output on disk, AND still
    confirmed=="yes"/path_confirmed=="yes" right now -- same staleness filter as
    view_width_by_stage.py's load_survey_index()."""
    survey_ids = pd.read_csv(SURVEY_LIST_FILE)["survey_id"].tolist()
    bathym = pd.read_csv(BATHYM_FIXED_FILE)
    bathym["survey_id"] = bathym["file"].str.replace("_SurveyPoint.gpkg", "", regex=False)
    status = bathym.set_index("survey_id")[["confirmed", "path_confirmed"]]

    ready = []
    for sid in survey_ids:
        if not (DEPTH_POLY_DIR / f"{sid}_depth_polygons.geojson").exists():
            continue
        if not (WIDTH_BY_STAGE_DIR / f"{sid}_width_by_stage.csv").exists():
            continue
        if sid not in status.index:
            continue
        row = status.loc[sid]
        if row["confirmed"] != "yes" or row["path_confirmed"] != "yes":
            continue
        ready.append(sid)
    return ready


def build_map():
    ready_ids = load_ready_survey_ids()
    fig = go.Figure()
    lons_all, lats_all = [], []
    for sid in ready_ids:
        for t in _depth_polygon_traces(DEPTH_POLY_DIR / f"{sid}_depth_polygons.geojson", sid):
            fig.add_trace(t)
            lons_all.extend(v for v in t.lon if v is not None)
            lats_all.extend(v for v in t.lat if v is not None)

        path_path = WIDTH_BY_STAGE_DIR / f"{sid}_navigable_path.geojson"
        if path_path.exists():
            path_gdf = gpd.read_file(path_path)
            if not path_gdf.empty:
                fig.add_trace(_path_trace(path_gdf, sid))

    center = dict(lon=sum(lons_all) / len(lons_all), lat=sum(lats_all) / len(lats_all)) \
        if lons_all else dict(lon=-90.0, lat=35.0)
    fig.update_layout(
        map=dict(style="carto-darkmatter", zoom=5, center=center),
        margin=dict(l=0, r=0, t=0, b=0),
        showlegend=False,
        paper_bgcolor="#1a1a1a",
    )
    return fig, ready_ids


# ---------------- APP ----------------
app = Dash(__name__)
_fig, _ready_ids = build_map()
_total_2022 = len(pd.read_csv(SURVEY_LIST_FILE))

app.layout = html.Div(
    style={"font-family": "Arial, sans-serif", "padding": "12px",
           "background": "#1a1a1a", "color": "white", "min-height": "100vh"},
    children=[
        html.H3(f"{len(_ready_ids)} of {_total_2022} 2022-list surveys fully through "
                f"stage 9 (depth polygons + width-by-stage table)"),
        dcc.Graph(figure=_fig, style={"height": "85vh", "width": "100%"},
                  config={"displayModeBar": False}),
    ],
)

if __name__ == "__main__":
    # dev_tools_ui off, same reasoning as the other review tools -- read-only viewer,
    # never deployed, own port separate from app.py/6/7b/8-viewer
    app.run(debug=True, dev_tools_ui=False, host="0.0.0.0", port=8063)

"""
Local QA viewer (read-only, no accept/reject decisions): inspect
8_compute_width_by_stage.py's output.

Top panel is an overview map with one dot per survey that has a
{survey_id}_width_by_stage.csv (i.e. has been through stages 7, 7b, and 8) -- just a
location marker, not the survey's own point cloud or footprint. Dots are red if that
survey's through_width_ft (the width at its own anchor-gage threshold -- the "initial"
scenario) is under LOW_WIDTH_FLAG_FT, green otherwise. A year dropdown filters which
dots are shown, for reviewing one year's batch of fully-processed surveys at a time.

Click a dot to load two things, drawn IN PLACE on the same map (at the survey's own
real coordinates, not a separate synthetic panel) plus a plot beside it:

  - The survey's own whole-foot depth-bin polygons (9_make_depth_polygons.py's own
    output, already reused by app.py's single-survey overlay the same way -- both are
    already in EPSG:4326, so they drop straight onto the map) as a base layer, its raw
    points colored by depth on top of that when they're still around (same
    style/colorscale as 6_review_surveys.py, including its 1-in-20 multibeam
    downsampling -- reads the raw gpkg straight from NAVD88Files/, so only works before
    stage 9 deletes it, and is reprojected from EPSG:3857 to 4326 to overlay), and the
    navigable path drawn on top of both in green. Any layer that isn't available for a
    given survey (older one, raw points already deleted, or no path yet) is just
    skipped -- whatever's there still renders. The map's `uirevision` is held constant
    so clicking a dot doesn't reset whatever pan/zoom the reviewer is currently at.
  - A width-vs-stage plot from that survey's {survey_id}_width_by_stage.csv -- the whole
    point of this viewer: seeing how a survey's minimum navigable width changes across
    the tested river-stage range, not just the single value at its anchor gage threshold
    that bathym_fixed.csv's vessel_path_width_ft holds. The anchor stage (this survey's
    actual low-water gage threshold) is marked on the plot for reference.

Local-only tool, never deployed -- runs on its own port, separate from app.py and from
6_review_surveys.py / 7b_review_navigable_path.py.
"""

import functools
from pathlib import Path

import geopandas as gpd
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, ctx

# ---------------- CONFIG ----------------
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
NAVD88_DIR = SCRIPT_DIR / "data" / "NAVD88Files"
DEPTH_POLY_DIR = SCRIPT_DIR / "data" / "DepthPolygons"
WIDTH_BY_STAGE_DIR = SCRIPT_DIR / "data" / "WidthByStage"
BATHYM_FIXED_FILE = REPO_ROOT / "bathym_fixed.csv"

GEO_CRS = "EPSG:4326"
MAP_UIREVISION = "keep-map-view"

# same triage colorscale as 6_review_surveys.py/7b_review_navigable_path.py
DEPTH_COLOR_MIN = 5
DEPTH_COLOR_MAX = 25
DEPTH_COLORSCALE = "RdYlBu"
MULTIBEAM_DOWNSAMPLE_FACTOR = 20
MULTIBEAM_DOWNSAMPLE_WHERE = f"rowid % {MULTIBEAM_DOWNSAMPLE_FACTOR} = 0"

# same risk thresholds app.py uses for its own vessel_path_width_ft coloring -- shown
# here as reference bands so a stage's width reads in the same "high/medium/low risk"
# terms the live map uses, not just as a bare number
NAVIGABLE_WIDTH_HIGH_FT = 300
NAVIGABLE_WIDTH_LOW_FT = 800

# depth-bin display bands, matching app.py's DEPTH_POLY_COLORS/_DISPLAY_BINS exactly so
# a survey's polygons read the same here as they do in the live map's single-survey
# overlay -- duplicated rather than imported since importing app.py would pull in the
# whole Dash app just for these constants.
DEPTH_POLY_COLORS = {
    "20+ ft":      "#084594",
    "15-20 ft":    "#4292c6",
    "12-15 ft":    "#74c476",
    "9-12 ft":     "#fee08b",
    "5-9 ft":      "#f46d43",
    "<5 ft":       "#a50026",
}
_DISPLAY_BINS = [
    (20,   None,  "20+ ft"),
    (15,   20,    "15-20 ft"),
    (12,   15,    "12-15 ft"),
    (9,    12,    "9-12 ft"),
    (5,    9,     "5-9 ft"),
    (None, 5,     "<5 ft"),
]


def _assign_display_bin(depth):
    for lo, hi, label in _DISPLAY_BINS:
        if lo is not None and depth < lo:
            continue
        if hi is not None and depth >= hi:
            continue
        return label
    return None


# ---------------- DATA ----------------

def load_survey_index():
    """One row per survey with a navigable_path.geojson: a representative lon/lat for
    the overview map (the path's own representative_point, not a full footprint), plus
    method/connected/through_width_ft/gage/anchor_stage_ft from that file's properties
    and date/milemarker joined in from bathym_fixed.csv."""
    rows = []
    for gj in sorted(WIDTH_BY_STAGE_DIR.glob("*_navigable_path.geojson")):
        survey_id = gj.stem.replace("_navigable_path", "")
        try:
            gdf = gpd.read_file(gj)
        except Exception:
            continue
        if gdf.empty:
            continue
        props = gdf.iloc[0]
        rep_pt = props.geometry.representative_point()
        rows.append({
            "survey_id": survey_id,
            "lon": rep_pt.x,
            "lat": rep_pt.y,
            "method": props.get("method"),
            "connected": props.get("connected"),
            "through_width_ft": props.get("through_width_ft"),
            "gage": props.get("gage"),
            "anchor_stage_ft": props.get("anchor_stage_ft"),
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    bathym = pd.read_csv(BATHYM_FIXED_FILE)
    bathym["survey_id"] = bathym["file"].str.replace("_SurveyPoint.gpkg", "", regex=False)
    df = df.merge(bathym[["survey_id", "date", "milemarker", "confirmed", "path_confirmed"]],
                  on="survey_id", how="left")

    # a survey's stage-8 output file can outlive its current status -- e.g. it was
    # path_confirmed=="yes" when 8 ran, then later sent to "Save for Later" in
    # 7b_review_navigable_path.py (path_confirmed becomes "deferred"). Filtering on
    # CURRENT status here (matching 9_make_depth_polygons.py's own gate) keeps this
    # viewer's overview map from showing stale entries nothing downstream trusts anymore.
    df = df[(df["confirmed"] == "yes") & (df["path_confirmed"] == "yes")].drop(
        columns=["confirmed", "path_confirmed"])
    return df


def load_navigable_path(survey_id):
    gj = WIDTH_BY_STAGE_DIR / f"{survey_id}_navigable_path.geojson"
    return gpd.read_file(gj) if gj.exists() else None


def load_width_by_stage(survey_id):
    csv_path = WIDTH_BY_STAGE_DIR / f"{survey_id}_width_by_stage.csv"
    return pd.read_csv(csv_path) if csv_path.exists() else None


def load_survey_raw_points(survey_id):
    """Raw survey points reprojected to EPSG:4326 (source is EPSG:3857) with depth_ft,
    downsampled the same way 6_review_surveys.py does for full_coverage (multibeam)
    surveys. Returns None once 9_make_depth_polygons.py has deleted this survey's raw
    gpkg."""
    gpkg = NAVD88_DIR / f"{survey_id}_SurveyPoint.gpkg"
    if not gpkg.exists():
        return None
    peek = gpd.read_file(gpkg, rows=1)
    survey_type = peek["survey_type"].iloc[0] if "survey_type" in peek.columns else None
    where = MULTIBEAM_DOWNSAMPLE_WHERE if survey_type == "full_coverage" else None
    gdf = gpd.read_file(gpkg, where=where)  # EPSG:3857
    return gdf.to_crs(GEO_CRS)


def find_depth_polygon_path(survey_id):
    """Fallback once raw points are gone: path to this survey's own whole-foot
    depth-bin polygons (9_make_depth_polygons.py's output) -- the same file app.py's own
    single-survey overlay reuses, so this stays consistent with what the live map
    already shows for an aged survey. Returns None if it doesn't exist; reading is left
    to _load_depth_polygon_bins so results can be cached by path."""
    gj = DEPTH_POLY_DIR / f"{survey_id}_depth_polygons.geojson"
    return gj if gj.exists() else None


# ---------------- FIGURES ----------------

LOW_WIDTH_FLAG_FT = 500


def build_overview_map(index_df):
    colors = ["#e41a1c" if w < LOW_WIDTH_FLAG_FT else "#39ff6a" for w in index_df["through_width_ft"]]
    fig = go.Figure(go.Scattermap(
        lon=index_df["lon"], lat=index_df["lat"], mode="markers",
        marker=dict(size=8, color=colors),
        customdata=index_df["survey_id"],
        hovertext=[
            f"{sid}<br>{d}<br>through-width {w:.0f} ft"
            for sid, d, w in zip(index_df["survey_id"], index_df["date"], index_df["through_width_ft"])
        ],
        hoverinfo="text",
    ))
    center = dict(lon=index_df["lon"].mean(), lat=index_df["lat"].mean())
    fig.update_layout(
        map=dict(style="carto-darkmatter", zoom=5, center=center),
        margin=dict(l=0, r=0, t=0, b=0),
        showlegend=False,
        paper_bgcolor="#1a1a1a",
        # held constant across re-renders (year filter, dot clicks) so Dash doesn't
        # snap the map back to the default center/zoom every time the figure is rebuilt
        uirevision=MAP_UIREVISION,
    )
    return fig


def _path_trace_geo(path_gdf):
    """Navigable path line, already EPSG:4326 -- drops straight onto the map at the
    survey's real location."""
    line = path_gdf.to_crs(GEO_CRS).geometry.iloc[0]
    lons, lats = line.xy
    return go.Scattermap(
        lon=list(lons), lat=list(lats), mode="lines+markers",
        line=dict(width=3, color="#39ff6a"), marker=dict(size=5, color="#39ff6a"),
        name="navigable path", hoverinfo="skip",
    )


def _geom_to_lonlat(geom):
    """Convert a shapely Polygon or MultiPolygon to parallel lon/lat lists for Scattermap
    fill -- identical to app.py's own helper of the same name, so a MultiPolygon with many
    disconnected patches still collapses to one trace (None-separated rings) instead of one
    trace per ring."""
    lons, lats = [], []
    polys = list(geom.geoms) if geom.geom_type == "MultiPolygon" else [geom]
    for poly in polys:
        for coord in poly.exterior.coords:
            lons.append(coord[0])
            lats.append(coord[1])
        lons.append(None)
        lats.append(None)
    return lons, lats


@functools.lru_cache(maxsize=32)
def _load_depth_polygon_bins(poly_path_str):
    """Parse a depth-polygon geojson into (bin_label, lons, lats) tuples, cached by path
    -- mirrors app.py's _load_depth_polygon_bins exactly, so a survey's polygons render
    the same here as they do in the live map's single-survey overlay. A raw single-survey
    file (see 9_make_depth_polygons.py) stores an exact whole-foot depth_bin at every row
    -- regroup + dissolve those into the same coarse DEPTH_POLY_COLORS display bands app.py
    uses, so one survey doesn't draw dozens to hundreds of same-colored overlapping traces
    (this is the fix for the viewer's own slow-render bug -- it used to add one trace per
    polygon *ring*, sometimes 700-1000+ per survey click)."""
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
        lons = [round(v, 6) if v is not None else None for v in lons]
        lats = [round(v, 6) if v is not None else None for v in lats]
        bins.append((row["depth_bin"], lons, lats))
    return bins


def _depth_polygon_traces_geo(poly_path):
    """One filled Scattermap trace per depth display-band (not per polygon ring) for the
    survey at `poly_path` -- see _load_depth_polygon_bins for why that distinction is the
    whole point."""
    traces = []
    for bin_label, lons, lats in _load_depth_polygon_bins(str(poly_path)):
        color = DEPTH_POLY_COLORS.get(bin_label, "#888888")
        traces.append(go.Scattermap(
            lon=lons, lat=lats, mode="lines", fill="toself",
            line=dict(width=1.5, color=color), fillcolor=color, opacity=0.75,
            name=bin_label, hoverinfo="text", hovertext=f"<b>{bin_label}</b>",
            hoverlabel=dict(bgcolor=color, bordercolor=color, font=dict(color="white")),
            showlegend=False,
        ))
    return traces


def _points_trace_geo(points_geo):
    depth = points_geo["depth_ft"]
    return go.Scattermap(
        lon=points_geo.geometry.x, lat=points_geo.geometry.y, mode="markers",
        marker=dict(
            size=5, color=depth,
            colorscale=DEPTH_COLORSCALE, cmin=DEPTH_COLOR_MIN, cmax=DEPTH_COLOR_MAX,
            colorbar=dict(title="Depth (ft)", tickfont=dict(color="white"), title_font=dict(color="white")),
        ),
        hovertext=[f"{d:.1f} ft" for d in depth],
        hoverinfo="text",
        name="survey points",
        showlegend=False,
    )


def add_survey_overlay(fig, points_geo, poly_path, path_gdf):
    """Add the clicked survey's own layers directly onto the overview map, in place at
    its real coordinates: whole-foot depth-bin polygons as a base layer (always drawn
    when available -- same as 9_make_depth_polygons.py's own output/what app.py's
    single-survey overlay shows), the survey's raw points colored by depth drawn on top
    for extra precision when they're still around, and the navigable path on top of
    everything in green. Any of the three layers can be missing (older survey, raw
    points already deleted by stage 9, or no path yet) -- whatever's available still
    renders. Mutates and returns `fig` (the overview map), drawn after the overview
    dots so the overlay sits on top of them."""
    if poly_path is not None:
        for t in _depth_polygon_traces_geo(poly_path):
            fig.add_trace(t)
    if points_geo is not None and not points_geo.empty:
        fig.add_trace(_points_trace_geo(points_geo))
    if path_gdf is not None and not path_gdf.empty:
        fig.add_trace(_path_trace_geo(path_gdf))
    return fig


def build_width_by_stage_plot(wbs_df, anchor_stage_ft):
    fig = go.Figure(go.Scatter(
        x=wbs_df["stage_ft"], y=wbs_df["width_ft"], mode="lines+markers",
        line=dict(color="#39ff6a"), marker=dict(size=5),
        name="width",
    ))
    fig.add_hrect(y0=0, y1=NAVIGABLE_WIDTH_HIGH_FT, fillcolor="#a50026", opacity=0.15, line_width=0)
    fig.add_hrect(y0=NAVIGABLE_WIDTH_HIGH_FT, y1=NAVIGABLE_WIDTH_LOW_FT, fillcolor="#fdae61", opacity=0.15, line_width=0)
    fig.add_hrect(y0=NAVIGABLE_WIDTH_LOW_FT, y1=max(wbs_df["width_ft"].max(), NAVIGABLE_WIDTH_LOW_FT) * 1.05,
                  fillcolor="#2166ac", opacity=0.15, line_width=0)
    if pd.notna(anchor_stage_ft):
        fig.add_vline(x=anchor_stage_ft, line=dict(color="white", dash="dash"),
                      annotation_text="anchor stage", annotation_font_color="white")
    fig.update_layout(
        xaxis=dict(title="river stage (ft)", color="white", gridcolor="#333"),
        yaxis=dict(title="min navigable width (ft)", color="white", gridcolor="#333"),
        margin=dict(l=60, r=20, t=20, b=50),
        plot_bgcolor="#1a1a1a", paper_bgcolor="#1a1a1a",
        font=dict(color="white"),
        showlegend=False,
    )
    return fig


# ---------------- APP ----------------
app = Dash(__name__)

_index_df = load_survey_index()
if not _index_df.empty:
    _index_df["year"] = pd.to_datetime(_index_df["date"]).dt.year
_YEAR_OPTIONS = ["All"] + sorted(_index_df["year"].dropna().unique().tolist(), reverse=True) \
    if not _index_df.empty else ["All"]

app.layout = html.Div(
    style={"font-family": "Arial, sans-serif", "padding": "12px",
           "background": "#1a1a1a", "color": "white", "min-height": "100vh"},
    children=[
        html.H3(f"{len(_index_df)} survey(s) with stage-8 output" if not _index_df.empty
                else "No stage-8 output found yet -- run 8_compute_width_by_stage.py first."),
        html.Div(
            style={"display": "flex", "align-items": "center", "gap": "10px", "margin-bottom": "8px"},
            children=[
                html.Label("Year:"),
                dcc.Dropdown(
                    id="year-filter",
                    options=[{"label": str(y), "value": str(y)} for y in _YEAR_OPTIONS],
                    value="All",
                    clearable=False,
                    style={"width": "160px", "color": "#000"},
                ),
            ],
        ),
        dcc.Graph(id="overview-map", figure=build_overview_map(_index_df) if not _index_df.empty else go.Figure(),
                  style={"height": "600px", "width": "100%"}),

        html.Div(id="survey-header", style={"margin-top": "14px", "margin-bottom": "6px", "font-weight": "bold"}),
        dcc.Graph(id="width-stage-plot", style={"height": "400px", "width": "100%"}),
    ],
)


def _year_filtered_df(year_value):
    if _index_df.empty:
        return _index_df
    return _index_df if year_value == "All" else _index_df[_index_df["year"] == int(year_value)]


@app.callback(
    Output("overview-map", "figure"),
    Output("survey-header", "children"),
    Output("width-stage-plot", "figure"),
    Input("year-filter", "value"),
    Input("overview-map", "clickData"),
)
def on_year_or_click(year_value, click_data):
    empty_plot = go.Figure()
    empty_plot.update_layout(margin=dict(l=0, r=0, t=0, b=0), plot_bgcolor="#1a1a1a", paper_bgcolor="#1a1a1a")

    df = _year_filtered_df(year_value)

    # a year change (or first load) rebuilds the dot layer fresh with no survey overlay;
    # a dot click keeps whatever dots are currently shown and adds that survey's layers
    # on top of them, so switching years always clears the previous click's overlay
    if ctx.triggered_id != "overview-map" or not click_data:
        fig = build_overview_map(df) if not df.empty else go.Figure()
        return fig, "Click a survey on the map above.", empty_plot

    survey_id = click_data["points"][0]["customdata"]
    row = _index_df.set_index("survey_id").loc[survey_id]

    path_gdf = load_navigable_path(survey_id)
    wbs_df = load_width_by_stage(survey_id)
    points_geo = load_survey_raw_points(survey_id)
    poly_path = find_depth_polygon_path(survey_id)

    fig = build_overview_map(df) if not df.empty else go.Figure()
    add_survey_overlay(fig, points_geo, poly_path, path_gdf)

    header = (
        f"{survey_id}  |  date: {row['date']}  |  mile: {row['milemarker']}  |  "
        f"method: {row['method']}  |  connected: {row['connected']}  |  "
        f"anchor ({row['gage']}) through-width: {row['through_width_ft']:.0f} ft"
    )
    if points_geo is None and poly_path is None:
        header += "  |  (no raw points or depth polygons available)"
    elif points_geo is None:
        header += "  |  (raw points gone -- showing depth-bin polygons only)"

    width_fig = build_width_by_stage_plot(wbs_df, row["anchor_stage_ft"]) if wbs_df is not None else empty_plot

    return fig, header, width_fig


if __name__ == "__main__":
    app.run(debug=True, dev_tools_ui=False, host="0.0.0.0", port=8062)

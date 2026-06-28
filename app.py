import os
import glob
import json
import textwrap
from pathlib import Path
import dash
from dash import dcc, html, Input, Output, State
import geopandas as gpd
import plotly.graph_objects as go
from shapely.ops import linemerge
from datetime import date
import numpy as np
import requests
import pandas as pd
from shapely import wkt
from PIL import Image, ImageDraw


def _build_dredge_marker():
    import fitz
    import numpy as np
    os.makedirs("assets", exist_ok=True)
    doc = fitz.open("dredgicon.pdf")
    pix = doc[0].get_pixmap(matrix=fitz.Matrix(4, 4), alpha=True)
    src = Image.frombytes("RGBA", (pix.width, pix.height), pix.samples)
    arr = np.array(src)
    # find and crop to non-white content
    non_white = np.any(arr[:, :, :3] < 240, axis=2)
    rows = np.where(non_white.any(axis=1))[0]
    cols = np.where(non_white.any(axis=0))[0]
    src = src.crop((cols[0], rows[0], cols[-1] + 1, rows[-1] + 1))
    arr = np.array(src)
    # make near-white pixels transparent
    arr[np.all(arr[:, :, :3] > 240, axis=2), 3] = 0
    Image.fromarray(arr).resize((64, 64), Image.LANCZOS).save("assets/dredge_marker.png")


def _build_shoaling_marker():
    import fitz
    import numpy as np
    os.makedirs("assets", exist_ok=True)
    doc = fitz.open("Yield.pdf")
    pix = doc[0].get_pixmap(matrix=fitz.Matrix(4, 4), alpha=True)
    src = Image.frombytes("RGBA", (pix.width, pix.height), pix.samples)
    arr = np.array(src)
    non_white = np.any(arr[:, :, :3] < 240, axis=2)
    rows = np.where(non_white.any(axis=1))[0]
    cols = np.where(non_white.any(axis=0))[0]
    src = src.crop((cols[0], rows[0], cols[-1] + 1, rows[-1] + 1))
    arr = np.array(src)
    arr[np.all(arr[:, :, :3] > 240, axis=2), 3] = 0
    Image.fromarray(arr).resize((64, 64), Image.LANCZOS).save("assets/shoaling_marker.png")


if not os.path.exists("assets/dredge_marker.png"):
    _build_dredge_marker()

if not os.path.exists("assets/shoaling_marker.png"):
    _build_shoaling_marker()


# LOAD DATA
bathy = pd.read_csv("bathym_fixed.csv")

# Ensure year is int
bathy["year"] = bathy["year"].astype(int)
bathy["date_dt"] = pd.to_datetime(bathy["date"]).dt.tz_localize(None)

years = sorted(bathy["year"].unique())
years = [int(y) for y in years]

# get center point for bathym measures
bathy["geometry"] = bathy["geometry"].apply(wkt.loads)
bathy = gpd.GeoDataFrame(bathy, geometry="geometry", crs="EPSG:4326")
bathy["rep_point"] = bathy.geometry.representative_point()
bathy["LON"] = bathy["rep_point"].apply(lambda p: p.x)
bathy["LAT"] = bathy["rep_point"].apply(lambda p: p.y)
bathy = pd.DataFrame(bathy.drop(columns=["geometry", "rep_point"]))
bathy["survey_id"] = (
    bathy["file"]
    .str.replace("_SurveyPoint.gpkg", "", regex=False)
    .str.replace("_w_datum.gpkg", "", regex=False)
    .str.replace(".gpkg", "", regex=False)
)

# survey IDs that have a depth polygon GeoJSON available for click-through detail
_DEPTH_POLY_DIR = Path("update_bathym/data/DepthPolygons")
DEPTH_POLY_FILES = {
    f.stem.replace("_depth_polygons", "")
    for f in _DEPTH_POLY_DIR.glob("*_depth_polygons.geojson")
} if _DEPTH_POLY_DIR.exists() else set()

# get navigation notices (dredging/shoaling/draft restriction/other) from the manually
# maintained notices_<year>.xlsx workbook(s) and locate them via mile markers or, for
# dredging at a named location instead of a mile marker, a manually entered lat/lon
CATEGORY_LABELS = {"dredging": "Dredging", "shoaling": "Shoaling", "draft": "Draft Restriction", "other": "Other"}
CATEGORY_COLORS = {"dredging": "#4a3000", "shoaling": "#fdd734", "draft": "#8c510a", "other": "#e6a817"}
CATEGORY_ICONS = {"dredging": "🛠️", "shoaling": "🔺", "other": "⚠️"}
DRAFT_ANNOUNCED_OPACITY = 0.45
DRAFT_IN_PLACE_OPACITY = 0.85
BIG, MED, SMALL = "18px", "14px", "11px"

# bathymetry survey points are bucketed into 3 depth bins, each with its own color/size
DEPTH_BINS = [
    ("Above 20 ft", "#2e7d32", 8),
    ("14-20 ft", "#ff9800", 13),
    ("Below 14 ft", "#e53935", 18),
]

# the workbook uses short river codes rather than the full names in the mile-marker table
RIVER_CODE_MAP = {"LMR": "MISSISSIPPI-LO", "UMR": "MISSISSIPPI-UP", "ARK": "ARKANSAS"}

mile_lookup = (
    pd.read_csv("update_bathym/usace_river_mile_markers.csv")
    .groupby(["RIVER_NAME", "MILE"])[["LON", "LAT"]].mean()
)
# ordered points per river, used to draw a smoothed range indicator between two mile
# markers instead of a single dot
river_mile_points = mile_lookup.reset_index().sort_values(["RIVER_NAME", "MILE"])

notices = pd.concat(
    [pd.read_excel(f) for f in sorted(glob.glob("notices_*.xlsx"))],
    ignore_index=True
)
notices["date_published"] = pd.to_datetime(notices["date_published"])
notices["date_start"] = pd.to_datetime(notices["date_start"])
notices["date_end"] = pd.to_datetime(notices["date_end"])
notices["cancelled_date"] = pd.to_datetime(notices["cancelled_date"])
notices["replaced_date"] = pd.to_datetime(notices["replaced_date"])
notices["year"] = notices["date_published"].dt.year
notices["date_str"] = notices["date_published"].dt.strftime("%Y-%m-%d")
notices["date_str_long"] = notices["date_published"].dt.strftime("%B %d, %Y")
notices["is_active_flag"] = notices["active?"].fillna("N").str.upper().eq("Y")
notices["river_name"] = notices["river"].map(RIVER_CODE_MAP)


def _fmt_date(d):
    return d.strftime("%b %d, %Y") if pd.notna(d) else ""


def _format_mm(row):
    lo, hi = row["mm_low"], row["mm_high"]
    if pd.isna(lo) and pd.isna(hi):
        return None
    if pd.isna(hi) or lo == hi:
        return f"Mile {lo:g}"
    return f"Miles {lo:g}–{hi:g}"


def _location_line(row):
    mm = row["mm_label"]
    detail = row.get("location_details")
    if pd.notna(mm) and pd.notna(detail):
        return f"Location: {mm}, {detail}"
    if pd.notna(mm):
        return f"Location: {mm}"
    if pd.notna(detail):
        return f"Location: {detail}"
    return ""


notices["mm_label"] = notices.apply(_format_mm, axis=1)
notices["mid_mile"] = notices[["mm_low", "mm_high"]].mean(axis=1)
notices["location_line"] = notices.apply(_location_line, axis=1)

# fill in lat/lon from the mile-marker table for any row that didn't already get one
# manually entered (the dredging rows located at a named spot like "McKellar Lake")
mile_lookup_flat = mile_lookup.reset_index().rename(
    columns={"RIVER_NAME": "river_name", "MILE": "mile_marker_round", "LON": "lon_lookup", "LAT": "lat_lookup"}
)
notices["mile_marker_round"] = notices["mid_mile"].round()
notices = notices.merge(mile_lookup_flat, on=["river_name", "mile_marker_round"], how="left")
notices["lat"] = notices["lat"].fillna(notices["lat_lookup"])
notices["lon"] = notices["lon"].fillna(notices["lon_lookup"])
notices = notices.drop(columns=["lat_lookup", "lon_lookup"])


def _smooth_path(lons, lats, window=5):
    """Light moving-average smoothing so a mile-marker range reads as a clean curve
    instead of the visibly jagged zigzag the raw ~1-mile-spaced survey points make."""
    lon_s = pd.Series(lons).rolling(window, center=True, min_periods=1).mean()
    lat_s = pd.Series(lats).rolling(window, center=True, min_periods=1).mean()
    return lon_s.tolist(), lat_s.tolist()

# get barge rate data  
url = "https://www.ams.usda.gov/sites/default/files/media/GTRFigure10Table9.xlsx"
response = requests.get(url, timeout=30)
with open('freight_rates_southbound.xlsx', 'wb') as file:
    file.write(response.content)
freight_rates = pd.read_excel('freight_rates_southbound.xlsx',sheet_name='Table 9_data',header=2,usecols=range(5))
barge_rates = freight_rates.rename(columns={'All Points':'week','ST LOUIS':'stlrate_per_ton'})
barge_rates = barge_rates.drop(index=[0,1])
barge_rates = barge_rates.loc[:,('week','stlrate_per_ton')]
barge_rates['week'] = pd.to_datetime(barge_rates['week'])
barge_rates['stlrate_per_ton'] = (barge_rates['stlrate_per_ton']*3.99)/100
barge_rates['week_no']= barge_rates['week'].dt.isocalendar().week
barge_rates['year'] = barge_rates['week'].dt.year
barge_demand = barge_rates.groupby(['week_no'])['stlrate_per_ton'].mean().reset_index().rename(columns={'stlrate_per_ton':'avg_stlrate'})
barge_std = barge_rates.groupby(['week_no'])['stlrate_per_ton'].std().reset_index().rename(columns={'stlrate_per_ton':'std_stlrate'})
barge_rates = barge_rates.merge(barge_demand,on='week_no',how='inner')
barge_rates = barge_rates.merge(barge_std,on='week_no',how='inner')
barge_rates['plusone'] = barge_rates['avg_stlrate'] + barge_rates['std_stlrate']
barge_rates['minusone'] = barge_rates['avg_stlrate'] - barge_rates['std_stlrate']

end_date = barge_rates["week"].max()
start_date = end_date - pd.Timedelta(weeks=52)
thisyear = date.today().year

# water level 
greenv = pd.read_excel('greenville_stage.xlsx',header=11,parse_dates=['Date / Time']).rename(columns={'Date / Time':'date','Stage (Ft)':'stage'}).assign(stage=lambda d: pd.to_numeric(d['stage'], errors='coerce'))[:-1]
greenv['date'] = pd.to_datetime(greenv['date'])
greenv['year'] = greenv['date'].dt.year
greenv['week_no'] = greenv['date'].dt.isocalendar().week
greenmean = greenv.groupby(['week_no'])['stage'].mean().reset_index().rename(columns={'stage':'avg_stage'})
greenstd = greenv.groupby(['week_no'])['stage'].std().reset_index().rename(columns={'stage':'std_stage'})
greenv = greenv.merge(greenmean,on='week_no',how='inner')
greenv = greenv.merge(greenstd,on='week_no',how='inner')
greenv['plusone'] = greenv['avg_stage'] + greenv['std_stage']
greenv['minusone'] = greenv['avg_stage'] - greenv['std_stage']

# now getting corn and soy price data 
url = "https://www.ams.usda.gov/sites/default/files/media/GTRTable2A_B.xlsx"
response = requests.get(url, timeout=30)
with open('price_spreads_futures_usda.xlsx', 'wb') as file:
    file.write(response.content)
corn_soy_spread = pd.read_excel('price_spreads_futures_usda.xlsx',sheet_name='Data',header=1,usecols=range(9))
corn_soy_spread = corn_soy_spread[(corn_soy_spread['Origin--destination']=='IL--Gulf')|(corn_soy_spread['Origin--destination']=='IL–Gulf')|(corn_soy_spread['Origin--destination']=='IA–Gulf')|(corn_soy_spread['Origin--destination']=='IA--Gulf')]

corn_spread = corn_soy_spread[corn_soy_spread['Commodity']=='Corn'].rename(columns = {'Unnamed: 0':'date' , 'Destination Price':'gulf_corn_price'})
corn_spread = corn_spread.loc[:,('date','gulf_corn_price')]
corn_spread['date'] = pd.to_datetime(corn_spread['date'])
corn_spread['week_no'] = corn_spread['date'].dt.isocalendar().week
corn_spread['year'] = corn_spread['date'].dt.year
corn_price = corn_spread[['date','week_no','year','gulf_corn_price']]
corn_price['month'] = corn_price['date'].dt.month
meancorn = corn_price.groupby(['month'])[['gulf_corn_price']].mean().reset_index().rename(columns={'gulf_corn_price':'avg_price'})
stdcorn = corn_price.groupby(['month'])[['gulf_corn_price']].std().reset_index().rename(columns={'gulf_corn_price':'std_price'})
corn_price = corn_price.merge(meancorn,on='month',how='inner')
corn_price = corn_price.merge(stdcorn,on='month',how='inner')
corn_price['plusone'] = corn_price['avg_price'] + corn_price['std_price']
corn_price['minusone'] = corn_price['avg_price'] - corn_price['std_price']


soy_spread = corn_soy_spread.rename(columns = {'Unnamed: 0':'date','Destination Price':'gulf_soy_price'})
soy_spread['date'] = soy_spread['date'].shift(1)
soy_spread = soy_spread[soy_spread['Commodity']=='Soybean']
soy_spread['date'] = soy_spread['date'].shift(1)
soy_spread = soy_spread.loc[:,('date','gulf_soy_price')]
soy_spread['date'] = pd.to_datetime(soy_spread['date'])
soy_spread['week_no'] = soy_spread['date'].dt.isocalendar().week
soy_spread['year'] = soy_spread['date'].dt.year
soy_price = soy_spread[['date','week_no','year','gulf_soy_price']]
soy_price['month'] = soy_price['date'].dt.month
meansoy = soy_price.groupby(['month'])[['gulf_soy_price']].mean().reset_index().rename(columns={'gulf_soy_price':'avg_price'})
stdsoy = soy_price.groupby(['month'])[['gulf_soy_price']].std().reset_index().rename(columns={'gulf_soy_price':'std_price'})
soy_price = soy_price.merge(meansoy,on='month',how='inner')
soy_price = soy_price.merge(stdsoy,on='month',how='inner')
soy_price['plusone'] = soy_price['avg_price'] + soy_price['std_price']
soy_price['minusone'] = soy_price['avg_price'] - soy_price['std_price']


# now get river line — filter to Mississippi at read time so the full shapefile is never loaded
mississippi = gpd.read_file('rivers_shapefile/rivers.shp', where="PNAME = 'MISSISSIPPI R'")
mississippi = mississippi.set_crs('EPSG:4326')
mississippi["geometry"] = mississippi.simplify(0.001)
river_line = linemerge(mississippi.union_all())
x, y = river_line.xy
river_lons_arr = np.array(list(x))
river_lats_arr = np.array(list(y))
del mississippi, river_line, x, y



# EXTRA_RIVERS = [
#     ("OHIO R",     "OHIO",     "Ohio River",     "#2166ac"),
#     ("MISSOURI R", "MISSOURI", "Missouri River", "#2166ac"),
#     ("ILLINOIS R", "ILLINOIS", "Illinois River", "#2166ac"),
#     ("ARKANSAS R", "ARKANSAS", "Arkansas River", "#2166ac"),
# ]
# extra_river_data = [
#     (display, color, *_build_river(sf, mm, display))
#     for sf, mm, display, color in EXTRA_RIVERS
# ]
extra_river_data = []


def _nearest_river_index(lon, lat):
    d2 = (river_lons_arr - lon) ** 2 + (river_lats_arr - lat) ** 2
    return int(np.argmin(d2))


def _nearest_mile_lonlat(river_name, mile):
    sub = river_mile_points[river_mile_points["RIVER_NAME"] == river_name]
    if sub.empty:
        return None
    row = sub.loc[(sub["MILE"] - mile).abs().idxmin()]
    return row["LON"], row["LAT"]


def _wrap_two_lines(text):
    text = str(text)
    if len(text) <= 40:
        return text
    mid = len(text) // 2
    left_space = text.rfind(" ", 0, mid)
    right_space = text.find(" ", mid)
    if left_space == -1 and right_space == -1:
        split_at = mid
    elif left_space == -1:
        split_at = right_space
    elif right_space == -1:
        split_at = left_space
    else:
        split_at = left_space if (mid - left_space) <= (right_space - mid) else right_space
    return text[:split_at] + "<br>" + text[split_at + 1:]

# --------------------------------------------------
# DASH APP
# --------------------------------------------------

app = dash.Dash(__name__)
app.title = "Grain Transportation Conditions"
app.index_string = """
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        <link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&display=swap" rel="stylesheet">
        {%css%}
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
"""

# --------------------------------------------------
# LAYOUT
# --------------------------------------------------

# Style constants for the slide-out plots panel
PLOTS_PANEL_WIDTH = "420px"
PLOTS_PANEL_CLOSED = {
    "position": "absolute", "top": "0", "right": "0", "height": "100%",
    "width": "0", "overflow": "hidden",
    "background": "rgba(255,255,255,0.97)",
    "box-shadow": "none",
    "transition": "width 0.25s ease",
    "zIndex": "15",
}
PLOTS_PANEL_OPEN = {
    **PLOTS_PANEL_CLOSED,
    "width": PLOTS_PANEL_WIDTH,
    "box-shadow": "-2px 0 6px rgba(0,0,0,0.3)",
    "padding": "20px",
    "overflow-y": "auto",
}

# Colors for each depth bin polygon overlay (deep → shallow)
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


def _geom_to_lonlat(geom):
    """Convert a shapely Polygon or MultiPolygon to parallel lon/lat lists for Scattermap fill."""
    lons, lats = [], []
    polys = list(geom.geoms) if geom.geom_type == "MultiPolygon" else [geom]
    for poly in polys:
        for coord in poly.exterior.coords:
            lons.append(coord[0])
            lats.append(coord[1])
        lons.append(None)
        lats.append(None)
    return lons, lats


# Style constants for the notice click-detail box, floating over the map. Shared by all
# four notice categories - clicking any dredging/shoaling/draft/other marker opens it.
NOTICE_DETAIL_HIDDEN = {"display": "none"}
NOTICE_DETAIL_VISIBLE = {
    "position": "absolute", "top": "15px", "right": "15px", "zIndex": "25",
    "width": "330px", "background": "rgba(255,255,255,0.97)",
    "padding": "16px 18px", "border-radius": "8px",
    "box-shadow": "0 2px 10px rgba(0,0,0,0.4)",
    "font-family": "Arial, sans-serif", "max-height": "80vh", "overflow-y": "auto",
}
MEMO_TEXT_HIDDEN = {"display": "none"}
MEMO_TEXT_VISIBLE = {
    "display": "block", "font-size": SMALL, "color": "#444", "margin-top": "6px",
    "padding": "8px", "background": "#f4f4f4", "border-radius": "4px",
}
SURVEY_BANNER_HIDDEN = {"display": "none"}
SURVEY_BANNER_VISIBLE = {
    "position": "absolute", "bottom": "30px", "left": "50%",
    "transform": "translateX(-50%)",
    "zIndex": "25", "background": "rgba(255,255,255,0.95)",
    "padding": "8px 16px", "border-radius": "20px",
    "box-shadow": "0 2px 8px rgba(0,0,0,0.35)",
    "font-family": "Arial, sans-serif", "display": "flex", "align-items": "center",
}

app.layout = html.Div(
    style={"width": "100%", "margin": "0", "padding": "0"},
    children=[

        # Title bar
        html.Div(
            style={
                "width": "100%",
                "padding": "15px 0",
                "background": "#2166ac",
                "text-align": "center",
            },
            children=[
                html.H2(
                    "Grain Transportation Conditions",
                    style={
                        "margin": 0, "color": "white",
                        "font-family": "'Bebas Neue', sans-serif",
                        "font-size": "26px", "letter-spacing": "1.5px",
                    }
                )
            ]
        ),

        dcc.Store(id="plots-panel-store", data=False),
        dcc.Store(id="notice-detail-store", data=None),
        dcc.Store(id="selected-survey-store", data=None),

        ##################################
        # Map fills the full width; controls and plots panel float on top of it
        html.Div(
            style={"position": "relative", "width": "100%", "height": "92vh"},
            children=[

                # Map, edge to edge
                dcc.Graph(id="map", style={"height": "100%", "width": "100%"}),

                # Notice click-detail box, top-right - shared by all four notice categories
                # (dredging/shoaling/draft/other); clicking any marker opens it
                html.Div(
                    id="notice-detail-box",
                    style=NOTICE_DETAIL_HIDDEN,
                    children=[
                        html.Button(
                            "✕", id="notice-detail-close",
                            style={
                                "position": "absolute", "top": "8px", "right": "10px",
                                "border": "none", "background": "none", "cursor": "pointer",
                                "font-size": "16px", "color": "#888",
                            }
                        ),
                        html.Div(id="notice-detail-content")
                    ]
                ),

                # Survey depth overlay banner — appears when a survey dot is clicked
                html.Div(
                    id="survey-detail-banner",
                    style={"display": "none"},
                    children=[
                        html.Span(id="survey-detail-label", style={"font-size": "13px", "font-weight": "bold", "color": "#1a237e"}),
                        html.Button(
                            "✕ Close",
                            id="survey-detail-close",
                            style={
                                "border": "none", "background": "none", "cursor": "pointer",
                                "font-size": "12px", "color": "#888", "margin-left": "12px",
                            }
                        ),
                    ]
                ),

                # Controls overlay, floating on top of the map
                html.Div(
                    style={
                        "position": "absolute",
                        "top": "15px",
                        "left": "15px",
                        "zIndex": "10",
                        "display": "flex",
                        "flex-direction": "column",
                        "gap": "10px",
                        "background": "rgba(255,255,255,0.9)",
                        "padding": "10px 15px",
                        "border-radius": "8px",
                        "box-shadow": "0 1px 4px rgba(0,0,0,0.3)"
                    },
                    children=[
                        # Year dropdown
                        html.Div(
                            style={"width": "220px"},
                            children=[
                                html.Label("Select Year"),
                                dcc.Dropdown(
                                    id="year-slider",
                                    options=[{"label": str(y), "value": y} for y in years],
                                    value=thisyear if thisyear in years else years[-1],
                                    clearable=False,
                                    style={"height": "40px", "font-size": "15px"}
                                )
                            ]
                        ),

                        # Layers checkboxes
                        html.Div(
                            style={"width": "240px"},
                            children=[
                                html.Label("Layers", style={"font-weight": "bold", "margin-bottom": "6px", "display": "block"}),
                                dcc.Checklist(
                                    id="layer-toggle",
                                    options=[
                                        {
                                            "label": html.Span([
                                                html.Div([
                                                    html.Span("Riverbed Surveys: Navigation Risk under Low Water"),
                                                    html.Div(
                                                        style={"display": "flex", "gap": "10px", "margin-top": "5px", "margin-left": "4px"},
                                                        children=[
                                                            html.Div([
                                                                html.Div(style={"width": "12px", "height": "12px", "border-radius": "50%", "background": DEPTH_BINS[0][1], "display": "inline-block", "margin-right": "4px", "vertical-align": "middle"}),
                                                                html.Span("Low", style={"font-size": "12px", "vertical-align": "middle"}),
                                                            ]),
                                                            html.Div([
                                                                html.Div(style={"width": "12px", "height": "12px", "border-radius": "50%", "background": DEPTH_BINS[1][1], "display": "inline-block", "margin-right": "4px", "vertical-align": "middle"}),
                                                                html.Span("Medium", style={"font-size": "12px", "vertical-align": "middle"}),
                                                            ]),
                                                            html.Div([
                                                                html.Div(style={"width": "12px", "height": "12px", "border-radius": "50%", "background": DEPTH_BINS[2][1], "display": "inline-block", "margin-right": "4px", "vertical-align": "middle"}),
                                                                html.Span("High", style={"font-size": "12px", "vertical-align": "middle"}),
                                                            ]),
                                                        ]
                                                    ),
                                                ])
                                            ]),
                                            "value": "bathy",
                                        },
                                        {
                                            "label": html.Span([
                                                html.Img(src="/assets/dredge_marker.png", height="22", style={"vertical-align": "middle", "margin-right": "5px"}),
                                                "Dredging",
                                            ]),
                                            "value": "dredging",
                                        },
                                        {
                                            "label": html.Span([
                                                html.Img(src="/assets/shoaling_marker.png", height="22", style={"vertical-align": "middle", "margin-right": "5px"}),
                                                "Shoaling",
                                            ]),
                                            "value": "shoaling",
                                        },
                                        {
                                            "label": html.Span([
                                                html.Div(style={
                                                    "display": "inline-block",
                                                    "width": "22px", "height": "4px",
                                                    "background": CATEGORY_COLORS["draft"],
                                                    "vertical-align": "middle",
                                                    "margin-right": "5px",
                                                    "border-radius": "2px",
                                                }),
                                                "Draft Restriction",
                                            ]),
                                            "value": "draft",
                                        },
                                    ],
                                    value=["bathy", "dredging", "shoaling", "draft"],
                                    inputStyle={"margin-right": "6px"},
                                    labelStyle={"display": "flex", "align-items": "center", "margin-bottom": "5px", "font-size": "14px"},
                                )
                            ]
                        )
                    ]
                ),



                # Tab to open/close the plots panel, on the right edge of the map
                html.Button(
                    "☰ Plots",
                    id="plots-toggle",
                    n_clicks=0,
                    style={
                        "position": "absolute",
                        "top": "50%",
                        "right": "0",
                        "transform": "translateY(-50%)",
                        "zIndex": "20",
                        "background": "#1b3a5c",
                        "color": "white",
                        "border": "none",
                        "border-radius": "6px 0 0 6px",
                        "padding": "12px 8px",
                        "cursor": "pointer",
                        "writing-mode": "vertical-rl",
                    }
                ),

                # Plots panel, slides in over the map
                html.Div(
                    id="plots-panel",
                    style=PLOTS_PANEL_CLOSED,
                    children=[
                        dcc.Graph(id="barge-rate-plot", style={"height": "300px"}),
                        dcc.Graph(id="water-plot", style={"height": "300px"}),
                        dcc.Graph(id="cornprice-plot", style={"height": "300px"}),
                        dcc.Graph(id="soyprice-plot", style={"height": "300px"})
                        # Additional plots can be added as more children
                    ]
                )

            ]
        )
    ]
)


# --------------------------------------------------
# PLOTS PANEL TOGGLE
# --------------------------------------------------

@app.callback(
    Output("plots-panel", "style"),
    Output("plots-panel-store", "data"),
    Output("plots-toggle", "children"),
    Input("plots-toggle", "n_clicks"),
    State("plots-panel-store", "data"),
    prevent_initial_call=True
)
def toggle_plots_panel(n_clicks, is_open):
    new_state = not is_open
    style = PLOTS_PANEL_OPEN if new_state else PLOTS_PANEL_CLOSED
    label = "✕ Close" if new_state else "☰ Plots"
    return style, new_state, label



# --------------------------------------------------
# CALLBACK
# --------------------------------------------------

@app.callback(
    Output("map", "figure"),
    Input("year-slider", "value"),
    Input("layer-toggle", "value"),
    Input("selected-survey-store", "data"),
)
def update_map(year, layers, selected_survey):

    fig = go.Figure()
    df_b = bathy[bathy['year']==year]
    # hide the dot for whichever survey is currently showing its polygon overlay
    if selected_survey:
        df_b = df_b[df_b["survey_id"] != selected_survey.get("survey_id")]
    df_n = notices[notices['year']==year]
    # plot river
    fig.add_trace(
    go.Scattermap(
        lon=river_lons_arr,
        lat=river_lats_arr,
        mode="lines",
        line=dict(color="#2166ac", width=2),
        name="Mississippi River",
        hoverinfo="none",
        showlegend=False,
    )
)
    # for display, color, r_lons, r_lats, r_hover in extra_river_data:
    #     if r_lons:
    #         fig.add_trace(go.Scattermap(
    #             lon=r_lons,
    #             lat=r_lats,
    #             mode="lines",
    #             line=dict(color=color, width=2),
    #             name=display,
    #             hoverinfo="text",
    #             hovertext=r_hover,
    #             showlegend=False,
    #         ))

    # draft restriction lines drawn first so they sit behind bathymetry and the other notice layers.
    # a restriction shows if it's currently active (active == "Y"), or if it hasn't started yet
    # ("about to be active") - those are drawn lighter until their start date arrives
    today = pd.Timestamp.now().normalize()
    if "draft" in layers:
        df_draft = df_n[df_n["category"] == "draft"].copy()
        df_draft["is_upcoming"] = (
            ~df_draft["is_active_flag"]
            & df_draft["date_start"].notna()
            & (df_draft["date_start"] > today)
        )
        df_draft = df_draft[df_draft["is_active_flag"] | df_draft["is_upcoming"]]

        shown_legend = {"active": False, "upcoming": False}
        for _, row in df_draft.iterrows():
            # follow the actual river curve rather than the sparse mile-marker points:
            # find where the restriction's start/end mile markers fall on the dense
            # river line geometry and slice that line between the two closest points
            start_lonlat = _nearest_mile_lonlat(row["river_name"], row["mm_low"])
            end_lonlat = _nearest_mile_lonlat(row["river_name"], row["mm_high"])
            if start_lonlat is None or end_lonlat is None:
                continue
            i0 = _nearest_river_index(*start_lonlat)
            i1 = _nearest_river_index(*end_lonlat)
            lo_idx, hi_idx = min(i0, i1), max(i0, i1)
            seg_lons = river_lons_arr[lo_idx:hi_idx + 1]
            seg_lats = river_lats_arr[lo_idx:hi_idx + 1]
            if len(seg_lons) < 2:
                continue

            is_upcoming = row["is_upcoming"]
            status = "upcoming" if is_upcoming else "active"
            date_start_str = row["date_start"].strftime("%b %d, %Y") if pd.notna(row["date_start"]) else ""
            header = (
                f"Draft Restriction begins {date_start_str}" if is_upcoming
                else "Active Draft Restriction"
            )
            # repeated per point so a click on any part of the line carries the full memo
            full_memo = row["full_memo"] if pd.notna(row["full_memo"]) else ""
            customdata = [["draft", full_memo]] * len(seg_lons)
            northbound = _wrap_two_lines(row["northbound"]) if pd.notna(row["northbound"]) else "—"
            southbound = "<br>".join(textwrap.wrap(str(row["southbound"]), width=35)) if pd.notna(row["southbound"]) else "—"
            hovertext = (
                f"<b>{header}</b><br>"
                f"{row['mm_label']}<br>"
                f"Start: {date_start_str or '—'}<br>"
                f"Southbound: {southbound}<br>"
                f"Northbound: {northbound}<br>"
                f"<i>Click for full USCG Memo</i>"
            )

            fig.add_trace(
                go.Scattermap(
                    lon=seg_lons,
                    lat=seg_lats,
                    mode="lines",
                    line=dict(color=CATEGORY_COLORS["draft"], width=6),
                    opacity=DRAFT_ANNOUNCED_OPACITY if is_upcoming else DRAFT_IN_PLACE_OPACITY,
                    legendgroup=f"draft-{status}",
                    showlegend=not shown_legend[status],
                    name="Draft Restriction" + (" (upcoming)" if is_upcoming else ""),
                    hoverinfo="text",
                    hovertext=hovertext,
                    customdata=customdata,
                )
            )
            shown_legend[status] = True

    #  bathym layer - 3 depth bins, each its own trace so size/color/legend are discrete.
    # drawn here (before dredging/shoaling/other) so it sits behind them on the map, but
    # legendrank pushes it below them in the legend regardless of draw order
    if "bathy" in layers:
        depth_masks = {
            "Above 20 ft": df_b["depth"] > 20,
            "14-20 ft": (df_b["depth"] >= 14) & (df_b["depth"] <= 20),
            "Below 14 ft": df_b["depth"] < 14,
        }
        for label, color, size in DEPTH_BINS:
            df_bin = df_b[depth_masks[label]].copy()
            if df_bin.empty:
                continue
            df_bin["date_fmt"] = pd.to_datetime(df_bin["date"]).dt.strftime("%B %-d, %Y")
            df_bin["click_hint"] = df_bin["survey_id"].apply(
                lambda sid: "<i>Click for depth survey map</i>" if sid in DEPTH_POLY_FILES else ""
            )
            custom = df_bin[["date_fmt", "depth", "survey_id", "click_hint"]].copy()
            custom.insert(0, "_type", "bathy")
            fig.add_trace(
                go.Scattermap(
                    lon=df_bin["LON"],
                    lat=df_bin["LAT"],
                    mode="markers",
                    marker=dict(size=size, color=color, opacity=1.0),
                    showlegend=True,
                    legendgroup="depth_survey",
                    legendgrouptitle_text="Survey Locations:<br>Depth Under Low Water",
                    legendrank=10,
                    customdata=custom.values,
                    name=label,
                    hovertemplate=(
                        "<b><span style='font-size:16px'>Riverbed Survey</span></b><br>"
                        "<span style='font-size:14px'>%{customdata[1]}</span><br>"
                        "<span style='font-size:15px'>Estimated Depth under Low Water: <b>%{customdata[2]:.1f} ft</b></span><br>"
                        "%{customdata[4]}<extra></extra>"
                    )
                )
            )

    # dredging/shoaling markers - one trace per category so each can be toggled and colored
    # on its own (draft restriction is drawn separately above, behind the bathymetry layer).
    # both stay on the map for the year they were reported, regardless of active status -
    # dredging's hover just relabels itself planned/in progress/completed based on its dates.
    icon_layers = []
    for category in ("dredging", "shoaling"):
        if category not in layers:
            continue
        df_cat = df_n[df_n["category"] == category].copy()
        df_cat = df_cat.dropna(subset=["lat", "lon"])
        if df_cat.empty:
            continue

        hovertexts = []
        customdatas = []
        for _, r in df_cat.iterrows():
            instructions = str(r["instructions"]) if pd.notna(r.get("instructions")) else ""
            loc_line = r["location_line"]
            full_memo = r["full_memo"] if pd.notna(r.get("full_memo")) else ""
            if category == "shoaling":
                date_range = r["date_str"]
                lines = [f"<b><span style='font-size:16px'>Shoaling reported on {date_range}</span></b>"]
                if loc_line:
                    lines.append(loc_line)
            else:
                date_start, date_end = r["date_start"], r["date_end"]
                if pd.isna(date_start):
                    status_word = "Reported"
                    date_range = r["date_str"]
                else:
                    start_txt = date_start.strftime("%b %d, %Y")
                    end_txt = date_end.strftime("%b %d, %Y") if pd.notna(date_end) else "ongoing"
                    date_range = f"{start_txt} – {end_txt}"
                    if date_start > today:
                        status_word = "Planned"
                    elif pd.isna(date_end) or date_end >= today:
                        status_word = "In Progress"
                    else:
                        status_word = "Completed"
                lines = [
                    f"<b><span style='font-size:16px'>Dredging {status_word}</span></b>",
                    f"<b><span style='font-size:16px'>{date_range}</span></b>",
                ]
                if loc_line:
                    lines.append(loc_line)
            customdatas.append([category, full_memo])
            if instructions:
                wrapped = "<br>".join(textwrap.wrap(instructions, width=55))
                lines.append(f"<span style='font-size:11px'>{wrapped}</span>")
            lines.append("<i>Click for full USCG Memo</i>")
            hovertexts.append("<br>".join(lines))

        if category == "dredging":
            # Invisible trace — hover/click detection only; the custom icon layer handles display
            fig.add_trace(
                go.Scattermap(
                    lon=df_cat["lon"],
                    lat=df_cat["lat"],
                    mode="markers",
                    marker=dict(size=20, color=CATEGORY_COLORS[category], opacity=0),
                    showlegend=True,
                    legendrank=1,
                    name=CATEGORY_LABELS[category],
                    hoverinfo="text",
                    hovertext=hovertexts,
                    customdata=customdatas,
                )
            )
            icon_layers.append({
                "sourcetype": "geojson",
                "source": {
                    "type": "FeatureCollection",
                    "features": [
                        {"type": "Feature",
                         "geometry": {"type": "Point", "coordinates": [row["lon"], row["lat"]]}}
                        for _, row in df_cat.iterrows()
                    ],
                },
                "type": "symbol",
                "symbol": {"icon": "dredge-icon", "iconsize": 4},
            })
        else:
            fig.add_trace(
                go.Scattermap(
                    lon=df_cat["lon"],
                    lat=df_cat["lat"],
                    mode="markers",
                    marker=dict(size=20, color=CATEGORY_COLORS[category], opacity=0),
                    showlegend=True,
                    legendrank=2,
                    name=CATEGORY_LABELS[category],
                    hoverinfo="text",
                    hovertext=hovertexts,
                    customdata=customdatas,
                )
            )
            icon_layers.append({
                "sourcetype": "geojson",
                "source": {
                    "type": "FeatureCollection",
                    "features": [
                        {"type": "Feature",
                         "geometry": {"type": "Point", "coordinates": [row["lon"], row["lat"]]}}
                        for _, row in df_cat.iterrows()
                    ],
                },
                "type": "symbol",
                "symbol": {"icon": "shoaling-icon", "iconsize": 3},
            })

    # "other" notices (e.g. tropical storms) - always shown regardless of layer toggle,
    # same as the standalone warning icon they replace
    df_other = df_n[(df_n["category"] == "other") & (df_n["is_active_flag"])].dropna(subset=["lat", "lon"])
    if not df_other.empty:
        hovertexts = [
            f"<b><span style='font-size:16px'>{CATEGORY_ICONS['other']} Navigation Warning</span></b>"
            f"<br>{r['other_notes']}<br><i>Click for details</i>"
            for _, r in df_other.iterrows()
        ]
        customdatas = [["other", r["other_notes"], r["full_memo"]] for _, r in df_other.iterrows()]
        fig.add_trace(
            go.Scattermap(
                lon=df_other["lon"],
                lat=df_other["lat"],
                mode="markers",
                marker=dict(size=13, color=CATEGORY_COLORS["other"], opacity=0.85),
                showlegend=True,
                legendrank=3,
                name=CATEGORY_LABELS["other"],
                hoverinfo="text",
                hovertext=hovertexts,
                customdata=customdatas,
            )
        )

    # depth polygon overlay for clicked survey
    if selected_survey:
        sid = selected_survey.get("survey_id", "")
        poly_path = _DEPTH_POLY_DIR / f"{sid}_depth_polygons.geojson"
        if poly_path.exists():
            poly_gdf = gpd.read_file(poly_path).sort_values("bin_order")
            for _, row in poly_gdf.iterrows():
                bin_label = row["depth_bin"]
                color = DEPTH_POLY_COLORS.get(bin_label, "#888888")
                lons, lats = _geom_to_lonlat(row.geometry)
                fig.add_trace(go.Scattermap(
                    lon=lons,
                    lat=lats,
                    mode="lines",
                    fill="toself",
                    fillcolor=color,
                    line=dict(width=0),
                    opacity=0.75,
                    name=bin_label,
                    hoverinfo="text",
                    hovertext=f"<b>{bin_label}</b>",
                    hoverlabel=dict(bgcolor=color, bordercolor=color, font=dict(color="white")),
                    showlegend=False,
                ))

    # map layout
    fig.update_layout(
        map=dict(
            style="carto-darkmatter",
            zoom=8.5,
            center=dict(lat=32.5, lon=-91.1),
            layers=icon_layers,
        ),
        margin=dict(l=0, r=0, t=0, b=0),
        uirevision="keep-map",
        showlegend=False
    )
    
    return fig


# --------------------------------------------------
# NOTICE CLICK DETAIL (shared by dredging/shoaling/draft/other)
# --------------------------------------------------

@app.callback(
    Output("notice-detail-store", "data"),
    Input("map", "clickData"),
    Input("notice-detail-close", "n_clicks"),
    prevent_initial_call=True
)
def handle_notice_click(click_data, n_close):
    if dash.ctx.triggered_id == "notice-detail-close":
        return None
    NOTICE_CATEGORIES = {"draft", "dredging", "shoaling", "other"}
    if click_data and click_data.get("points"):
        customdata = click_data["points"][0].get("customdata")
        if customdata and customdata[0] in NOTICE_CATEGORIES:
            return {"category": customdata[0], "fields": list(customdata[1:])}
    return dash.no_update


@app.callback(
    Output("notice-detail-box", "style"),
    Output("notice-detail-content", "children"),
    Input("notice-detail-store", "data")
)
def render_notice_detail(data):
    if not data:
        return NOTICE_DETAIL_HIDDEN, []

    category = data["category"]
    fields = data["fields"]

    if category in ("draft", "dredging", "shoaling"):
        (full_memo,) = fields
        children = [
            html.H3("USCG BROADCAST NOTICE TO MARINERS",
                    style={"margin": "0 0 10px 0", "color": CATEGORY_COLORS[category], "font-size": "18px"}),
            html.P(full_memo, style={"white-space": "pre-wrap"}),
        ]

    else:  # "other"
        other_notes, full_memo = fields
        children = [
            html.H3(f"{CATEGORY_ICONS['other']} NAVIGATION WARNING",
                    style={"margin": "0 0 10px 0", "color": CATEGORY_COLORS["other"], "font-size": "18px"}),
            html.P(other_notes),
            html.Hr() if pd.notna(full_memo) else None,
            html.P(full_memo, style={"font-size": SMALL, "color": "#444"}) if pd.notna(full_memo) else None,
        ]

    return NOTICE_DETAIL_VISIBLE, children


# --------------------------------------------------
# SURVEY DEPTH POLYGON CLICK
# --------------------------------------------------

@app.callback(
    Output("selected-survey-store", "data"),
    Input("map", "clickData"),
    Input("survey-detail-close", "n_clicks"),
    State("selected-survey-store", "data"),
    prevent_initial_call=True,
)
def handle_survey_click(click_data, n_close, current):
    if dash.ctx.triggered_id == "survey-detail-close":
        return None
    if not click_data or not click_data.get("points"):
        return dash.no_update
    customdata = click_data["points"][0].get("customdata")
    if not customdata or customdata[0] != "bathy":
        return dash.no_update
    survey_id = customdata[3]
    if survey_id not in DEPTH_POLY_FILES:
        return dash.no_update
    # toggle off if clicking the same survey again
    if current and current.get("survey_id") == survey_id:
        return None
    date_str = customdata[1]
    return {"survey_id": survey_id, "date": date_str}


@app.callback(
    Output("survey-detail-banner", "style"),
    Output("survey-detail-label", "children"),
    Input("selected-survey-store", "data"),
)
def render_survey_banner(data):
    if not data:
        return SURVEY_BANNER_HIDDEN, ""
    label = f"Depth survey: {data['survey_id']}  ·  {data['date']}"
    return SURVEY_BANNER_VISIBLE, label


# another callback for the barge rate plot
@app.callback(
    Output("barge-rate-plot", "figure"),
    Input("year-slider", "value")
)
def update_barge_rate_plot(year):
    # filter barge rates by year
    if year == thisyear: 
        df52 = barge_rates[(barge_rates["week"] >= start_date) &(barge_rates["week"] <= end_date)]
        title = "STL to NOLA Barge Freight Rates: Past 52 Weeks"
    else: 
        df52 = barge_rates[barge_rates['year']==year]
        title = f"STL to NOLA Barge Freight Rates: {year}"

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(x=df52["week"],y=df52["stlrate_per_ton"],
            mode="lines",line=dict(width=2,color='#d95f0e'),name=year,showlegend=False)
    )
    fig.add_trace(
        go.Scatter(x=df52["week"],y=df52["avg_stlrate"],
            mode="lines",line=dict(width=2,color='grey',dash='dash'),name="Mean")
    )
    fig.add_trace(
        go.Scatter(x=df52["week"],y=df52["plusone"],
            mode="lines",line=dict(width=0),hoverinfo="skip",showlegend=False)
    )
    fig.add_trace(
        go.Scatter(x=df52["week"],y=df52["minusone"],
            mode="lines",fill="tonexty",fillcolor="rgba(160,160,160,0.3)",
            name="±1 SD",line=dict(width=0),hoverinfo="skip")
    )
    fig.update_layout(title=title,
        yaxis_title="$/ton",
        yaxis=dict(range=[barge_rates['stlrate_per_ton'].min(), barge_rates['stlrate_per_ton'].max()]),
            height=300,legend=dict(
            x=0.02,y=0.98,xanchor="left",yanchor="top",
            bgcolor="rgba(255,255,255,0.6)",bordercolor="black",borderwidth=1),
        margin=dict(l=50, r=20, t=40, b=40),
        hovermode="x unified"
    )
    return fig

# water level plot 
@app.callback(
    Output("water-plot", "figure"),
    Input("year-slider", "value")
)
def update_water_plot(year):
    # filter barge rates by year
    if year == thisyear: 
        df365 = greenv[(greenv["date"] >= start_date) &(greenv["date"] <= end_date)]
        title = "Greenville River Stage: Past 52 Weeks"
    else: 
        df365 = greenv[greenv['year']==year]
        title = f"Greenville River Stage: {year}"

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(x=df365["date"],y=df365["stage"],
            mode="lines",line=dict(width=2,color='#2b8cbe'),showlegend=False,name='Barge Rate')
    )
    fig.add_trace(
        go.Scatter(x=df365["date"],y=df365["avg_stage"],
            mode="lines",line=dict(width=2,color='grey'),name="Mean")
    )
    fig.add_trace(
        go.Scatter(x=df365["date"],y=df365["plusone"],
            mode="lines",line=dict(width=0),hoverinfo="skip",showlegend=False)
    )
    fig.add_trace(
        go.Scatter(x=df365["date"],y=df365["minusone"],
            mode="lines",fill="tonexty",fillcolor="rgba(160,160,160,0.3)",
            name="±1 SD",line=dict(width=0),hoverinfo="skip")
    )
    fig.update_layout(title=title,
        yaxis_title="Stage (ft)",
        yaxis=dict(range=[greenv['stage'].min(), greenv['stage'].max()]),
        height=300,legend=dict(
           x=0.02,y=0.98,xanchor="left",yanchor="top",
           bgcolor="rgba(255,255,255,0.6)",bordercolor="black",borderwidth=1),
        margin=dict(l=50, r=20, t=40, b=40),
        hovermode="x unified"
    )
    return fig


#now a callback for corn price plot 
@app.callback(
    Output("cornprice-plot", "figure"),
    Input("year-slider", "value")
)
def update_cornprice_plot(year):
    # filter barge rates by year
    if year == thisyear: 
        df365 = corn_price[(corn_price["date"] >= start_date) &(corn_price["date"] <= end_date)]
        title = "Gulf Corn Price: Past 52 Weeks"
    else: 
        df365 = corn_price[corn_price['year']==year]
        title = f"Gulf Corn Price: {year}"

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(x=df365["date"],y=df365["gulf_corn_price"],
            mode="lines",line=dict(width=2,color='#006837'),name=year,showlegend=False)
    )
    fig.add_trace(
        go.Scatter(x=df365["date"],y=df365["avg_price"],
            mode="lines",line=dict(width=2,color='grey',dash='dash'),name="Mean")
    )
    fig.add_trace(
        go.Scatter(x=df365["date"],y=df365["plusone"],
            mode="lines",line=dict(width=0),hoverinfo="skip",showlegend=False)
    )
    fig.add_trace(
        go.Scatter(x=df365["date"],y=df365["minusone"],
            mode="lines",fill="tonexty",fillcolor="rgba(160,160,160,0.3)",
            name="±1 SD",line=dict(width=0),hoverinfo="skip")
    )
    fig.update_layout(title=title,
        yaxis_title="Price ($/bushel)",
        yaxis=dict(range=[corn_price['gulf_corn_price'].min()-0.1, corn_price['gulf_corn_price'].max()+0.1]),
        height=300,legend=dict(
           x=0.02,y=0.98,xanchor="left",yanchor="top",
           bgcolor="rgba(255,255,255,0.6)",bordercolor="black",borderwidth=1),
        margin=dict(l=50, r=20, t=40, b=40),
        hovermode="x unified"
    )
    return fig

@app.callback(
    Output("soyprice-plot", "figure"),
    Input("year-slider", "value")
)
def update_soyprice_plot(year):
    # filter barge rates by year
    if year == thisyear: 
        df365 = soy_price[(soy_price["date"] >= start_date) &(soy_price["date"] <= end_date)]
        title = "Gulf Soy Price: Past 52 Weeks"
    else: 
        df365 = soy_price[soy_price['year']==year]
        title = f"Gulf Soy Price: {year}"

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(x=df365["date"],y=df365["gulf_soy_price"],
            mode="lines",line=dict(width=2,color='#f1a340'),name=year,showlegend=False)
    )
    fig.add_trace(
        go.Scatter(x=df365["date"],y=df365["avg_price"],
            mode="lines",line=dict(width=2,color='grey',dash='dash'),name="Mean")
    )
    fig.add_trace(
        go.Scatter(x=df365["date"],y=df365["plusone"],
            mode="lines",line=dict(width=0),hoverinfo="skip",showlegend=False)
    )
    fig.add_trace(
        go.Scatter(x=df365["date"],y=df365["minusone"],
            mode="lines",fill="tonexty",fillcolor="rgba(160,160,160,0.3)",
            name="±1 SD",line=dict(width=0),hoverinfo="skip")
    )
    fig.update_layout(title=title,
        yaxis_title="Price ($/bushel)",
        yaxis=dict(range=[soy_price['gulf_soy_price'].min()-0.1, soy_price['gulf_soy_price'].max()+0.1]),
        height=300,legend=dict(
           x=0.02,y=0.98,xanchor="left",yanchor="top",
           bgcolor="rgba(255,255,255,0.6)",bordercolor="black",borderwidth=1),
        margin=dict(l=50, r=20, t=40, b=40),
        hovermode="x unified"
    )
    return fig
# --------------------------------------------------
# RUN
# --------------------------------------------------
if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8050)),
        debug=False
    )

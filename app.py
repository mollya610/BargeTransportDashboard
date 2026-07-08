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


def _build_at_risk_marker():
    import fitz
    import numpy as np
    os.makedirs("assets", exist_ok=True)
    doc = fitz.open("assets/Warning2.pdf")
    pix = doc[0].get_pixmap(matrix=fitz.Matrix(4, 4), alpha=True)
    src = Image.frombytes("RGBA", (pix.width, pix.height), pix.samples)
    arr = np.array(src)
    # find and crop to non-white content
    non_white = np.any(arr[:, :, :3] < 240, axis=2)
    rows = np.where(non_white.any(axis=1))[0]
    cols = np.where(non_white.any(axis=0))[0]
    src = src.crop((int(cols[0]), int(rows[0]), int(cols[-1]) + 1, int(rows[-1]) + 1))
    arr = np.array(src)
    # the crop is a square bounding box around the circle -- mask out the corner
    # whitespace outside the circle itself, keeping its white interior opaque
    h, w = arr.shape[:2]
    yy, xx = np.mgrid[0:h, 0:w]
    dist = np.sqrt((yy - h / 2) ** 2 + (xx - w / 2) ** 2)
    alpha_mask = np.clip((min(h, w) / 2 - dist) / 2.0 + 0.5, 0, 1)
    arr[:, :, 3] = (arr[:, :, 3].astype(float) * alpha_mask).astype(np.uint8)
    Image.fromarray(arr).resize((64, 64), Image.LANCZOS).save("assets/at_risk_marker.png")


if not os.path.exists("assets/dredge_marker.png"):
    _build_dredge_marker()

if not os.path.exists("assets/shoaling_marker.png"):
    _build_shoaling_marker()

if not os.path.exists("assets/at_risk_marker.png"):
    _build_at_risk_marker()


# LOAD DATA
bathy = pd.read_csv("bathym_fixed.csv")
if "confirmed" in bathy.columns:
    bathy = bathy[bathy["confirmed"].fillna("yes").str.lower() == "yes"]

# Ensure year is int
bathy["year"] = bathy["year"].astype(int)
bathy["date_dt"] = pd.to_datetime(bathy["date"]).dt.tz_localize(None)

years = sorted(bathy["year"].unique())
years = [int(y) for y in years]

bathy["at_risk_eff"] = bathy["at_risk"].fillna("no") if "at_risk" in bathy.columns else "no"

# get center point for bathym measures
bathy["geometry"] = bathy["geometry"].apply(wkt.loads)
bathy = gpd.GeoDataFrame(bathy, geometry="geometry", crs="EPSG:4326")
bathy["rep_point"] = bathy.geometry.representative_point()
bathy["LON"] = bathy["rep_point"].apply(lambda p: p.x)
bathy["LAT"] = bathy["rep_point"].apply(lambda p: p.y)
bathy = pd.DataFrame(bathy.drop(columns=["geometry", "rep_point"]))

# for at-risk surveys, the reviewer can mark the exact problem spot within the
# surveyed area (review_surveys.py) -- plot the dot there instead of the survey's
# overall center so it points at the actual issue on a large survey. Full (not at
# risk) surveys always show at their overall center, marked surveys keep no other
# behavior change.
if "problem_lon" in bathy.columns and "problem_lat" in bathy.columns:
    problem_lon = pd.to_numeric(bathy["problem_lon"], errors="coerce")
    problem_lat = pd.to_numeric(bathy["problem_lat"], errors="coerce")
    has_problem_point = (bathy["at_risk_eff"] == "yes") & problem_lon.notna() & problem_lat.notna()
    bathy.loc[has_problem_point, "LON"] = problem_lon[has_problem_point]
    bathy.loc[has_problem_point, "LAT"] = problem_lat[has_problem_point]
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

# per-gage uncertainty (ft) for the "actual depth could vary ±X ft" note on a survey's
# depth legend -- the anchor gages themselves (stlouis/memphis/greenville) are 0 by
# definition, which isn't a useful error bound for a survey elsewhere on the river, so
# use the nearest gage on the opposite side of the survey point from the anchor instead
gage_uncertainty = pd.read_csv("update_bathym/gage_uncertainty.csv")


def _uncertainty_for_mile(mile):
    diffs = (gage_uncertainty["MileMarker"] - mile).abs()
    nearest = gage_uncertainty.loc[diffs.idxmin()]
    if nearest["uncertainty_ft"] > 0:
        return float(nearest["uncertainty_ft"])
    # nearest gage is an anchor (0 ft by definition) -- fall back to the nearest
    # gage on the far side of the survey point from that anchor
    if nearest["MileMarker"] > mile:
        candidates = gage_uncertainty[gage_uncertainty["MileMarker"] < mile]
    else:
        candidates = gage_uncertainty[gage_uncertainty["MileMarker"] > mile]
    if candidates.empty:
        return float(nearest["uncertainty_ft"])
    opp = candidates.loc[(candidates["MileMarker"] - mile).abs().idxmin()]
    return float(opp["uncertainty_ft"])

# get navigation notices (dredging/shoaling/draft restriction/other) from the manually
# maintained notices_<year>.xlsx workbook(s) and locate them via mile markers or, for
# dredging at a named location instead of a mile marker, a manually entered lat/lon
CATEGORY_LABELS = {"dredging": "Dredging", "shoaling": "Shoaling", "draft": "Draft Restriction", "other": "Other"}
CATEGORY_COLORS = {"dredging": "#4a3000", "shoaling": "#fdd734", "draft": "#8c510a", "other": "#e6a817"}
CATEGORY_ICONS = {"dredging": "🛠️", "shoaling": "🔺", "other": "⚠️"}
DRAFT_ANNOUNCED_OPACITY = 0.45
DRAFT_IN_PLACE_OPACITY = 0.85
BIG, MED, SMALL = "18px", "14px", "11px"

# bathymetry survey points are colored by the review app's at_risk flag, not a depth
# threshold -- red if flagged at risk, green otherwise (including legacy rows from
# before at_risk existed, which default to "not at risk")
RISK_BINS = [
    ("Not At Risk", "#2e7d32", 9),
    ("At Risk", "#e53935", 16),
]

# the workbook uses short river codes rather than the full names in the mile-marker table
RIVER_CODE_MAP = {"LMR": "MISSISSIPPI-LO", "UMR": "MISSISSIPPI-UP", "ARK": "ARKANSAS"}

# River stage gauge locations and data sources
# St. Louis: USGS NWIS 07010000 (confirmed, correct NWS-equivalent datum)
# Memphis / Greenville: NWS NWPS API (USACE main-stem gauges, local gage datum)
RIVER_GAGES = {
    "St. Louis": {"site": "07010000", "source": "usgs", "lat": 38.629,    "lon": -90.17977778},
    "Memphis":   {"site": "MEMT1",    "source": "nws",  "lat": 35.123024, "lon": -90.077404},
    "Greenville":{"site": "GEEM6",    "source": "nws",  "lat": 33.2854,   "lon": -91.15632222},
}

# low-water reference threshold per gage (same anchors as the survey depth legend)
GAGE_THRESHOLDS = {"St. Louis": -3, "Memphis": -10, "Greenville": 7}


_stage_csv = Path("river_stage_history.csv")
if _stage_csv.exists():
    river_stage_df = pd.read_csv(_stage_csv, parse_dates=["date"])
    river_stage_df["date"] = pd.to_datetime(river_stage_df["date"]).dt.normalize()
else:
    river_stage_df = pd.DataFrame(columns=["date", "gage", "stage"])

_climatology_csv = Path("river_stage_climatology.csv")
if _climatology_csv.exists():
    river_stage_climatology = pd.read_csv(_climatology_csv)
else:
    river_stage_climatology = pd.DataFrame(
        columns=["gage", "day_of_year", "avg_stage", "p25_stage", "p75_stage", "n_obs"]
    )

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
notices = notices[notices["confirmed"].fillna("").str.upper() == "Y"]
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

# get barge rate data (raw history fetched/updated daily by fetch_market_data.py)
try:
    barge_rates = pd.read_csv("barge_rates_history.csv", parse_dates=["week"])
    barge_rates['week_no']= barge_rates['week'].dt.isocalendar().week
    barge_rates['year'] = barge_rates['week'].dt.year
    barge_demand = barge_rates.groupby(['week_no'])['stlrate_per_ton'].mean().reset_index().rename(columns={'stlrate_per_ton':'avg_stlrate'})
    barge_std = barge_rates.groupby(['week_no'])['stlrate_per_ton'].std().reset_index().rename(columns={'stlrate_per_ton':'std_stlrate'})
    barge_rates = barge_rates.merge(barge_demand,on='week_no',how='inner')
    barge_rates = barge_rates.merge(barge_std,on='week_no',how='inner')
    barge_rates['plusone'] = barge_rates['avg_stlrate'] + barge_rates['std_stlrate']
    barge_rates['minusone'] = barge_rates['avg_stlrate'] - barge_rates['std_stlrate']
except Exception as e:
    print(f"Warning: could not load barge rate data ({e}). Freight rate chart will be empty.")
    barge_rates = pd.DataFrame(columns=['week','stlrate_per_ton','week_no','year','avg_stlrate','std_stlrate','plusone','minusone'])

end_date = barge_rates["week"].max() if not barge_rates.empty else pd.Timestamp.today()
start_date = end_date - pd.Timedelta(weeks=52)
thisyear = date.today().year

# corn and soy price data (raw history fetched/updated daily by fetch_market_data.py)
try:
    corn_price = pd.read_csv("corn_price_history.csv", parse_dates=["date"])
    corn_price['week_no'] = corn_price['date'].dt.isocalendar().week
    corn_price['year'] = corn_price['date'].dt.year
    corn_price['month'] = corn_price['date'].dt.month
    meancorn = corn_price.groupby(['month'])[['gulf_corn_price']].mean().reset_index().rename(columns={'gulf_corn_price':'avg_price'})
    stdcorn = corn_price.groupby(['month'])[['gulf_corn_price']].std().reset_index().rename(columns={'gulf_corn_price':'std_price'})
    corn_price = corn_price.merge(meancorn,on='month',how='inner')
    corn_price = corn_price.merge(stdcorn,on='month',how='inner')
    corn_price['plusone'] = corn_price['avg_price'] + corn_price['std_price']
    corn_price['minusone'] = corn_price['avg_price'] - corn_price['std_price']

    soy_price = pd.read_csv("soy_price_history.csv", parse_dates=["date"])
    soy_price['week_no'] = soy_price['date'].dt.isocalendar().week
    soy_price['year'] = soy_price['date'].dt.year
    soy_price['month'] = soy_price['date'].dt.month
    meansoy = soy_price.groupby(['month'])[['gulf_soy_price']].mean().reset_index().rename(columns={'gulf_soy_price':'avg_price'})
    stdsoy = soy_price.groupby(['month'])[['gulf_soy_price']].std().reset_index().rename(columns={'gulf_soy_price':'std_price'})
    soy_price = soy_price.merge(meansoy,on='month',how='inner')
    soy_price = soy_price.merge(stdsoy,on='month',how='inner')
    soy_price['plusone'] = soy_price['avg_price'] + soy_price['std_price']
    soy_price['minusone'] = soy_price['avg_price'] - soy_price['std_price']
except Exception as e:
    print(f"Warning: could not load corn/soy price data ({e}). Price charts will be empty.")
    corn_price = pd.DataFrame(columns=['date','week_no','year','gulf_corn_price','month','avg_price','std_price','plusone','minusone'])
    soy_price = pd.DataFrame(columns=['date','week_no','year','gulf_soy_price','month','avg_price','std_price','plusone','minusone'])


# now get river lines — filter to just the rivers we need at read time so the full
# shapefile is never loaded into memory
EXTRA_RIVERS = [
    ("OHIO R",     "OHIO",     "Ohio River"),
    ("MISSOURI R", "MISSOURI", "Missouri River"),
    ("ILLINOIS R", "ILLINOIS", "Illinois River"),
    ("ARKANSAS R", "ARKANSAS", "Arkansas River"),
]
_pnames = ", ".join(repr(p) for p in ["MISSISSIPPI R"] + [sf for sf, _, _ in EXTRA_RIVERS])
rivers = gpd.read_file('rivers_shapefile/rivers.shp', where=f"PNAME IN ({_pnames})")
rivers = rivers.set_crs('EPSG:4326')
rivers["geometry"] = rivers.simplify(0.001)

mississippi = rivers[rivers["PNAME"] == "MISSISSIPPI R"]
river_line = linemerge(mississippi.union_all())
x, y = river_line.xy
river_lons_arr = np.array(list(x))
river_lats_arr = np.array(list(y))


def _build_river(shapefile_name, mm_name):
    """Return (lons, lats) for a navigable tributary, clipped to its mile-marker extent."""
    mm_sub = river_mile_points[river_mile_points["RIVER_NAME"] == mm_name]
    if mm_sub.empty:
        return [], []
    buf = 0.5
    clipped = rivers[rivers["PNAME"] == shapefile_name].cx[
        mm_sub["LON"].min() - buf: mm_sub["LON"].max() + buf,
        mm_sub["LAT"].min() - buf: mm_sub["LAT"].max() + buf,
    ]
    if clipped.empty:
        return [], []
    geom = linemerge(clipped.union_all())
    lines = list(geom.geoms) if geom.geom_type == "MultiLineString" else [geom]
    r_lons, r_lats = [], []
    for line in lines:
        lx, ly = line.xy
        r_lons.extend(list(lx) + [None])
        r_lats.extend(list(ly) + [None])
    return r_lons, r_lats


extra_river_data = [
    (display, *_build_river(sf, mm))
    for sf, mm, display in EXTRA_RIVERS
]
del rivers, mississippi, river_line, x, y


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
GAGE_DETAIL_HIDDEN = {"display": "none"}
GAGE_DETAIL_VISIBLE = {
    "position": "absolute", "top": "15px", "right": "15px", "zIndex": "30",
    "width": "340px", "background": "rgba(255,255,255,0.97)",
    "padding": "16px 18px 12px 18px", "border-radius": "8px",
    "box-shadow": "0 2px 10px rgba(0,0,0,0.4)",
    "font-family": "Arial, sans-serif",
}
SURVEY_LEGEND_HIDDEN = {"display": "none"}
SURVEY_LEGEND_VISIBLE = {
    "width": "260px", "background": "rgba(255,255,255,0.97)",
    "padding": "14px 16px", "border-radius": "8px",
    "box-shadow": "0 2px 10px rgba(0,0,0,0.4)",
    "font-family": "Arial, sans-serif",
}
GAGE_FREQ_LINK_HIDDEN = {"display": "none"}
GAGE_FREQ_LINK_VISIBLE = {
    "border": "none", "background": "none", "cursor": "pointer", "padding": "0",
    "color": "#1a237e", "text-decoration": "underline", "font-size": "12px",
    "margin-top": "10px", "display": "block", "text-align": "left",
}
CURRENT_GAGE_HIDDEN = {"display": "none"}
CURRENT_GAGE_VISIBLE = {
    "width": "200px", "background": "rgba(255,255,255,0.97)",
    "padding": "10px 14px", "border-radius": "8px",
    "box-shadow": "0 2px 10px rgba(0,0,0,0.4)",
    "font-family": "Arial, sans-serif",
}
SURVEY_BANNER_HIDDEN = {"display": "none"}
SURVEY_BANNER_VISIBLE = {
    "position": "relative", "width": "max-content", "max-width": "420px",
    "background": "rgba(255,255,255,0.97)",
    "padding": "10px 26px 10px 14px", "border-radius": "8px",
    "box-shadow": "0 2px 10px rgba(0,0,0,0.4)",
    "font-family": "Arial, sans-serif",
}
# gage-frequency panel — floats above the bottom of the map like the other detail
# panels, stopping short of the survey legend column (260px legend + 15px margin
# + a little breathing room)
GAGE_FREQ_HIDDEN = {"display": "none"}
GAGE_FREQ_VISIBLE = {
    "position": "absolute", "bottom": "15px", "left": "15px", "right": "330px",
    "zIndex": "24", "background": "rgba(255,255,255,0.97)",
    "border-radius": "8px",
    "box-shadow": "0 2px 10px rgba(0,0,0,0.4)",
    "padding": "10px 40px 6px 14px",
    "font-family": "Arial, sans-serif",
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
        dcc.Store(id="selected-gage-store", data=None),
        dcc.Store(id="gage-freq-store", data=None),

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

                # Survey depth banner + legend — stacked top-right, banner directly
                # above the legend so its close button (which dismisses both) reads
                # as belonging to the pair instead of sitting far away from the legend
                html.Div(
                    style={
                        "position": "absolute", "top": "15px", "right": "15px", "zIndex": "25",
                        "display": "flex", "flex-direction": "column", "align-items": "flex-end",
                        "gap": "10px",
                    },
                    children=[

                        # Survey depth overlay banner — appears when a survey dot is clicked
                        html.Div(
                            id="survey-detail-banner",
                            style={"display": "none"},
                            children=[
                                html.Button(
                                    "✕",
                                    id="survey-detail-close",
                                    style={
                                        "position": "absolute", "top": "6px", "right": "8px",
                                        "border": "none", "background": "none", "cursor": "pointer",
                                        "font-size": "14px", "font-weight": "bold", "color": "#333",
                                        "line-height": "1", "padding": "2px",
                                    }
                                ),
                                html.Div(id="survey-detail-label"),
                            ]
                        ),

                        # Survey depth legend — appears below the banner when a survey map is shown.
                        # gage-freq-link is a static, permanent component (not part of
                        # survey-legend-content's dynamically-replaced children) -- Dash fires a
                        # component's n_clicks-tracking callback on first mount, so recreating this
                        # button fresh on every survey selection made it look "clicked" immediately
                        html.Div(
                            id="survey-legend-box",
                            style={"display": "none"},
                            children=[
                                html.Div(id="survey-legend-content"),
                                html.Button("", id="gage-freq-link", n_clicks=0, style={"display": "none"}),
                            ]
                        ),

                        # Current gage reading — appears below the legend when a survey dot is
                        # clicked, showing the most recent stage reading for whichever gage that
                        # survey's depth legend is anchored to
                        html.Div(
                            id="current-gage-box",
                            style={"display": "none"},
                        ),
                    ]
                ),

                # River stage detail panel — appears when a gage dot is clicked
                html.Div(
                    id="gage-detail-box",
                    style={"display": "none"},
                    children=[
                        html.Button(
                            "✕", id="gage-detail-close",
                            style={
                                "position": "absolute", "top": "8px", "right": "10px",
                                "border": "none", "background": "none", "cursor": "pointer",
                                "font-size": "16px", "color": "#888",
                            }
                        ),
                        html.Div(id="gage-detail-content"),
                        dcc.Loading(
                            type="circle",
                            children=dcc.Graph(id="gage-stage-plot", style={"height": "260px"}, config={"displayModeBar": False}),
                        ),
                    ]
                ),

                # Gage-frequency panel — appears across the bottom of the map when the
                # "how often does the gage reach X ft?" link is clicked from a survey's
                # depth legend
                html.Div(
                    id="gage-freq-panel",
                    style=GAGE_FREQ_HIDDEN,
                    children=[
                        html.Button(
                            "✕", id="gage-freq-close",
                            style={
                                "position": "absolute", "top": "8px", "right": "10px",
                                "border": "none", "background": "none", "cursor": "pointer",
                                "font-size": "16px", "color": "#888",
                            }
                        ),
                        dcc.Graph(id="gage-freq-graph", style={"height": "240px"}, config={"displayModeBar": False}),
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
                                                                html.Div(style={"width": "12px", "height": "12px", "border-radius": "50%", "background": RISK_BINS[0][1], "display": "inline-block", "margin-right": "4px", "vertical-align": "middle"}),
                                                                html.Span("Not at risk", style={"font-size": "12px", "vertical-align": "middle"}),
                                                            ]),
                                                            html.Div([
                                                                html.Img(src="/assets/at_risk_marker.png", height="16", style={"display": "inline-block", "margin-right": "4px", "vertical-align": "middle"}),
                                                                html.Span("At risk", style={"font-size": "12px", "vertical-align": "middle"}),
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
                                        {
                                            "label": html.Span([
                                                html.Img(src="/assets/raindrop.png", height="22", style={"vertical-align": "middle", "margin-right": "5px"}),
                                                "Stream Gage",
                                            ]),
                                            "value": "stage",
                                        },
                                    ],
                                    value=["bathy", "dredging", "shoaling", "draft", "stage"],
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
    # hide the dot for whichever survey is currently showing its polygon overlay, but if it's
    # at risk, keep its marker up (faded) at the problem point so it's not lost under the polygon
    selected_at_risk_row = None
    if selected_survey:
        sid = selected_survey.get("survey_id")
        df_b = df_b[df_b["survey_id"] != sid]
        match = bathy[(bathy["survey_id"] == sid) & (bathy["at_risk_eff"] == "yes")]
        if not match.empty:
            selected_at_risk_row = match.iloc[0]
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
    for display, r_lons, r_lats in extra_river_data:
        if r_lons:
            fig.add_trace(go.Scattermap(
                lon=r_lons,
                lat=r_lats,
                mode="lines",
                line=dict(color="#2166ac", width=1),
                name=display,
                hoverinfo="none",
                showlegend=False,
            ))

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

    icon_layers = []

    #  bathym layer - 2 risk bins (at_risk yes/no), each its own trace so color/legend are discrete.
    # drawn here (before dredging/shoaling/other) so it sits behind them on the map, but
    # legendrank pushes it below them in the legend regardless of draw order
    if "bathy" in layers:
        risk_masks = {
            "Not At Risk": df_b["at_risk_eff"] != "yes",
            "At Risk": df_b["at_risk_eff"] == "yes",
        }
        for label, color, size in RISK_BINS:
            df_bin = df_b[risk_masks[label]].copy()
            if df_bin.empty:
                continue
            df_bin["date_fmt"] = pd.to_datetime(df_bin["date"]).dt.strftime("%B %-d, %Y")
            df_bin["click_hint"] = df_bin["survey_id"].apply(
                lambda sid: "<i>Click for depth map and details</i>" if sid in DEPTH_POLY_FILES else ""
            )
            # thresholds south of the Arkansas River confluence (~mile 580) are anchored
            # to Greenville = 7ft instead of Memphis -- see threshold calculation/
            # calculate_lowwater_thresh_datums.py's GREENVILLE_TARGET/CONFLUENCE_MILE
            def _gage_info(m):
                if m >= 951:
                    return "St. Louis", -3, "St. Louis gage is at -3ft"
                if m >= 580:
                    return "Memphis", -10, "Memphis gage is at -10ft"
                return "Greenville", 7, "Greenville gage is at 7ft"
            gage_info = df_bin["milemarker"].apply(_gage_info)
            df_bin["gage_name"] = gage_info.apply(lambda t: t[0])
            df_bin["gage_value"] = gage_info.apply(lambda t: t[1])
            df_bin["gage_label"] = gage_info.apply(lambda t: t[2])
            df_bin["gage_uncertainty"] = df_bin["milemarker"].apply(_uncertainty_for_mile)
            custom = df_bin[["date_fmt", "depth", "survey_id", "click_hint", "gage_label", "gage_name", "gage_value", "gage_uncertainty"]].copy()
            custom.insert(0, "_type", "bathy")
            is_at_risk = label == "At Risk"
            fig.add_trace(
                go.Scattermap(
                    lon=df_bin["LON"],
                    lat=df_bin["LAT"],
                    mode="markers",
                    marker=dict(size=size, color=color, opacity=0.0 if is_at_risk else 1.0),
                    showlegend=True,
                    legendgroup="depth_survey",
                    legendgrouptitle_text="Survey Locations:<br>Navigation Risk under Low Water",
                    legendrank=10,
                    customdata=custom.values,
                    name=label,
                    hovertemplate=(
                        "<b><span style='font-size:16px'>Riverbed Survey</span></b><br>"
                        "<span style='font-size:14px'>%{customdata[1]}</span><br>"
                        "%{customdata[4]}<extra></extra>"
                    )
                )
            )
            if is_at_risk:
                icon_layers.append({
                    "sourcetype": "geojson",
                    "source": {
                        "type": "FeatureCollection",
                        "features": [
                            {"type": "Feature",
                             "geometry": {"type": "Point", "coordinates": [row["LON"], row["LAT"]]}}
                            for _, row in df_bin.iterrows()
                        ],
                    },
                    "type": "symbol",
                    "symbol": {"icon": "at-risk-icon", "iconsize": 2.5},
                })

    if selected_at_risk_row is not None:
        icon_layers.append({
            "sourcetype": "geojson",
            "source": {
                "type": "FeatureCollection",
                "features": [{
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [selected_at_risk_row["LON"], selected_at_risk_row["LAT"]],
                    },
                }],
            },
            "type": "symbol",
            "symbol": {"icon": "at-risk-icon-selected", "iconsize": 2.5},
        })

    # dredging/shoaling markers - one trace per category so each can be toggled and colored
    # on its own (draft restriction is drawn separately above, behind the bathymetry layer).
    # both stay on the map for the year they were reported, regardless of active status -
    # dredging's hover just relabels itself planned/in progress/completed based on its dates.
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

    # river stage gage dots
    if "stage" in layers:
        gage_names = list(RIVER_GAGES.keys())
        fig.add_trace(go.Scattermap(
            lon=[info["lon"] for info in RIVER_GAGES.values()],
            lat=[info["lat"] for info in RIVER_GAGES.values()],
            mode="markers+text",
            marker=dict(size=18, color="#1565c0", opacity=0),
            text=gage_names,
            textposition="top right",
            textfont=dict(size=13, color="white"),
            showlegend=False,
            customdata=[["gage", gage_name] for gage_name in gage_names],
            hovertext=[f"<b>{gage_name} River Stage</b><br><i>Click for current reading</i>" for gage_name in gage_names],
            hoverinfo="text",
            name="River Stage Gages",
        ))
        icon_layers.append({
            "sourcetype": "geojson",
            "source": {
                "type": "FeatureCollection",
                "features": [
                    {"type": "Feature",
                     "geometry": {"type": "Point", "coordinates": [info["lon"], info["lat"]]}}
                    for info in RIVER_GAGES.values()
                ],
            },
            "type": "symbol",
            "symbol": {"icon": "gage-icon", "iconsize": 3},
        })

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
    gage_name = customdata[6] if len(customdata) > 6 else "Memphis"
    gage_value = customdata[7] if len(customdata) > 7 else -10
    gage_uncertainty = customdata[8] if len(customdata) > 8 else 0.0
    return {
        "survey_id": survey_id, "date": date_str,
        "gage_name": gage_name, "gage_value": gage_value, "gage_uncertainty": gage_uncertainty,
    }


@app.callback(
    Output("survey-detail-banner", "style"),
    Output("survey-detail-label", "children"),
    Input("selected-survey-store", "data"),
)
def render_survey_banner(data):
    if not data:
        return SURVEY_BANNER_HIDDEN, ""
    label = html.Div([
        html.Div(
            "U.S Army Corps of Engineers Hydrographic Survey:",
            style={"font-size": "12px", "color": "#666", "line-height": "1.3", "white-space": "nowrap"},
        ),
        html.Div(
            data["survey_id"],
            style={"font-size": "14px", "font-weight": "bold", "color": "#1a237e", "margin-top": "2px", "white-space": "nowrap"},
        ),
        html.Div(
            data["date"],
            style={"font-size": "12px", "color": "#888", "margin-top": "2px", "white-space": "nowrap"},
        ),
    ])
    return SURVEY_BANNER_VISIBLE, label


@app.callback(
    Output("current-gage-box", "style"),
    Output("current-gage-box", "children"),
    Input("selected-survey-store", "data"),
)
def render_current_gage(data):
    if not data:
        return CURRENT_GAGE_HIDDEN, []
    gage_name = data.get("gage_name", "Memphis")
    latest = river_stage_df[river_stage_df["gage"] == gage_name].sort_values("date")
    if latest.empty:
        return CURRENT_GAGE_HIDDEN, []
    latest_row = latest.iloc[-1]
    content = [
        html.Div(
            f"{gage_name} gage is currently at",
            style={"font-size": "12px", "color": "#444", "line-height": "1.3"},
        ),
        html.Div(
            f"{latest_row['stage']:.1f} ft",
            style={"font-size": "22px", "font-weight": "bold", "color": "#1a237e", "margin-top": "2px"},
        ),
        html.Div(
            f"as of {latest_row['date'].strftime('%B %-d, %Y')}",
            style={"font-size": "10px", "color": "#888", "margin-top": "2px"},
        ),
    ]
    return CURRENT_GAGE_VISIBLE, content


@app.callback(
    Output("survey-legend-box", "style"),
    Output("survey-legend-content", "children"),
    Output("gage-freq-link", "children"),
    Output("gage-freq-link", "style"),
    Input("selected-survey-store", "data"),
)
def render_survey_legend(data):
    if not data:
        return SURVEY_LEGEND_HIDDEN, [], "", GAGE_FREQ_LINK_HIDDEN
    gage_name = data.get("gage_name", "Memphis")
    gage_value = data.get("gage_value", -10)
    gage_uncertainty = data.get("gage_uncertainty", 0.0)
    title = html.Div([
        html.Div(
            "River Depth When",
            style={"font-size": "16px", "font-weight": "normal", "line-height": "1.3", "text-transform": "uppercase"},
        ),
        html.Div(
            [
                html.Span(gage_name, style={"font-weight": "bold"}),
                " Gage is at ",
                html.Span(f"{int(gage_value)} ft", style={"font-weight": "bold"}),
            ],
            style={"font-size": "16px", "font-weight": "normal", "line-height": "1.3", "text-transform": "uppercase"},
        ),
        html.Div(
            f"Depth estimate accurate to ±{gage_uncertainty:g} ft",
            style={"font-size": "11px", "font-style": "italic", "color": "#666", "margin-top": "4px"},
        ),
    ], style={"margin-bottom": "10px"})
    rows = [
        html.Div(
            style={"display": "flex", "align-items": "center", "margin-bottom": "5px"},
            children=[
                html.Span(style={
                    "display": "inline-block", "width": "18px", "height": "14px",
                    "background": color, "border-radius": "2px", "flex-shrink": "0",
                }),
                html.Span(bin_label, style={"font-size": "12px", "margin-left": "8px"}),
            ]
        )
        for bin_label, color in DEPTH_POLY_COLORS.items()
    ]
    n_rows = len(rows) // 2 + len(rows) % 2
    rows_grid = html.Div(
        rows,
        style={
            "display": "grid", "grid-template-columns": "1fr 1fr",
            "grid-template-rows": f"repeat({n_rows}, auto)", "grid-auto-flow": "column",
            "column-gap": "6px",
        },
    )
    link_text = f"How often does the {gage_name} gage reach {int(gage_value)}ft?"
    content = [title, rows_grid]
    return SURVEY_LEGEND_VISIBLE, content, link_text, GAGE_FREQ_LINK_VISIBLE


@app.callback(
    Output("gage-freq-store", "data"),
    Input("gage-freq-link", "n_clicks"),
    Input("gage-freq-close", "n_clicks"),
    Input("selected-survey-store", "data"),
    prevent_initial_call=True,
)
def toggle_gage_freq(n_open, n_close, survey_data):
    # closing the panel directly, or changing/closing the survey it belongs to,
    # both dismiss it -- otherwise it could linger showing a stale gage after the
    # underlying survey selection has moved on
    if dash.ctx.triggered_id in ("gage-freq-close", "selected-survey-store"):
        return None
    if not survey_data:
        return dash.no_update
    return {
        "gage_name": survey_data.get("gage_name", "Memphis"),
        "gage_value": survey_data.get("gage_value", -10),
    }


@app.callback(
    Output("gage-freq-panel", "style"),
    Output("gage-freq-graph", "figure"),
    Input("gage-freq-store", "data"),
)
def render_gage_freq(data):
    empty_fig = go.Figure()
    empty_fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", margin=dict(l=0, r=0, t=0, b=0))
    if not data:
        return GAGE_FREQ_HIDDEN, empty_fig

    gage_name = data["gage_name"]
    gage_value = data["gage_value"]
    cutoff = pd.Timestamp.today().normalize() - pd.DateOffset(years=5)
    df = river_stage_df[
        (river_stage_df["gage"] == gage_name) & (river_stage_df["date"] >= cutoff)
    ][["date", "stage"]].sort_values("date").reset_index(drop=True)

    below = df[df["stage"] <= gage_value]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["stage"], mode="lines", name="Stage",
        line=dict(color="#1565c0", width=1),
        hovertemplate="%{x|%B %d, %Y}<br>%{y:.1f} ft<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=below["date"], y=below["stage"], mode="markers",
        name=f"At/below {int(gage_value)} ft",
        marker=dict(color="#e53935", size=5),
        hovertemplate="%{x|%B %d, %Y}<br>%{y:.1f} ft<extra></extra>",
    ))
    fig.add_hline(y=gage_value, line=dict(color="#e53935", width=1, dash="dot"))
    # light grey dotted line at Jan 1 of each year in range, so a multi-year span is
    # easier to read at a glance
    for year in range(cutoff.year, pd.Timestamp.today().year + 1):
        fig.add_vline(x=pd.Timestamp(year=year, month=1, day=1), line=dict(color="#bbb", width=1, dash="dot"))
    fig.update_layout(
        margin=dict(l=55, r=15, t=15, b=25),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(245,248,255,1)",
        showlegend=False,
        xaxis=dict(showgrid=False, tickfont=dict(size=10)),
        yaxis=dict(
            title=f"{gage_name} River Stage (ft)", gridcolor="#ddd",
            tickfont=dict(size=13), title_font=dict(size=14),
        ),
        hovermode="closest",
    )
    return GAGE_FREQ_VISIBLE, fig


@app.callback(
    Output("selected-gage-store", "data"),
    Input("map", "clickData"),
    Input("gage-detail-close", "n_clicks"),
    State("selected-gage-store", "data"),
    prevent_initial_call=True,
)
def handle_gage_click(click_data, n_close, current):
    if dash.ctx.triggered_id == "gage-detail-close":
        return None
    if not click_data or not click_data.get("points"):
        return dash.no_update
    customdata = click_data["points"][0].get("customdata")
    if not customdata or customdata[0] != "gage":
        return dash.no_update
    gage_name = customdata[1]
    if current and current.get("gage_name") == gage_name:
        return None
    return {"gage_name": gage_name}


@app.callback(
    Output("gage-detail-box", "style"),
    Output("gage-detail-content", "children"),
    Output("gage-stage-plot", "figure"),
    Input("selected-gage-store", "data"),
)
def render_gage_panel(data):
    empty_fig = go.Figure()
    empty_fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", margin=dict(l=0,r=0,t=0,b=0))
    if not data:
        return GAGE_DETAIL_HIDDEN, [], empty_fig

    gage_name = data["gage_name"]
    cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=29)
    df = river_stage_df[
        (river_stage_df["gage"] == gage_name) & (river_stage_df["date"] >= cutoff)
    ][["date", "stage"]].sort_values("date").reset_index(drop=True)

    if df.empty:
        content = [html.P("Could not load stage data.", style={"color": "#888", "font-size": "13px"})]
        return GAGE_DETAIL_VISIBLE, content, empty_fig

    df = df.sort_values("date").reset_index(drop=True)
    df["day_of_year"] = df["date"].dt.dayofyear.where(df["date"].dt.dayofyear != 366, 365)
    clim = river_stage_climatology[river_stage_climatology["gage"] == gage_name]
    df = df.merge(clim.drop(columns="gage"), on="day_of_year", how="left")

    current_stage = df["stage"].iloc[-1]

    # NWS gages (Memphis, Greenville) report hourly -- the daily pipeline runs at
    # 7am CT, so "today"'s row is a partial early-morning reading until tomorrow's
    # run backfills it with the full day's last reading. St. Louis pulls USGS's own
    # already-finalized daily value, so this caveat doesn't apply there.
    is_today = df["date"].iloc[-1] == pd.Timestamp.today().normalize()
    is_nws = RIVER_GAGES.get(gage_name, {}).get("source") == "nws"
    today_note = " (as of ~7am CT)" if (is_today and is_nws) else ""

    # custom HTML legend, above the plot -- Plotly's own built-in legend has a rendering
    # quirk here where the visual order doesn't reliably follow trace order, legendrank,
    # or yanchor (tried all three independently; render stayed fixed regardless), so
    # showlegend is off on the figure and this replaces it with full control over order
    def _legend_row(swatch_style, label):
        return html.Div([
            html.Span(style={"display": "inline-block", "width": "18px", "margin-right": "6px", "vertical-align": "middle", **swatch_style}),
            html.Span(label, style={"font-size": "9px", "vertical-align": "middle"}),
        ], style={"margin-bottom": "3px"})

    legend_rows = html.Div([
        _legend_row({"height": "3px", "background": "#1565c0"}, "DAILY STAGE"),
        _legend_row({"height": "0", "border-top": "2px dashed #90caf9"}, "HISTORICAL AVERAGE (2000-2025)"),
        _legend_row({"height": "10px", "background": "rgba(144,202,249,0.3)", "border-radius": "2px"}, "HISTORICAL 25-75TH PERCENTILE"),
    ], style={"margin-bottom": "8px"})

    content = [
        html.Div(gage_name, style={"font-size": "18px", "font-weight": "bold", "margin-bottom": "4px"}),
        html.Div([
            html.Span("Current stage: ", style={"font-size": "13px", "color": "#444"}),
            html.Span(f"{current_stage:.1f} ft", style={"font-size": "16px", "font-weight": "bold"}),
            html.Span(today_note, style={"font-size": "11px", "color": "#888"}),
        ], style={"margin-bottom": "10px"}),
        legend_rows,
    ]

    # trace add-order drives legend order (daily stage first) -- the percentile band's
    # fill="tonexty" only needs its two traces (p75/p25) adjacent to each other, so the
    # pair can move to the end without breaking the fill
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["stage"],
        mode="lines", name="DAILY STAGE",
        line=dict(color="#1565c0", width=2),
        hovertemplate="%{y:.1f} ft<extra>Daily stage</extra>",
    ))
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["avg_stage"],
        mode="lines", name="HISTORICAL AVERAGE (2000-2025)",
        line=dict(color="#90caf9", width=2, dash="dash"),
        hovertemplate="%{y:.1f} ft<extra>Historical avg</extra>",
    ))
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["p75_stage"],
        mode="lines", line=dict(width=0),
        showlegend=False, hoverinfo="skip",
    ))
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["p25_stage"],
        mode="lines", line=dict(width=0),
        fill="tonexty", fillcolor="rgba(144,202,249,0.3)",
        name="HISTORICAL 25-75TH PERCENTILE", hoverinfo="skip",
    ))
    fig.add_trace(go.Scatter(
        x=[df["date"].iloc[-1]], y=[current_stage],
        mode="markers", name="Today",
        marker=dict(color="#1565c0", size=8),
        showlegend=False, hoverinfo="skip",
    ))
    # extend the y-axis down to the gage's low-water threshold (no extra padding -- goes
    # exactly to the critical mark) and at least 10ft above the highest value so the top
    # isn't cramped the way a small proportional pad left it
    threshold = GAGE_THRESHOLDS.get(gage_name, 0)
    y_max = max(df["stage"].max(), df["p75_stage"].max())
    y_range = [threshold, y_max + 10]
    fig.update_layout(
        margin=dict(l=40, r=10, t=10, b=30),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(245,248,255,1)",
        showlegend=False,
        xaxis=dict(showgrid=False, tickfont=dict(size=10)),
        yaxis=dict(title="Stage (ft)", range=y_range, gridcolor="#ddd", tickfont=dict(size=10), title_font=dict(size=15)),
        hovermode="x unified",
        hoverlabel=dict(bgcolor="white", bordercolor="#888", font=dict(color="#222", size=11)),
    )
    return GAGE_DETAIL_VISIBLE, content, fig


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

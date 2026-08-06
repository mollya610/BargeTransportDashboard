import os
import re
import glob
import json
import textwrap
from pathlib import Path
import dash
from dash import dcc, html, Input, Output, State
import geopandas as gpd
import plotly.graph_objects as go
from shapely.ops import linemerge
from shapely.ops import unary_union
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
years = [int(y) for y in years if int(y) >= 2021]

bathy["at_risk_eff"] = bathy["at_risk"].fillna("low") if "at_risk" in bathy.columns else "low"

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
    has_problem_point = bathy["at_risk_eff"].isin(["medium", "high"]) & problem_lon.notna() & problem_lat.notna()
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

# bathymetry survey points are colored by the review app's at_risk flag (low/medium/high),
# not a depth threshold. Low and Medium render as plain colored dots; High gets the same
# custom warning-icon treatment as the old binary "At Risk" tier (see icon_layers below).
# Legacy/blank rows (from before at_risk existed) default to "low".
RISK_BINS = [
    ("Low Risk", "#2e7d32", 9),
    ("Medium Risk", "#fb8c00", 12),
    ("High Risk", "#e53935", 16),
]

# the workbook uses short river codes rather than the full names in the mile-marker table
RIVER_CODE_MAP = {
    "LMR": "MISSISSIPPI-LO", "AHP": "MISSISSIPPI-LO", "UMR": "MISSISSIPPI-UP",
    "ARK": "ARKANSAS", "RED": "RED", "WHITE": "WHITE", "ATCH": "ATCHAFALAY",
}

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

# Memphis gage stage, year-over-year -- same shape as the price plots below (2015+ only,
# to keep the "other years" background from getting too cluttered with older history)
memphis_stage = river_stage_df[
    (river_stage_df["gage"] == "Memphis") & (river_stage_df["date"].dt.year >= 2015)
].copy()
memphis_stage["week_no"] = memphis_stage["date"].dt.isocalendar().week
memphis_stage["year"] = memphis_stage["date"].dt.year
memphis_stage = memphis_stage.sort_values("date").reset_index(drop=True)

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


def _interp_mile_lonlat(river_name, mile):
    """Lon/lat for a (possibly fractional) mile marker, linearly interpolated between
    the nearest surveyed mile points on either side -- e.g. MM 751.2 lands 1/5 of the
    way from the MM 751 point to the MM 752 point, rather than snapping to one of them."""
    if pd.isna(mile):
        return None
    sub = river_mile_points[river_mile_points["RIVER_NAME"] == river_name]
    if sub.empty:
        return None
    below = sub[sub["MILE"] <= mile]
    above = sub[sub["MILE"] >= mile]
    if below.empty or above.empty:
        row = sub.loc[(sub["MILE"] - mile).abs().idxmin()]
        return row["LON"], row["LAT"]
    lo = below.loc[below["MILE"].idxmax()]
    hi = above.loc[above["MILE"].idxmin()]
    if lo["MILE"] == hi["MILE"]:
        return lo["LON"], lo["LAT"]
    frac = (mile - lo["MILE"]) / (hi["MILE"] - lo["MILE"])
    return lo["LON"] + frac * (hi["LON"] - lo["LON"]), lo["LAT"] + frac * (hi["LAT"] - lo["LAT"])


def _mile_brackets(river_name, mile):
    """The two surveyed mile-marker points straddling `mile`, for the faint reference
    lines shown on click. If `mile` lands exactly on a surveyed point, steps out to the
    next point on each side instead of returning the same point twice."""
    if pd.isna(mile):
        return None
    sub = river_mile_points[river_mile_points["RIVER_NAME"] == river_name]
    if sub.empty:
        return None
    below = sub[sub["MILE"] < mile]
    above = sub[sub["MILE"] > mile]
    if below.empty or above.empty:
        return None
    return below.loc[below["MILE"].idxmax()], above.loc[above["MILE"].idxmin()]


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

# historical shoaling notices (2021-2025), backfilled once via
# notice_to_mariners/fetch_shoaling_history.py and manually reviewed -- separate from
# the live notices_<year>.xlsx pipeline above (which only tracks 2026 onward), merged
# in here so it renders through the same map/hover/click code as current notices
_hist_shoaling_csv = Path("notice_to_mariners/data/shoaling_notices_2021_2025DONE.csv")
if _hist_shoaling_csv.exists():
    hist_shoaling = pd.read_csv(_hist_shoaling_csv)
    hist_shoaling = hist_shoaling[hist_shoaling["confirmed"].fillna("").str.upper() == "Y"]
    hist_shoaling["date_published"] = pd.to_datetime(hist_shoaling["date"])
    hist_shoaling["year"] = hist_shoaling["date_published"].dt.year
    hist_shoaling["date_str"] = hist_shoaling["date_published"].dt.strftime("%Y-%m-%d")
    hist_shoaling["category"] = "shoaling"
    hist_shoaling["is_active_flag"] = False
    hist_shoaling["instructions"] = np.nan
    hist_shoaling["full_memo"] = hist_shoaling["memo"]
    # river is almost always a clean abbreviation ("LMR"), but a couple of rows picked up
    # stray free text during manual review -- pull a known abbreviation out of it instead
    # of trusting the raw field, defaulting to LMR (the river nearly every row is on)
    _river_abbr_re = re.compile(r"\b(LMR|UMR|AHP|ARK|RED|WHITE|ATCH)\b")
    hist_shoaling["river_abbr"] = hist_shoaling["river"].astype(str).apply(
        lambda s: (m.group(1) if (m := _river_abbr_re.search(s)) else "LMR")
    )
    hist_shoaling["river_name"] = hist_shoaling["river_abbr"].map(RIVER_CODE_MAP)
    hist_shoaling["mm_low"] = hist_shoaling[["mm1", "mm2"]].min(axis=1)
    hist_shoaling["mm_high"] = hist_shoaling[["mm1", "mm2"]].max(axis=1)
    hist_shoaling["mm_label"] = hist_shoaling.apply(_format_mm, axis=1)
    hist_shoaling["mid_mile"] = hist_shoaling[["mm_low", "mm_high"]].mean(axis=1)
    hist_shoaling["location_details"] = hist_shoaling["location_description"]
    hist_shoaling["location_line"] = hist_shoaling.apply(_location_line, axis=1)
    # place each notice at its exact fractional mile marker (see _interp_mile_lonlat)
    # instead of snapping to the nearest whole mile like the live notices pipeline does
    _lonlat = hist_shoaling.apply(lambda r: _interp_mile_lonlat(r["river_name"], r["mid_mile"]), axis=1)
    hist_shoaling["lon"] = [ll[0] if ll else np.nan for ll in _lonlat]
    hist_shoaling["lat"] = [ll[1] if ll else np.nan for ll in _lonlat]
    notices = pd.concat([notices, hist_shoaling], ignore_index=True)


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
    barge_rates = barge_rates.sort_values('week').reset_index(drop=True)
except Exception as e:
    print(f"Warning: could not load barge rate data ({e}). Freight rate chart will be empty.")
    barge_rates = pd.DataFrame(columns=['week','stlrate_per_ton','week_no','year'])

# forward (not-yet-delivered) barge rate quotes -- same USDA workbook as the spot rate
# above, but the NXTMONTH/THREEMONTH tabs quote the rate for a contract 1 or 3 months
# out, alongside which calendar month that contract is for
try:
    barge_rates_nextmonth = pd.read_csv("barge_rates_nextmonth_history.csv", parse_dates=["week"])
    barge_rates_nextmonth['week_no'] = barge_rates_nextmonth['week'].dt.isocalendar().week
    barge_rates_nextmonth['year'] = barge_rates_nextmonth['week'].dt.year
    barge_rates_nextmonth = barge_rates_nextmonth.sort_values('week').reset_index(drop=True)

    barge_rates_threemonth = pd.read_csv("barge_rates_threemonth_history.csv", parse_dates=["week"])
    barge_rates_threemonth['week_no'] = barge_rates_threemonth['week'].dt.isocalendar().week
    barge_rates_threemonth['year'] = barge_rates_threemonth['week'].dt.year
    barge_rates_threemonth = barge_rates_threemonth.sort_values('week').reset_index(drop=True)
except Exception as e:
    print(f"Warning: could not load forward barge rate data ({e}). Forward rate charts will be empty.")
    barge_rates_nextmonth = pd.DataFrame(columns=['week','contract_month_label','fwd_rate_per_ton','week_no','year'])
    barge_rates_threemonth = pd.DataFrame(columns=['week','contract_month_label','fwd_rate_per_ton','week_no','year'])

thisyear = date.today().year

# corn and soy price data (raw history fetched/updated daily by fetch_market_data.py)
try:
    corn_price = pd.read_csv("corn_price_history.csv", parse_dates=["date"])
    corn_price['week_no'] = corn_price['date'].dt.isocalendar().week
    corn_price['year'] = corn_price['date'].dt.year
    corn_price = corn_price.sort_values('date').reset_index(drop=True)

    soy_price = pd.read_csv("soy_price_history.csv", parse_dates=["date"])
    soy_price['week_no'] = soy_price['date'].dt.isocalendar().week
    soy_price['year'] = soy_price['date'].dt.year
    soy_price = soy_price.sort_values('date').reset_index(drop=True)
except Exception as e:
    print(f"Warning: could not load corn/soy price data ({e}). Price charts will be empty.")
    corn_price = pd.DataFrame(columns=['date','week_no','year','gulf_corn_price'])
    soy_price = pd.DataFrame(columns=['date','week_no','year','gulf_soy_price'])

# Illinois-to-Gulf corn price spread -- same USDA workbook as the corn/soy Gulf prices
# above, just a different (precomputed) column on the same sheet
try:
    corn_spread = pd.read_csv("corn_spread_history.csv", parse_dates=["date"])
    corn_spread['week_no'] = corn_spread['date'].dt.isocalendar().week
    corn_spread['year'] = corn_spread['date'].dt.year
    corn_spread = corn_spread.sort_values('date').reset_index(drop=True)
except Exception as e:
    print(f"Warning: could not load corn spread data ({e}). Corn spread chart will be empty.")
    corn_spread = pd.DataFrame(columns=['date','week_no','year','il_gulf_corn_spread'])

# Barge Demand indicator: current-year WASDE production estimate (fetch_wasde.py)
try:
    wasde_latest = pd.read_csv("wasde_production_estimate.csv").iloc[0].to_dict()
except Exception as e:
    print(f"Warning: could not load WASDE production estimate ({e}). Barge Demand production chart will skip the current-year bar.")
    wasde_latest = None

# Barge Demand indicator: ~10yr history of final US corn/soybean production (fetch_grain_production.py, NASS QuickStats)
try:
    grain_production_history = pd.read_csv("grain_production_history.csv")
except Exception as e:
    print(f"Warning: could not load grain production history ({e}). Barge Demand production chart will show only the current-year estimate.")
    grain_production_history = pd.DataFrame(columns=['year', 'corn_production_million_bu', 'soybean_production_million_bu'])

# Year-comparison scatterplot only: pre-2016 production (harvest_wasde2.xlsx, a WASDE-report
# vintage, manually compiled back to 2008) so the scatterplot can go back further than the
# NASS final-production history above. Only used to fill years grain_production_history doesn't
# have -- NASS final figures run ~2% off WASDE report estimates (different vintage/methodology),
# so where both exist the NASS figure above wins rather than silently blending the two.
try:
    wasde_production_backfill = pd.read_excel("harvest_wasde2.xlsx").rename(
        columns={"corn_prod": "corn_production_million_bu", "soy_prod": "soybean_production_million_bu"}
    )[["year", "corn_production_million_bu", "soybean_production_million_bu"]]
except Exception as e:
    print(f"Warning: could not load WASDE production backfill ({e}). Year-comparison scatterplot will start in {grain_production_history['year'].min() if not grain_production_history.empty else thisyear}.")
    wasde_production_backfill = pd.DataFrame(columns=['year', 'corn_production_million_bu', 'soybean_production_million_bu'])

# Barge Demand indicator: Dec-corn / Nov-soy new-crop futures history (fetch_market_data.py).
# Compared day-by-day against a trailing 5-year average for that same day of year, same
# pattern as barge_rate_plot/cornprice_plot/soyprice_plot in the Plots panel. The average
# is built against a full-year date skeleton (not just this year's dates so far) so it
# draws all the way to December even while the current-year line stops at today, and is
# smoothed with a 7-day rolling mean since a raw day-of-year average across only 5 years
# of samples is too choppy to read.
try:
    futures_hist = pd.read_csv("futures_dec_nov_history.csv", parse_dates=["date"])
    futures_hist['doy'] = futures_hist['date'].dt.dayofyear
    futures_hist['year'] = futures_hist['date'].dt.year

    # Once a contract expires, the report starts basing quotes off *next* year's Dec/Nov
    # contract instead of leaving the field blank -- so the raw price jumps to a different
    # contract's level rather than just stopping. Cut off before that happens (Dec contract
    # data stops at the end of November, Nov contract data stops at the end of October) so
    # neither the current-year line nor the historical average keeps going past that point
    # using what is actually a different year's contract.
    month_day = futures_hist['date'].dt.strftime('%m-%d')
    futures_hist.loc[month_day > '11-30', 'corn_dec_futures'] = pd.NA
    futures_hist.loc[month_day > '10-31', 'soy_nov_futures'] = pd.NA

    trailing_5yr = futures_hist[(futures_hist['year'] < thisyear) & (futures_hist['year'] >= thisyear - 5)]
    mean_by_doy = trailing_5yr.groupby('doy')[['corn_dec_futures', 'soy_nov_futures']].mean() \
        .rename(columns={'corn_dec_futures': 'corn_dec_avg', 'soy_nov_futures': 'soy_nov_avg'}) \
        .sort_index()

    # Smooth only over the real (pre-cutoff) portion of each series, then stop -- smoothing
    # across the cutoff boundary would blend in the empty tail and taper the line off instead
    # of ending it cleanly.
    corn_cutoff_doy = pd.Timestamp(f'{thisyear}-11-30').dayofyear
    soy_cutoff_doy = pd.Timestamp(f'{thisyear}-10-31').dayofyear
    mean_by_doy = pd.DataFrame({
        'corn_dec_avg': mean_by_doy.loc[mean_by_doy.index <= corn_cutoff_doy, 'corn_dec_avg']
            .rolling(7, center=True, min_periods=1).mean(),
        'soy_nov_avg': mean_by_doy.loc[mean_by_doy.index <= soy_cutoff_doy, 'soy_nov_avg']
            .rolling(7, center=True, min_periods=1).mean(),
    })

    full_year_skeleton = pd.DataFrame({'date': pd.date_range(f'{thisyear}-01-01', f'{thisyear}-12-31', freq='D')})
    full_year_skeleton['doy'] = full_year_skeleton['date'].dt.dayofyear

    this_year_futures = futures_hist[futures_hist['year'] == thisyear][['date', 'corn_dec_futures', 'soy_nov_futures']]
    futures_weekly = full_year_skeleton.merge(this_year_futures, on='date', how='left').merge(mean_by_doy, on='doy', how='left')
except Exception as e:
    print(f"Warning: could not load Dec/Nov futures history ({e}). Barge Demand futures chart will be empty.")
    futures_weekly = pd.DataFrame(columns=['date', 'week_no', 'corn_dec_futures', 'soy_nov_futures', 'corn_dec_avg', 'soy_nov_avg'])

# Barge Demand indicator: which years counted as "low water" years, for the
# year-comparison scatterplot. A year qualifies if any gage's stage dropped to/below
# that gage's existing low-water threshold (GAGE_THRESHOLDS, same anchors used for the
# survey-risk map coloring) at any point that year. A handful of single-day 0.0 readings
# scattered across gages/years are sensor gaps, not real zero-stage events (surrounding
# days are all 20-40ft higher) -- dropped before taking the yearly min so they can't
# falsely flag a year as low-water.
_stage_for_low_water = river_stage_df[river_stage_df['stage'] != 0.0].copy()
_stage_for_low_water['year'] = _stage_for_low_water['date'].dt.year
_yearly_min_stage = _stage_for_low_water.groupby(['gage', 'year'])['stage'].min().reset_index()
_yearly_min_stage['threshold'] = _yearly_min_stage['gage'].map(GAGE_THRESHOLDS)
LOW_WATER_YEARS = set(
    _yearly_min_stage.loc[_yearly_min_stage['stage'] <= _yearly_min_stage['threshold'], 'year'].tolist()
)


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

river_line = linemerge(unary_union(mississippi.geometry))
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
    geom = linemerge(unary_union(clipped.geometry))
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
server = app.server
app.index_string = """
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>{%title%}</title>
        {%favicon%}
        <link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@500;600;700&display=swap" rel="stylesheet">
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

# Style constants for the slide-out 2026 insights panel — same mechanism as
# the plots panel above, just a second tab stacked on the same map edge
INSIGHTS_PANEL_WIDTH = "380px"
INSIGHTS_PANEL_CLOSED = {
    "position": "absolute", "top": "0", "right": "0", "height": "100%",
    "width": "0", "overflow": "hidden",
    "background": "rgba(255,255,255,0.97)",
    "box-shadow": "none",
    "transition": "width 0.25s ease",
    "zIndex": "16",
}
INSIGHTS_PANEL_OPEN = {
    **INSIGHTS_PANEL_CLOSED,
    "width": INSIGHTS_PANEL_WIDTH,
    "box-shadow": "-2px 0 6px rgba(0,0,0,0.3)",
    # extra right padding keeps text from running under the toggle tab, which
    # floats on top of the panel at the same right edge while it's open
    "padding": "24px 60px 24px 24px",
    "overflow-y": "auto",
}

# Shared styling for the two right-edge tabs (insights + price plots)
INSIGHTS_TOGGLE_LABEL = ["2026", html.Br(), "Insights", html.Br(), "Summary"]
PLOTS_TOGGLE_LABEL = ["View", html.Br(), "Price", html.Br(), "Plots"]
TAB_BASE_STYLE = {
    "background": "#1b3a5c", "color": "white", "border": "2px solid white",
    "border-radius": "6px 0 0 6px", "padding": "12px 8px", "cursor": "pointer",
    "font-size": "15px", "text-align": "center",
}
TAB_HIDDEN_STYLE = {**TAB_BASE_STYLE, "display": "none"}

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


# AIS-derived dredge activity (2021-2024) -- distinct from the manually logged USACE
# notices above. dredge_events_2021_2025 shapefile has one row per year, each a
# MultiPolygon of that year's individual dredge-event footprints aggregated together;
# the CSV has one row per individual event (vessel, MMSI, dates, duration) with a
# center point, giving per-event hover detail the polygon layer doesn't have on its own.
AIS_DREDGE_BY_YEAR = {}
_ais_dredge_shp = Path("dredge_events_2021_2025/dredge_events_2021_2025.shp")
_ais_dredge_csv = Path("dredge_events_2021_2025.csv")
if _ais_dredge_shp.exists() and _ais_dredge_csv.exists():
    _ais_poly = gpd.read_file(_ais_dredge_shp).to_crs(4326)
    _ais_pts = pd.read_csv(_ais_dredge_csv)
    for _, _row in _ais_poly.iterrows():
        _year = int(_row["year"])
        _lons, _lats = _geom_to_lonlat(_row.geometry)
        _pts = _ais_pts[_ais_pts["year"] == _year]
        _hovertexts = [
            "<br>".join([
                "<b><span style='font-size:16px'>Dredging Completed</span></b>",
                f"<b><span style='font-size:16px'>"
                f"{pd.to_datetime(p.start_date).strftime('%B %-d, %Y')} – "
                f"{pd.to_datetime(p.end_date).strftime('%B %-d, %Y')}</span></b>",
                p.vessel_name,
                f"{p.duration_hrs:.1f} hours operating",
            ])
            for p in _pts.itertuples()
        ]
        AIS_DREDGE_BY_YEAR[_year] = {
            "lons": _lons,
            "lats": _lats,
            "num_events": int(_row["num_events"]),
            "num_polygons": len(_row.geometry.geoms) if _row.geometry.geom_type == "MultiPolygon" else 1,
            "point_lons": _pts["center_lon"].tolist(),
            "point_lats": _pts["center_lat"].tolist(),
            "point_hovertext": _hovertexts,
        }


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
ZOOM_MEMO_HIDDEN = {"display": "none"}
ZOOM_MEMO_VISIBLE = {
    "width": "200px", "background": "rgba(255,255,255,0.9)",
    "padding": "8px 14px", "border-radius": "8px",
    "font-family": "Arial, sans-serif", "font-size": "11px",
    "font-style": "italic", "color": "#777", "line-height": "1.35",
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
# Welcome intro modal -- shown over the map on first load, dismissed with the ✕ and
# never shown again for the rest of the session (no re-open trigger, unlike the other
# detail boxes).
WELCOME_BACKDROP_VISIBLE = {
    "position": "absolute", "top": 0, "left": 0, "width": "100%", "height": "100%",
    "background": "rgba(0,0,0,0.35)", "zIndex": "40",
}
WELCOME_BACKDROP_HIDDEN = {"display": "none"}
WELCOME_BOX_VISIBLE = {
    "position": "absolute", "top": "50%", "left": "50%", "transform": "translate(-50%, -50%)",
    "zIndex": "41", "width": "480px", "max-width": "90%",
    "background": "white", "padding": "26px 30px", "border-radius": "10px",
    "box-shadow": "0 4px 24px rgba(0,0,0,0.4)", "font-family": "Arial, sans-serif",
}
WELCOME_BOX_HIDDEN = {"display": "none"}

# Top-level pages -- clicking a header nav link switches which one is visible.
# "River Conditions" (the map) is the default; "Barge Demand" is a full page, not
# an overlay, so it gets the same amount of room as the map does.
RIVER_PAGE_VISIBLE = {"display": "block"}
RIVER_PAGE_HIDDEN = {"display": "none"}
DEMAND_PAGE_VISIBLE = {"display": "block", "background": "white", "min-height": "92vh", "padding": "28px 40px"}
DEMAND_PAGE_HIDDEN = {"display": "none"}
ABOUT_PAGE_VISIBLE = {"display": "block", "background": "white", "min-height": "92vh", "padding": "28px 40px"}
ABOUT_PAGE_HIDDEN = {"display": "none"}

COMPARE_YEARS_TOGGLE_STYLE = {
    "background": "#2166ac", "color": "white", "border": "none",
    "border-radius": "8px", "padding": "16px 30px", "font-size": "20px",
    "cursor": "pointer", "margin-top": "10px",
    "font-family": "'DM Sans', sans-serif", "font-weight": "600",
}
COMPARE_YEARS_BOX_HIDDEN = {"display": "none"}
COMPARE_YEARS_BOX_VISIBLE = {
    "display": "block", "position": "fixed", "top": "50%", "left": "50%",
    "transform": "translate(-50%, -50%)", "zIndex": "1000",
    "border": "1px solid #b8d4ec", "border-radius": "8px",
    "padding": "36px 24px 20px 24px", "background": "#f7fbfe",
    "box-shadow": "0 4px 24px rgba(0,0,0,0.35)",
    "max-height": "90vh", "overflow-y": "auto",
}

NAV_LINK_BASE = {
    "background": "none", "border": "none", "cursor": "pointer",
    "font-size": "15px", "padding": "6px 2px", "margin-left": "24px",
    "font-family": "'DM Sans', sans-serif",
}
NAV_LINK_ACTIVE = {**NAV_LINK_BASE, "color": "white", "font-weight": "bold", "border-bottom": "2px solid white"}
NAV_LINK_INACTIVE = {**NAV_LINK_BASE, "color": "rgba(255,255,255,0.65)", "font-weight": "normal", "border-bottom": "2px solid transparent"}

SECTION_HEADER_STYLE = {"margin": "0 0 4px 0", "font-size": "18px", "color": "#1b3a5c", "font-family": "'DM Sans', sans-serif", "font-weight": "600"}
SECTION_SUBTEXT_STYLE = {"margin": "0 0 10px 0", "font-size": "17px", "color": "#555", "font-family": "'DM Sans', sans-serif"}
CAPTION_STYLE = {"margin-top": "0px", "font-size": "14px", "color": "#777", "font-style": "italic", "font-family": "'DM Sans', sans-serif"}

CROP_COLORS = {"corn": "#006837", "soybean": "#f1a340"}
# current-year bar on the production chart is highlighted in a lighter shade of each
# crop's own color, rather than one shared highlight color across both charts
CURRENT_YEAR_HIGHLIGHT_COLORS = {"corn": "#78c679", "soybean": "#d95f0e"}


def build_production_chart(crop):
    col = f"{crop}_production_million_bu"
    label = "Corn" if crop == "corn" else "Soybean"
    color = CROP_COLORS[crop]

    hist = grain_production_history[["year", col]].dropna().sort_values("year") if col in grain_production_history else pd.DataFrame(columns=["year", col])
    years = hist["year"].tolist()
    values = hist[col].tolist()
    bar_colors = [color] * len(years)

    caption = ""
    if wasde_latest is not None:
        current_year = int(str(wasde_latest["marketing_year"]).split("/")[0])
        current_value = wasde_latest[col]
        years = years + [current_year]
        values = values + [current_value]
        bar_colors = bar_colors + [CURRENT_YEAR_HIGHLIGHT_COLORS[crop]]
        caption = f"The {current_year} estimate is from the {wasde_latest['report_label']} WASDE report."

    fig = go.Figure()
    fig.add_trace(go.Bar(x=years, y=values, marker_color=bar_colors, showlegend=False))
    fig.update_layout(
        title=dict(
            text=f"US <b>{label}</b> Production",
            subtitle=dict(text="Source: U.S. Department of Agriculture National Agricultural Statistics Service", font=dict(size=10, color="#999")),
        ),
        yaxis_title="Million bushels",
        xaxis=dict(type="category"),
        height=300,
        margin=dict(l=50, r=20, t=55, b=25),
    )
    return fig, caption


def build_futures_chart(crop):
    if crop == "corn":
        prefix, month_label, crop_label, color = "corn_dec", "December", "Corn", CROP_COLORS["corn"]
    else:
        prefix, month_label, crop_label, color = "soy_nov", "November", "Soybean", CROP_COLORS["soybean"]

    value_col = f"{prefix}_futures"
    avg_col = f"{prefix}_avg"
    df = futures_weekly[["date", value_col, avg_col]] if value_col in futures_weekly else pd.DataFrame(columns=["date", value_col, avg_col])

    # extra headroom above the highest line so the top-left legend box doesn't sit on top of it
    combined = pd.concat([df[value_col], df[avg_col]]).dropna()
    if not combined.empty:
        y_min, y_max = combined.min(), combined.max()
        pad = (y_max - y_min) * 0.15 or 0.5
        yaxis_range = [y_min - pad, y_max + pad * 3]
    else:
        yaxis_range = None

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["date"], y=df[value_col],
        mode="lines+markers", line=dict(width=2, color=color), marker=dict(size=4), name=str(thisyear),
        hovertemplate="$%{y:.2f}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=df["date"], y=df[avg_col],
        mode="lines", line=dict(width=2, color="grey", dash="dash"), name="Average",
        hovertemplate="$%{y:.2f}<extra></extra>",
    ))
    fig.update_layout(
        title=dict(
            text=f"<b>{month_label} {crop_label}</b> Futures Contract Prices",
            subtitle=dict(text="Source: USDA Agricultural Marketing Service, MyMarketNews", font=dict(size=10, color="#999")),
        ),
        yaxis_title="Price ($/bushel)",
        yaxis=dict(range=yaxis_range),
        xaxis=dict(range=[f"{thisyear}-01-01", f"{thisyear}-12-31"]),
        height=300,
        legend=dict(x=0.02, y=0.98, xanchor="left", yanchor="top",
                     bgcolor="rgba(255,255,255,0.6)", bordercolor="black", borderwidth=1),
        margin=dict(l=50, r=20, t=55, b=40),
        hovermode="x unified",
    )
    return fig


def build_compare_years_data():
    """One row per year: a composite production index against a composite demand-price
    index. Production is a straight sum of corn + soybean production (million bushels) --
    no normalization, since both crops are already in the same unit. Price can't just be
    summed the same way (soybean futures trade at a persistently higher $/bu level than
    corn, so a raw sum would be soybean-price-dominated), so corn's price is first rescaled
    by the ratio of the two crops' historical mean prices (putting it on soybean's price
    scale) before the two are averaged.

    Production: NASS final-production years (grain_production_history, 2016+) are used
    where available; wasde_production_backfill fills in earlier years (back to 2008) NASS
    doesn't cover. Price: the futures contract is quoted daily starting many months before
    expiration, but we want a single representative pre-harvest value per year, so this
    averages only the September quotes (the month new-crop harvest expectations firm up),
    not the full year."""
    prod_nass = grain_production_history.dropna(subset=["corn_production_million_bu", "soybean_production_million_bu"]).copy()
    prod_backfill = wasde_production_backfill[~wasde_production_backfill["year"].isin(prod_nass["year"])].dropna(
        subset=["corn_production_million_bu", "soybean_production_million_bu"]
    ).copy()
    prod = pd.concat([prod_nass, prod_backfill], ignore_index=True)
    prod["production_index"] = prod["corn_production_million_bu"] + prod["soybean_production_million_bu"]
    rows = prod[["year", "production_index"]]

    # Current year's production is only a WASDE estimate, not a finished harvest, so it
    # isn't part of the historical set above -- add it in the same way (a raw sum).
    if wasde_latest is not None:
        current_year = int(str(wasde_latest["marketing_year"]).split("/")[0])
        current_index = wasde_latest["corn_production_million_bu"] + wasde_latest["soybean_production_million_bu"]
        rows = pd.concat([rows, pd.DataFrame([{"year": current_year, "production_index": current_index}])], ignore_index=True)

    sept_futures = futures_hist[futures_hist["date"].dt.month == 9]
    yearly_price = sept_futures.groupby("year")[["corn_dec_futures", "soy_nov_futures"]].mean().reset_index()
    corn_mean = yearly_price["corn_dec_futures"].mean()
    soy_mean = yearly_price["soy_nov_futures"].mean()
    price_ratio = soy_mean / corn_mean
    yearly_price["price_index"] = (price_ratio * yearly_price["corn_dec_futures"] + yearly_price["soy_nov_futures"]) / 2

    # September hasn't happened yet for the in-progress year, so it has no row above --
    # use the most recent available Dec-corn/Nov-soy quote instead, rescaled with the same
    # price_ratio so it lands on the same scale as every other year's index.
    if thisyear not in yearly_price["year"].values:
        latest = futures_hist[futures_hist["year"] == thisyear].dropna(
            subset=["corn_dec_futures", "soy_nov_futures"], how="all"
        ).sort_values("date")
        if not latest.empty:
            corn_latest, soy_latest = latest.iloc[-1][["corn_dec_futures", "soy_nov_futures"]]
            current_index = (price_ratio * corn_latest + soy_latest) / 2
            yearly_price = pd.concat(
                [yearly_price, pd.DataFrame([{"year": thisyear, "price_index": current_index}])],
                ignore_index=True,
            )

    df = rows.merge(yearly_price[["year", "price_index"]], on="year", how="left")
    df = df.dropna(subset=["production_index", "price_index"]).sort_values("year")
    df["low_water"] = df["year"].isin(LOW_WATER_YEARS)
    df["is_current_year"] = df["year"] == thisyear
    return df


def build_compare_years_fig(df):
    fig = go.Figure()

    normal = df[~df["low_water"] & ~df["is_current_year"]]
    low_water = df[df["low_water"] & ~df["is_current_year"]]
    current = df[df["is_current_year"]]

    # customdata is stored as strings, not numpers -- plotly silently drops customdata
    # from click/hover events when it's a homogeneous numeric array (it gets encoded as a
    # compact typed-array blob that Dash's click handler doesn't unpack per-point).
    fig.add_trace(go.Scatter(
        x=normal["price_index"], y=normal["production_index"],
        customdata=normal["year"].astype(str).values.reshape(-1, 1),
        mode="markers", marker=dict(size=12, color="#8ea6bd"), name="Other years",
        hovertemplate="%{customdata[0]} (click to see barge rates for %{customdata[0]})<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=low_water["price_index"], y=low_water["production_index"],
        customdata=low_water["year"].astype(str).values.reshape(-1, 1),
        mode="markers", marker=dict(size=13, color="#d95f0e"), name="Low-water years",
        hovertemplate="%{customdata[0]} (low-water year)<br>(click to see barge rates for %{customdata[0]})<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=current["price_index"], y=current["production_index"],
        customdata=current["year"].astype(str).values.reshape(-1, 1),
        mode="markers", marker=dict(size=17, color="#1b3a5c", symbol="star"), name=f"{thisyear} (this year)",
        hovertemplate="%{customdata[0]} (this year)<br>(click to see barge rates for %{customdata[0]})<extra></extra>",
    ))

    # dividers split the plot into 4 quadrants -- e.g. top-left is a low-price,
    # high-production year. Centering on the mean/median pulls the divider toward
    # whichever side has more clustered years (here, several lower-price years drag
    # the average down), leaving a lot of dead space on the other side. Centering on
    # the midpoint of the actual min/max range instead keeps the divider visually
    # balanced between the two extremes, with only the padding -- not the whole
    # range -- added symmetrically around it.
    x_center = (df["price_index"].min() + df["price_index"].max()) / 2
    y_center = (df["production_index"].min() + df["production_index"].max()) / 2
    x_half = (df["price_index"].max() - df["price_index"].min()) / 2 * 1.15
    y_half = (df["production_index"].max() - df["production_index"].min()) / 2 * 1.15
    fig.add_vline(x=x_center, line_dash="dot", line_color="#bbb")
    fig.add_hline(y=y_center, line_dash="dot", line_color="#bbb")

    # Brief how-to-read-this-chart note in the right margin, above the legend (which is
    # vertically centered at paper y=0.5). Line breaks are manual (<br>) rather than
    # relying on the `width` auto-wrap property, since that left the text running off
    # the page in the ~150px margin reserved by margin.r below.
    fig.add_annotation(
        xref="paper", yref="paper", x=1.04, y=0.70, xanchor="left", yanchor="bottom",
        align="center", showarrow=False, width=140,
        text="Years closer to<br>the <b>top right</b><br>had higher barge<br>demand, making a<br>"
             "harvest-season<br>rate spike more<br>likely.",
        font=dict(size=12, color="#666"),
    )

    fig.update_layout(
        title=dict(text="<b>Grain Production vs Grain Prices</b>", font=dict(size=19)),
        # x-axis title is blanked here and rebuilt in HTML just below the graph (with a "?"
        # info icon in front of it) -- Plotly's native axis title can't have an interactive
        # hover icon placed next to it.
        xaxis=dict(title="", showticklabels=False,
                    range=[x_center - x_half, x_center + x_half]),
        yaxis=dict(title="Grain Production Index", title_font=dict(size=15), showticklabels=False,
                    range=[y_center - y_half, y_center + y_half]),
        height=450,
        width=596,
        # legend sits to the right of the plot, outside the data area, so it can never overlap a dot.
        # r=150 is sized for the legend text itself (doesn't shrink with the plot), not the plot square.
        legend=dict(orientation="v", x=1.02, y=0.5, xanchor="left", yanchor="middle",
                     bgcolor="rgba(255,255,255,0.6)", bordercolor="black", borderwidth=1),
        # b is minimal since the x-axis title text itself was removed (rebuilt in HTML
        # directly below the graph) and showticklabels=False means there's no tick text
        # to leave room for either.
        margin=dict(l=40, r=156, t=40, b=2),
    )
    return fig


# Week number doesn't mean anything to a reader on its own -- map the first week of each
# month (using an arbitrary non-leap reference year, since ISO week numbering only depends
# on day-of-week/day-of-year) to that month's abbreviation, so the x-axis reads as a rough
# calendar instead of a raw week count.
_month_starts = pd.date_range("2001-01-01", periods=12, freq="MS")
MONTH_WEEK_TICKVALS = _month_starts.isocalendar()["week"].tolist()
MONTH_WEEK_TICKTEXT = _month_starts.strftime("%b").tolist()


def _year_overlay_traces(df, x_col, y_col, year, color, hover_date_col, value_hover_fmt,
                          compare_year=None, compare_color="#000000", month_label_col=None):
    """Background line per non-selected year in light grey, with the selected year's line
    highlighted in `color` on top, and an optional second `compare_year` highlighted in
    `compare_color` for side-by-side comparison. Years aren't aligned on calendar dates (a
    given week/day lands on a different date each year), so x_col is expected to be a
    year-agnostic axis like ISO week number, with hover_date_col supplying the real date
    for the tooltip. Pass `month_label_col` for forward-rate series where the hover should
    also show which calendar month the quoted rate's contract is for."""
    def _highlighted_trace(y, line_color, rank):
        df_y = df[df["year"] == y].sort_values(x_col)
        if month_label_col:
            customdata = df_y[[hover_date_col, month_label_col]]
            hovertemplate = (
                f"%{{customdata[0]|%b %d, %Y}}: {value_hover_fmt}"
                f"<br>Contract month: %{{customdata[1]}}<extra></extra>"
            )
        else:
            customdata = df_y[hover_date_col]
            hovertemplate = f"%{{customdata|%b %d, %Y}}: {value_hover_fmt}<extra></extra>"
        return go.Scatter(
            x=df_y[x_col], y=df_y[y_col],
            customdata=customdata,
            mode="lines", line=dict(width=2.5, color=line_color), name=str(y), legendrank=rank,
            hovertemplate=hovertemplate,
        )

    traces = []
    skip_years = {year, compare_year}
    other_years = sorted(y for y in df["year"].unique() if y not in skip_years)
    for i, other_year in enumerate(other_years):
        df_other = df[df["year"] == other_year].sort_values(x_col)
        traces.append(go.Scatter(
            x=df_other[x_col], y=df_other[y_col],
            mode="lines", line=dict(width=1, color="#cccccc"),
            name="Other years", legendgroup="other_years", showlegend=(i == 0),
            legendrank=3, hoverinfo="skip",
        ))
    if compare_year is not None and compare_year != year:
        traces.append(_highlighted_trace(compare_year, compare_color, rank=2))
    traces.append(_highlighted_trace(year, color, rank=1))
    return traces


def build_barge_rate_year_fig(year):
    fig = go.Figure(data=_year_overlay_traces(
        barge_rates, "week_no", "stlrate_per_ton", year, "#7b3fa0", "week", "$%{y:.2f}"
    ))
    fig.update_layout(
        title=dict(text="<b>St Louis to New Orleans Spot Barge Rates</b>", font=dict(size=17)),
        xaxis=dict(tickvals=MONTH_WEEK_TICKVALS, ticktext=MONTH_WEEK_TICKTEXT,
                    automargin=False),
        yaxis=dict(title="Barge Rate ($/ton)", automargin=False),
        height=300,
        width=596,
        legend=dict(orientation="v", x=1.02, y=0.5, xanchor="left", yanchor="middle", font=dict(size=13),
                     bgcolor="rgba(255,255,255,0.6)", bordercolor="black", borderwidth=1),
        margin=dict(l=40, r=156, t=30, b=25),
        hovermode="closest",
    )
    return fig


def build_barge_rate_placeholder_fig():
    fig = go.Figure()
    fig.update_layout(
        height=300,
        width=596,
        margin=dict(l=40, r=156, t=15, b=15),
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        annotations=[dict(
            text="Click a year above to see that year's barge rates vs. the average.",
            xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False,
            font=dict(size=12, color="#888"),
        )],
    )
    return fig


DEMAND_EXPLANATION_BOX_STYLE = {
    "flex": "1", "min-width": "400px",
    "border": "1px solid #b8d4ec", "border-radius": "8px",
    "padding": "16px 20px", "background": "#eaf3fb",
}


def build_demand_explanation_boxes():
    return html.Div(
        style={"display": "flex", "gap": "30px", "flex-wrap": "wrap", "margin-bottom": "28px"},
        children=[
            html.Div(
                className="demand-explanation-box",
                style=DEMAND_EXPLANATION_BOX_STYLE,
                children=[
                    html.H4("1. How much grain is the US expected to produce?", style=SECTION_HEADER_STYLE),
                    html.Div(
                        "A bigger harvest means more grain competing for barge space to move it to market.",
                        style=SECTION_SUBTEXT_STYLE
                    ),
                ]
            ),
            html.Div(
                className="demand-explanation-box",
                style=DEMAND_EXPLANATION_BOX_STYLE,
                children=[
                    html.H4("2. What does global grain demand look like?", style=SECTION_HEADER_STYLE),
                    html.Div(
                        "Higher futures prices signal stronger global demand pulling more grain "
                        "toward export, and more grain moving by barge.",
                        style=SECTION_SUBTEXT_STYLE
                    ),
                ]
            ),
        ]
    )


def build_demand_crop_section(production_fig, production_caption, futures_fig):
    return html.Div(
        style={"margin-bottom": "40px"},
        children=[
            html.Div(
                style={"display": "flex", "gap": "30px", "flex-wrap": "wrap"},
                children=[
                    html.Div(
                        className="crop-chart-col",
                        style={"flex": "1", "min-width": "400px"},
                        children=[
                            dcc.Graph(figure=production_fig, config={"displayModeBar": False}),
                            html.Div(production_caption, style=CAPTION_STYLE),
                        ]
                    ),
                    html.Div(
                        className="crop-chart-col",
                        style={"flex": "1", "min-width": "400px"},
                        children=[
                            dcc.Graph(figure=futures_fig, config={"displayModeBar": False}),
                        ]
                    ),
                ]
            ),
        ]
    )


def _layer_info_icon(source, description, wide=False):
    """Small '?' badge for a layer-toggle label; CSS-only hover tooltip (see custom.css)
    shows the data source and an explanation of what the layer means. `wide` widens the
    tooltip box for longer explanations (e.g. the riverbed-survey risk breakdown).
    `source=None` skips the "Source: ..." line, for tooltips explaining a calculation
    rather than a data source (e.g. the compare-years index)."""
    tooltip_class = "layer-info-tooltip layer-info-tooltip-wide" if wide else "layer-info-tooltip"
    source_children = []
    if source is not None:
        sources = source if isinstance(source, list) else [source]
        source_children = [
            html.Span(f"Source: {s}", style={"font-weight": "bold", "display": "block", "margin-bottom": "3px"})
            for s in sources
        ]
    description_children = description if isinstance(description, list) else [description]
    return html.Span(
        [
            "?",
            html.Span(
                [*source_children, *description_children],
                className=tooltip_class,
            ),
        ],
        className="layer-info-icon",
    )


_corn_production_fig, _corn_production_caption = build_production_chart("corn")
_soybean_production_fig, _soybean_production_caption = build_production_chart("soybean")
_corn_futures_fig = build_futures_chart("corn")
_soybean_futures_fig = build_futures_chart("soybean")
_compare_years_fig = build_compare_years_fig(build_compare_years_data())
_barge_rate_placeholder_fig = build_barge_rate_placeholder_fig()

app.layout = html.Div(
    style={"width": "100%", "margin": "0", "padding": "0"},
    children=[

        # Title bar
        html.Div(
            id="app-header",
            style={
                "display": "flex", "align-items": "center", "justify-content": "space-between",
                "width": "100%", "box-sizing": "border-box",
                "height": "55px", "padding": "0 20px",
                "background": "#2166ac",
            },
            children=[
                html.H2(
                    "Grain Transportation Conditions",
                    id="app-title",
                    style={
                        "margin": "0 0 0 30px", "color": "white",
                        "font-family": "'DM Sans', sans-serif",
                        "font-weight": "700",
                        "font-size": "30px", "letter-spacing": "1.5px",
                    }
                ),
                html.Div(
                    id="nav-links",
                    style={"display": "flex", "align-items": "center"},
                    children=[
                        html.Button("About", id="nav-about", n_clicks=0, style=NAV_LINK_INACTIVE),
                        html.Button("Mississippi River Conditions", id="nav-river-conditions", n_clicks=0, style=NAV_LINK_ACTIVE),
                        html.Button("Barge Demand", id="nav-barge-demand", n_clicks=0, style=NAV_LINK_INACTIVE),
                    ]
                ),
            ]
        ),

        dcc.Store(id="active-panel-store", data=None),
        dcc.Store(id="notice-detail-store", data=None),
        dcc.Store(id="selected-survey-store", data=None),
        dcc.Store(id="selected-gage-store", data=None),
        dcc.Store(id="gage-freq-store", data=None),
        dcc.Store(id="selected-shoaling-mile-store", data=None),

        ##################################
        # Map fills the full width; controls and plots panel float on top of it
        html.Div(
            id="river-page",
            style=RIVER_PAGE_VISIBLE,
            children=[
            html.Div(
            id="map-container",
            style={"position": "relative", "width": "100%", "height": "92vh"},
            children=[

                # Map, edge to edge
                dcc.Graph(id="map", style={"height": "100%", "width": "100%"}, config={"displayModeBar": False}),

                # Welcome intro -- explains the map/site on first load, dismissed for the session
                html.Div(id="welcome-backdrop", style=WELCOME_BACKDROP_VISIBLE),
                html.Div(
                    id="welcome-box",
                    style=WELCOME_BOX_VISIBLE,
                    children=[
                        html.Button(
                            "✕", id="welcome-close",
                            style={
                                "position": "absolute", "top": "10px", "right": "14px",
                                "border": "none", "background": "none", "cursor": "pointer",
                                "font-size": "18px", "color": "#888",
                            }
                        ),
                        html.H3(
                            "Welcome!",
                            style={
                                "margin": "0 0 12px 0", "font-family": "'DM Sans', sans-serif",
                                "font-weight": "700", "font-size": "21px", "color": "#1b3a5c",
                            }
                        ),
                        html.Div(
                            [
                                html.Span(
                                    "The Mississippi River has been dropping to critically low levels "
                                    "more often than ever. ",
                                    style={"font-weight": "700"}
                                ),
                                "These low water levels typically hit during the ",
                                html.Span("September-November", style={"text-decoration": "underline"}),
                                " harvest season, which is the busiest stretch for barge navigation.",
                            ],
                            style={"font-size": "15px", "color": "#333", "line-height": "1.5", "margin-bottom": "10px"}
                        ),
                        html.Div(
                            "This dashboard keeps you updated on barge navigation, including",
                            style={"font-size": "15px", "color": "#333", "line-height": "1.5"}
                        ),
                        html.Ul(
                            [
                                html.Li([
                                    html.Span("Riverbed conditions", style={"font-weight": "700"}),
                                    ": surveys and shoaling reports show where the river is most "
                                    "likely to ground a barge under low water.",
                                ]),
                                html.Li([
                                    html.Span("Grain markets", style={"font-weight": "700"}),
                                    ": production and price data show what's driving barge "
                                    "demand and rates.",
                                ]),
                            ],
                            style={"font-size": "15px", "color": "#333", "line-height": "1.5", "margin": "6px 0 10px 0", "padding-left": "22px"}
                        ),
                        html.Div(
                            "Explore the map and charts to see how this year compares to past ones.",
                            style={"font-size": "15px", "color": "#333", "line-height": "1.5", "margin-bottom": "10px"}
                        ),
                        html.Div(
                            "Provided for informational purposes only; not an official navigation "
                            "aid. Underlying data comes from public government sources, but its "
                            "processing, analysis, and presentation here are independent and not "
                            "reviewed or endorsed by those agencies.",
                            style={"font-size": "12px", "color": "#888", "font-style": "italic", "line-height": "1.4"}
                        ),
                    ]
                ),

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
                    id="survey-panels-stack",
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

                        # Reminder that the depth-legend map only renders once zoomed in far
                        # enough on the selected survey point -- shown alongside the current
                        # gage reading so it's visible right when someone clicks a survey dot
                        html.Div(
                            id="survey-zoom-memo",
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
                    id="map-controls",
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
                            id="year-select-wrapper",
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
                            id="layer-toggle-wrapper",
                            style={"width": "240px"},
                            children=[
                                html.Label("Layers", style={"font-weight": "bold", "margin-bottom": "6px", "display": "block"}),
                                dcc.Checklist(
                                    id="layer-toggle",
                                    options=[
                                        {
                                            "label": html.Span([
                                                html.Div([
                                                    html.Span("Riverbed Surveys", style={"font-size": "16px"}),
                                                    _layer_info_icon(
                                                        "U.S. Army Corps of Engineers eHydro",
                                                        [
                                                            html.Span(
                                                                "Hydrographic surveys (“riverbed surveys”) "
                                                                "measure the elevation of the riverbed. We analyze "
                                                                "each survey to estimate how shallow the channel "
                                                                "could get at that location if the river dropped "
                                                                "to a historic low-water stage.",
                                                                style={"display": "block", "margin-bottom": "6px"},
                                                            ),
                                                            html.Span([
                                                                html.Span("High risk: ", style={"font-weight": "bold"}),
                                                                "a 9-ft-deep path may not exist across the channel, "
                                                                "so barge traffic is likely to be disrupted under "
                                                                "low water conditions.",
                                                            ], style={"display": "block", "margin-bottom": "4px"}),
                                                            html.Span([
                                                                html.Span("Medium risk: ", style={"font-weight": "bold"}),
                                                                "a 9-ft-deep path should exist, but it may be "
                                                                "narrow or prone to shoaling.",
                                                            ], style={"display": "block", "margin-bottom": "4px"}),
                                                            html.Span([
                                                                html.Span("Low risk: ", style={"font-weight": "bold"}),
                                                                "no barge navigation issues expected, even under "
                                                                "low water.",
                                                            ], style={"display": "block"}),
                                                        ],
                                                        wide=True,
                                                    ),
                                                    html.Div(
                                                        "Navigation Risk under Low Water:",
                                                        style={"font-size": "13px", "display": "block", "width": "100%"}
                                                    ),
                                                    html.Div(
                                                        style={"display": "flex", "gap": "10px", "margin-top": "5px", "margin-left": "4px"},
                                                        children=[
                                                            html.Div([
                                                                html.Div(style={"width": "12px", "height": "12px", "border-radius": "50%", "background": RISK_BINS[0][1], "display": "inline-block", "margin-right": "4px", "vertical-align": "middle"}),
                                                                html.Span("Low", style={"font-size": "13px", "vertical-align": "middle"}),
                                                            ]),
                                                            html.Div([
                                                                html.Div(style={"width": "12px", "height": "12px", "border-radius": "50%", "background": RISK_BINS[1][1], "display": "inline-block", "margin-right": "4px", "vertical-align": "middle"}),
                                                                html.Span("Medium", style={"font-size": "13px", "vertical-align": "middle"}),
                                                            ]),
                                                            html.Div([
                                                                html.Img(src="/assets/at_risk_marker.png", height="16", style={"display": "inline-block", "margin-right": "4px", "vertical-align": "middle"}),
                                                                html.Span("High", style={"font-size": "13px", "vertical-align": "middle"}),
                                                            ]),
                                                        ]
                                                    ),
                                                ])
                                            ]),
                                            "value": "bathy",
                                        },
                                        {
                                            "label": html.Span([
                                                html.Img(src="/assets/raindrop.png", height="22", style={"vertical-align": "middle", "margin-right": "5px"}),
                                                "Stream Gage",
                                                _layer_info_icon(
                                                    "USGS / NOAA-NWS",
                                                    [
                                                        html.Span(
                                                            "Daily river stage (water level) readings at the "
                                                            "St. Louis, Memphis, and Greenville gages.",
                                                            style={"display": "block", "margin-bottom": "6px"},
                                                        ),
                                                        html.Span(
                                                            "River stage measures the elevation of the river "
                                                            "surface rather than the actual depth of the river.",
                                                            style={"display": "block"},
                                                        ),
                                                    ],
                                                    wide=True,
                                                ),
                                            ]),
                                            "value": "stage",
                                        },
                                        {
                                            "label": html.Span([
                                                html.Img(src="/assets/dredge_marker.png", height="22", style={"vertical-align": "middle", "margin-right": "5px"}),
                                                "Dredging",
                                                _layer_info_icon(
                                                    [
                                                        "U.S. Coast Guard Broadcast Notice to Mariners (2026)",
                                                        "Marine Cadastre AIS Data (2021–2025)",
                                                    ],
                                                    [
                                                        html.Span(
                                                            "Dredging is performed by the U.S. Army Corps of "
                                                            "Engineers to remove sediment from the riverbed and "
                                                            "deepen the channel for navigation.",
                                                            style={"display": "block", "margin-bottom": "6px"},
                                                        ),
                                                        html.Span(
                                                            "Recent dredging reports come from USCG notices.",
                                                            style={"display": "block", "margin-bottom": "6px"},
                                                        ),
                                                        html.Span(
                                                            "Exact dredging locations are available through 2025, "
                                                            "calculated from AIS vessel location data.",
                                                            style={"display": "block"},
                                                        ),
                                                    ],
                                                    wide=True,
                                                ),
                                            ]),
                                            "value": "dredging",
                                        },
                                        {
                                            "label": html.Span([
                                                html.Img(src="/assets/shoaling_marker.png", height="22", style={"vertical-align": "middle", "margin-right": "5px"}),
                                                "Shoaling",
                                                _layer_info_icon(
                                                    "U.S. Coast Guard Broadcast Notice to Mariners",
                                                    "Reports of shoaling (sediment buildup on the riverbed) "
                                                    "indicate restricted channel depth and a higher risk of "
                                                    "barge grounding."
                                                ),
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
                                                _layer_info_icon(
                                                    "U.S. Coast Guard Broadcast Notice to Mariners",
                                                    [
                                                        html.Span(
                                                            "The USCG imposes draft restrictions when water "
                                                            "levels drop to critical lows.",
                                                            style={"display": "block", "margin-bottom": "6px"},
                                                        ),
                                                        html.Span(
                                                            "A barge's draft is how far it sits below the "
                                                            "waterline. The deeper the draft, the greater the "
                                                            "risk of grounding in shallow water.",
                                                            style={"display": "block", "margin-bottom": "6px"},
                                                        ),
                                                        html.Span(
                                                            "Operators reduce draft by loading less cargo.",
                                                            style={"display": "block"},
                                                        ),
                                                    ],
                                                    wide=True,
                                                ),
                                            ]),
                                            "value": "draft",
                                        },
                                    ],
                                    value=["bathy", "stage"],
                                    inputStyle={"margin-right": "6px"},
                                    labelStyle={"display": "flex", "align-items": "center", "margin-bottom": "5px", "font-size": "16px"},
                                )
                            ]
                        )
                    ]
                ),



                # Tabs to open/close the insights and plots panels, stacked on
                # the right edge of the map. The pair is centered together as
                # one group (translateY(-50%) on the wrapper, not each tab) so
                # adding the insights tab above didn't require re-centering math.
                html.Div(
                    id="right-tabs-stack",
                    style={
                        "position": "absolute", "top": "50%", "right": "0",
                        "transform": "translateY(-50%)", "zIndex": "20",
                        "display": "flex", "flex-direction": "column", "gap": "18px",
                    },
                    children=[
                        html.Button(
                            INSIGHTS_TOGGLE_LABEL,
                            id="insights-toggle",
                            n_clicks=0,
                            style=TAB_BASE_STYLE,
                        ),
                        html.Button(
                            PLOTS_TOGGLE_LABEL,
                            id="plots-toggle",
                            n_clicks=0,
                            style=TAB_BASE_STYLE,
                        ),
                    ]
                ),

                # Insights panel, slides in over the map
                html.Div(
                    id="insights-panel",
                    style=INSIGHTS_PANEL_CLOSED,
                    children=[
                        html.H4("2026 Insights Summary", style=SECTION_HEADER_STYLE),
                        html.P(
                            "River stage is trending downward and running below this week's "
                            "average, but survey data shows the Army Corps has kept the channel "
                            "well maintained heading into fall. A few spots are worth watching: "
                            "just south of St. Louis, plus smaller trouble spots near Rosedale and "
                            "Lake Providence. Overall, the river is well prepared for low water.",
                            style=SECTION_SUBTEXT_STYLE,
                        ),
                        html.P(
                            "Barge rates are already running higher than typical for this time of "
                            "year, with forward barge contracts at their highest ever rate for this "
                            "time of year, and grain barge demand is expected to remain moderate to "
                            "high through harvest.",
                            style=SECTION_SUBTEXT_STYLE,
                        ),
                    ]
                ),

                # Plots panel, slides in over the map
                html.Div(
                    id="plots-panel",
                    style=PLOTS_PANEL_CLOSED,
                    children=[
                        html.Div(
                            style={
                                "margin-bottom": "16px", "display": "flex",
                                "align-items": "center", "gap": "10px",
                            },
                            children=[
                                html.Label(
                                    "Compare to another year?",
                                    style={
                                        "font-weight": "bold", "font-size": "13px", "color": "#1b3a5c",
                                        "font-family": "'DM Sans', sans-serif", "white-space": "nowrap",
                                    }
                                ),
                                dcc.Dropdown(
                                    id="compare-year-dropdown",
                                    options=[],
                                    value=None,
                                    placeholder="Select a year",
                                    clearable=True,
                                    style={"font-size": "13px", "width": "110px"},
                                ),
                            ]
                        ),
                        dcc.Graph(id="barge-rate-plot", style={"height": "300px"}, config={"displayModeBar": False}),
                        dcc.Graph(id="barge-rate-nextmonth-plot", style={"height": "300px"}, config={"displayModeBar": False}),
                        dcc.Graph(id="barge-rate-threemonth-plot", style={"height": "300px"}, config={"displayModeBar": False}),
                        dcc.Graph(id="corn-spread-plot", style={"height": "300px"}, config={"displayModeBar": False}),
                        dcc.Graph(id="cornprice-plot", style={"height": "300px"}, config={"displayModeBar": False}),
                        dcc.Graph(id="soyprice-plot", style={"height": "300px"}, config={"displayModeBar": False}),
                        dcc.Graph(id="memphis-stage-plot", style={"height": "300px"}, config={"displayModeBar": False})
                        # Additional plots can be added as more children
                    ]
                )

            ]
        )
        ],
        ),

        # Barge Demand page -- full page (not an overlay), hidden until its nav link is clicked
        html.Div(
            id="demand-page",
            style=DEMAND_PAGE_HIDDEN,
            children=[
                html.H3(
                    "Demand for Grain Barges is impacted by:",
                    style={
                        "margin": "0 0 18px 0",
                        "font-family": "'DM Sans', sans-serif",
                        "font-weight": "700",
                        "letter-spacing": "0.5px", "font-size": "24px", "color": "#1b3a5c",
                    }
                ),

                build_demand_explanation_boxes(),
                build_demand_crop_section(_corn_production_fig, _corn_production_caption, _corn_futures_fig),
                build_demand_crop_section(_soybean_production_fig, _soybean_production_caption, _soybean_futures_fig),

                dcc.Store(id="compare-years-store", data=False),
                html.Button(
                    "See how this year compares to other years",
                    id="compare-years-toggle", n_clicks=0, style=COMPARE_YEARS_TOGGLE_STYLE,
                ),
                html.Div(
                    id="compare-years-box",
                    style=COMPARE_YEARS_BOX_HIDDEN,
                    children=[
                        html.Button(
                            "✕", id="compare-years-close",
                            style={
                                "position": "absolute", "top": "8px", "right": "10px",
                                "zIndex": "10",
                                "border": "none", "background": "none", "cursor": "pointer",
                                "font-size": "18px", "color": "#888",
                            }
                        ),
                        # Graph and its x-axis-title replacement share one relatively-positioned
                        # wrapper so the label can be pinned with `position: absolute` a precise
                        # number of pixels from the plot's bottom edge, instead of a negative
                        # margin -- negative margins on a flex item nested inside the scrollable
                        # compare-years-box (overflow-y: auto) were clipping/whiting-out the top
                        # of the label text rather than cleanly overlapping the plot.
                        html.Div(
                            style={"display": "flex", "justify-content": "center"},
                            children=[
                                html.Div(
                                    style={"position": "relative", "width": "596px"},
                                    children=[
                                        dcc.Graph(
                                            id="compare-years-scatter", figure=_compare_years_fig,
                                            config={"displayModeBar": False, "responsive": False},
                                        ),

                                        # Replaces the (blanked) Plotly x-axis title -- Plotly
                                        # can't put a hoverable "?" icon next to a native axis
                                        # title. Padding mirrors the figure's own l/r margins
                                        # (40px/156px out of 596px) so the icon + label line up
                                        # under the actual plot area, not the whole graph width.
                                        # `bottom` is a positive offset from the graph's bottom
                                        # edge, i.e. it sits inside the figure's blank margin
                                        # rather than below the graph entirely.
                                        html.Div(
                                            style={
                                                "position": "absolute", "bottom": "7px", "left": "0",
                                                "width": "596px", "box-sizing": "border-box",
                                                "padding": "0 156px 0 40px",
                                                "display": "flex", "justify-content": "center",
                                                "align-items": "center", "gap": "5px",
                                            },
                                            children=[
                                                _layer_info_icon(
                                                    None,
                                                    [
                                                        html.Span(
                                                            "Each axis combines corn and soybean "
                                                            "conditions into one value.",
                                                            style={"display": "block", "margin-bottom": "6px"},
                                                        ),
                                                        html.Span([
                                                            html.Span("Price index: ", style={"font-weight": "bold"}),
                                                            "Corn futures and soybean futures are scaled "
                                                            "to similar levels and then averaged.",
                                                        ], style={"display": "block", "margin-bottom": "6px"}),
                                                        html.Span([
                                                            html.Span("Production index: ", style={"font-weight": "bold"}),
                                                            "Corn and soybean production is added together.",
                                                        ], style={"display": "block"}),
                                                    ],
                                                    wide=True,
                                                ),
                                                html.Span(
                                                    "Grain Price Index",
                                                    style={"font-size": "15px", "color": "#444", "font-family": "Arial, sans-serif"}
                                                ),
                                            ]
                                        ),
                                    ]
                                ),
                            ]
                        ),
                        html.Div(
                            style={"display": "flex", "justify-content": "center"},
                            children=[
                                dcc.Graph(
                                    id="compare-years-barge-rate-plot", figure=_barge_rate_placeholder_fig,
                                    config={"displayModeBar": False},
                                ),
                            ]
                        ),
                        # Space for Molly's own write-up on how to read this chart -- left blank on purpose.
                        html.Div(id="compare-years-explanation"),
                    ]
                ),
            ]
        ),

        # About page -- full page, hidden until its nav link is clicked
        html.Div(
            id="about-page",
            style=ABOUT_PAGE_HIDDEN,
            children=[
                html.H3(
                    "About This Dashboard",
                    style={
                        "margin": "0 0 18px 0",
                        "font-family": "'DM Sans', sans-serif",
                        "font-weight": "700",
                        "letter-spacing": "0.5px", "font-size": "24px", "color": "#1b3a5c",
                    }
                ),
                html.Div(
                    style={"max-width": "700px"},
                    children=[
                        html.Div(
                            [
                                html.B(
                                    "Low water on the Mississippi River can disrupt grain "
                                    "transportation, increase barge freight rates, and create "
                                    "uncertainty throughout the agricultural supply chain."
                                ),
                                " This dashboard combines riverbed surveys, dredging activity, river "
                                "stages, barge freight rates, grain prices, and more into a single, "
                                "interactive platform that will help users better understand "
                                "transportation conditions on the Mississippi River system.",
                            ],
                            style={**SECTION_SUBTEXT_STYLE, "margin-bottom": "18px"}
                        ),
                        html.Div(
                            [
                                "The project was developed by ",
                                html.A(
                                    "Molly Alcorn", href="https://www.linkedin.com/in/molly-alcorn-990042227/",
                                    target="_blank", style={"color": "#2166ac"}
                                ),
                                ", a PhD student at the University of North Carolina at Chapel Hill ",
                                html.A(
                                    "Institute for Risk Management and Insurance Innovation",
                                    href="https://irmii.unc.edu/",
                                    target="_blank", style={"color": "#2166ac"}
                                ),
                                " researching how environmental disruptions, particularly low water "
                                "levels, affect inland waterway transportation and commodity markets. "
                                "By combining hydrologic, navigation, and market data, her work seeks "
                                "to better understand and forecast barge freight rates and "
                                "transportation delays, ultimately supporting more informed "
                                "decision making across the grain supply chain.",
                            ],
                            style={**SECTION_SUBTEXT_STYLE, "margin-bottom": "18px"}
                        ),
                        html.Div(
                            "The dashboard was created to make these publicly available datasets "
                            "easier to access, interpret, and explore. It integrates information from "
                            "multiple agencies into one interface while adding visualizations and "
                            "analyses designed to support research and decision making.",
                            style={**SECTION_SUBTEXT_STYLE, "margin-bottom": "18px"}
                        ),
                        html.Div(
                            "Provided for informational purposes only; not an official navigation "
                            "aid. Underlying data comes from public government sources, but its "
                            "processing, analysis, and presentation here are independent and not "
                            "reviewed or endorsed by those agencies.",
                            style={**SECTION_SUBTEXT_STYLE, "margin-bottom": "18px", "font-style": "italic", "color": "#777", "font-size": "13px"}
                        ),
                        html.Div(
                            [
                                "Questions or feedback? ",
                                html.A("malcor@unc.edu", href="mailto:malcor@unc.edu", style={"color": "#2166ac"}),
                            ],
                            style={**SECTION_SUBTEXT_STYLE, "margin-bottom": "24px"}
                        ),
                        html.H4("Data Sources", style=SECTION_HEADER_STYLE),
                        html.Ul(
                            [
                                html.Li([
                                    html.B("U.S. Army Corps of Engineers (USACE) eHydro"), " — riverbed "
                                    "surveys and dredging depth data",
                                ]),
                                html.Li([
                                    html.B("USCG Local Notice to Mariners"), " — shoaling, draft "
                                    "restriction, dredging, and other navigation notices",
                                ]),
                                html.Li([
                                    html.B("NOAA Marine Cadastre AIS data"), " — dredge vessel activity",
                                ]),
                                html.Li([
                                    html.B("USGS National Water Information System"), " and ",
                                    html.B("NOAA National Weather Service"), " — river stage / gage readings",
                                ]),
                                html.Li([
                                    html.B("USDA Agricultural Marketing Service (AMS)"), " — barge "
                                    "freight rates, grain price spreads, and futures prices",
                                ]),
                                html.Li([
                                    html.B("USDA World Agricultural Supply and Demand Estimates (WASDE)"),
                                    " — current-year grain production estimates",
                                ]),
                                html.Li([
                                    html.B("USDA National Agricultural Statistics Service (NASS) QuickStats"),
                                    " — historical grain production",
                                ]),
                            ],
                            style={**SECTION_SUBTEXT_STYLE, "padding-left": "20px", "margin": "0"}
                        ),
                    ]
                ),
            ]
        ),
    ]
)


# --------------------------------------------------
# WELCOME INTRO MODAL
# --------------------------------------------------


@app.callback(
    Output("welcome-box", "style"),
    Output("welcome-backdrop", "style"),
    Input("welcome-close", "n_clicks"),
    prevent_initial_call=True,
)
def close_welcome(n_clicks):
    return WELCOME_BOX_HIDDEN, WELCOME_BACKDROP_HIDDEN


# --------------------------------------------------
# PLOTS / INSIGHTS PANEL TOGGLES
# --------------------------------------------------
# The two tabs share one right-edge stack, and only one panel makes sense
# open at a time -- clicking one opens its panel, hides the other tab (so
# there's nothing floating over the open panel's text), and shows just a
# close option. Closing brings both tabs back. A single callback keyed off
# which tab fired (dash.ctx.triggered_id) keeps that shared state consistent,
# since two independent callbacks can't both write to the sibling tab's style.

@app.callback(
    Output("plots-panel", "style"),
    Output("insights-panel", "style"),
    Output("plots-toggle", "style"),
    Output("insights-toggle", "style"),
    Output("plots-toggle", "children"),
    Output("insights-toggle", "children"),
    Output("active-panel-store", "data"),
    Input("plots-toggle", "n_clicks"),
    Input("insights-toggle", "n_clicks"),
    State("active-panel-store", "data"),
    prevent_initial_call=True
)
def toggle_right_panels(plots_clicks, insights_clicks, active_panel):
    clicked = dash.ctx.triggered_id
    new_active = None if active_panel == clicked else clicked

    plots_panel_style = PLOTS_PANEL_OPEN if new_active == "plots-toggle" else PLOTS_PANEL_CLOSED
    insights_panel_style = INSIGHTS_PANEL_OPEN if new_active == "insights-toggle" else INSIGHTS_PANEL_CLOSED

    if new_active is None:
        plots_tab_style, insights_tab_style = TAB_BASE_STYLE, TAB_BASE_STYLE
        plots_label, insights_label = PLOTS_TOGGLE_LABEL, INSIGHTS_TOGGLE_LABEL
    elif new_active == "plots-toggle":
        plots_tab_style, insights_tab_style = TAB_BASE_STYLE, TAB_HIDDEN_STYLE
        plots_label, insights_label = "✕ Close", INSIGHTS_TOGGLE_LABEL
    else:
        plots_tab_style, insights_tab_style = TAB_HIDDEN_STYLE, TAB_BASE_STYLE
        plots_label, insights_label = PLOTS_TOGGLE_LABEL, "✕ Close"

    return (
        plots_panel_style, insights_panel_style,
        plots_tab_style, insights_tab_style,
        plots_label, insights_label,
        new_active,
    )


# --------------------------------------------------
# BARGE DEMAND PAGE
# --------------------------------------------------

@app.callback(
    Output("river-page", "style"),
    Output("demand-page", "style"),
    Output("about-page", "style"),
    Output("nav-river-conditions", "style"),
    Output("nav-barge-demand", "style"),
    Output("nav-about", "style"),
    Input("nav-river-conditions", "n_clicks"),
    Input("nav-barge-demand", "n_clicks"),
    Input("nav-about", "n_clicks"),
    prevent_initial_call=True
)
def toggle_top_level_page(river_clicks, demand_clicks, about_clicks):
    if dash.ctx.triggered_id == "nav-barge-demand":
        return RIVER_PAGE_HIDDEN, DEMAND_PAGE_VISIBLE, ABOUT_PAGE_HIDDEN, NAV_LINK_INACTIVE, NAV_LINK_ACTIVE, NAV_LINK_INACTIVE
    if dash.ctx.triggered_id == "nav-about":
        return RIVER_PAGE_HIDDEN, DEMAND_PAGE_HIDDEN, ABOUT_PAGE_VISIBLE, NAV_LINK_INACTIVE, NAV_LINK_INACTIVE, NAV_LINK_ACTIVE
    return RIVER_PAGE_VISIBLE, DEMAND_PAGE_HIDDEN, ABOUT_PAGE_HIDDEN, NAV_LINK_ACTIVE, NAV_LINK_INACTIVE, NAV_LINK_INACTIVE


@app.callback(
    Output("compare-years-box", "style"),
    Output("compare-years-store", "data"),
    Input("compare-years-toggle", "n_clicks"),
    Input("compare-years-close", "n_clicks"),
    State("compare-years-store", "data"),
    prevent_initial_call=True
)
def toggle_compare_years_box(open_clicks, close_clicks, is_open):
    new_state = False if dash.ctx.triggered_id == "compare-years-close" else not is_open
    style = COMPARE_YEARS_BOX_VISIBLE if new_state else COMPARE_YEARS_BOX_HIDDEN
    return style, new_state


@app.callback(
    Output("compare-years-barge-rate-plot", "figure"),
    Input("compare-years-scatter", "clickData"),
)
def update_compare_years_barge_rate(click_data):
    if not click_data:
        return _barge_rate_placeholder_fig
    customdata = click_data["points"][0].get("customdata")
    if not customdata:
        return _barge_rate_placeholder_fig
    year = int(customdata[0])
    if year not in barge_rates["year"].values:
        return _barge_rate_placeholder_fig
    return build_barge_rate_year_fig(year)


# --------------------------------------------------
# CALLBACK
# --------------------------------------------------

@app.callback(
    Output("map", "figure"),
    Input("year-slider", "value"),
    Input("layer-toggle", "value"),
    Input("selected-survey-store", "data"),
    Input("selected-shoaling-mile-store", "data"),
)
def update_map(year, layers, selected_survey, selected_shoaling_mile):

    fig = go.Figure()
    df_b = bathy[bathy['year']==year]
    # UM (Upper Mississippi) survey dots, north of Cairo, are only shown for 2026 onward
    if year < 2026:
        df_b = df_b[~df_b["survey_id"].str.startswith("UM")]
    # only show surveys that have a depth-polygon file -- clicking a dot with none does
    # nothing (see handle_survey_click), which reads as broken, so don't plot it at all
    df_b = df_b[df_b["survey_id"].isin(DEPTH_POLY_FILES)]
    # hide the dot for whichever survey is currently showing its polygon overlay, but if it's
    # High risk, keep its marker up (faded) at the problem point so it's not lost under the
    # polygon -- only High needs this since its marker is otherwise invisible (opacity 0) with
    # only the icon_layers overlay representing it, which is also excluded once df_b drops sid.
    # Low/Medium use plain opacity-1 dot markers, so hiding them under the polygon is fine as-is.
    selected_at_risk_row = None
    if selected_survey:
        sid = selected_survey.get("survey_id")
        df_b = df_b[df_b["survey_id"] != sid]
        match = bathy[(bathy["survey_id"] == sid) & (bathy["at_risk_eff"] == "high")]
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

    #  bathym layer - 3 risk bins (at_risk low/medium/high), each its own trace so color/legend
    # are discrete. drawn here (before dredging/shoaling/other) so it sits behind them on the
    # map, but legendrank pushes it below them in the legend regardless of draw order
    if "bathy" in layers:
        risk_masks = {
            "Low Risk": df_b["at_risk_eff"] == "low",
            "Medium Risk": df_b["at_risk_eff"] == "medium",
            "High Risk": df_b["at_risk_eff"] == "high",
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
            is_high_risk = label == "High Risk"
            fig.add_trace(
                go.Scattermap(
                    lon=df_bin["LON"],
                    lat=df_bin["LAT"],
                    mode="markers",
                    marker=dict(size=size, color=color, opacity=0.0 if is_high_risk else 1.0),
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
            if is_high_risk:
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
            if category == "shoaling":
                # river_name/mid_mile ride along so a click can look up the two
                # surveyed mile markers straddling this notice and draw faint
                # reference lines at them (see handle_shoaling_mile_click)
                customdatas.append([category, full_memo, r.get("river_name"), r.get("mid_mile")])
            else:
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
                "symbol": {"icon": "dredge-icon", "iconsize": 2.5},
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
        gage_source_labels = {"usgs": "USGS", "nws": "NOAA/NWS"}
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
            hovertext=[
                f"<b>{gage_name} River Stage</b><br>"
                f"Source: {gage_source_labels.get(info['source'], info['source'].upper())}<br>"
                f"<i>Click for current reading</i>"
                for gage_name, info in RIVER_GAGES.items()
            ],
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

    # faint reference lines at the two surveyed mile markers straddling a clicked
    # shoaling notice, so its position relative to the mile markers is visible
    if selected_shoaling_mile:
        brackets = _mile_brackets(selected_shoaling_mile.get("river_name"), selected_shoaling_mile.get("mile"))
        if brackets:
            lo, hi = brackets
            fig.add_trace(go.Scattermap(
                lon=[lo["LON"], hi["LON"]],
                lat=[lo["LAT"], hi["LAT"]],
                mode="lines",
                line=dict(color="rgba(255,255,255,0.4)", width=2),
                hoverinfo="none",
                showlegend=False,
            ))
            fig.add_trace(go.Scattermap(
                lon=[lo["LON"], hi["LON"]],
                lat=[lo["LAT"], hi["LAT"]],
                mode="markers+text",
                marker=dict(size=7, color="rgba(255,255,255,0.55)"),
                text=[f"MM {lo['MILE']:g}", f"MM {hi['MILE']:g}"],
                textposition="top center",
                textfont=dict(size=11, color="rgba(255,255,255,0.7)"),
                hoverinfo="none",
                showlegend=False,
            ))

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

    # AIS-derived dredge activity, shown alongside the manually logged dredging notices
    # above when the "Dredging" layer is on. Only covers 2021-2024 -- other years show
    # nothing here. Polygon layer is the year's aggregate footprint (hover = totals);
    # point layer is one dot per individual event (hover = vessel/MMSI/dates/duration).
    # Added after the survey depth-polygon overlay above so its outline draws on top
    # instead of being covered when a survey is selected.
    if "dredging" in layers and year in AIS_DREDGE_BY_YEAR:
        ais = AIS_DREDGE_BY_YEAR[year]
        fig.add_trace(go.Scattermap(
            lon=ais["lons"],
            lat=ais["lats"],
            mode="lines",
            line=dict(color="white", width=1.5),
            name="Dredge Activity (AIS)",
            legendrank=1.5,
            hoverinfo="none",
        ))
        fig.add_trace(go.Scattermap(
            lon=ais["point_lons"],
            lat=ais["point_lats"],
            mode="markers",
            marker=dict(size=20, color=CATEGORY_COLORS["dredging"], opacity=0),
            name="Dredge Events (AIS)",
            showlegend=False,
            hoverinfo="text",
            hovertext=ais["point_hovertext"],
        ))
        icon_layers.append({
            "sourcetype": "geojson",
            "source": {
                "type": "FeatureCollection",
                "features": [
                    {"type": "Feature", "geometry": {"type": "Point", "coordinates": [lon, lat]}}
                    for lon, lat in zip(ais["point_lons"], ais["point_lats"])
                ],
            },
            "type": "symbol",
            "symbol": {"icon": "dredge-icon", "iconsize": 2.5},
        })

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
    Output("selected-shoaling-mile-store", "data"),
    Input("map", "clickData"),
    Input("notice-detail-close", "n_clicks"),
    State("selected-shoaling-mile-store", "data"),
    prevent_initial_call=True,
)
def handle_shoaling_mile_click(click_data, n_close, current):
    """Clicking a shoaling notice shows faint reference lines at the two surveyed mile
    markers straddling it (see _mile_brackets); clicking it again, clicking any other
    marker, or closing the notice detail panel clears them."""
    if dash.ctx.triggered_id == "notice-detail-close":
        return None
    if not click_data or not click_data.get("points"):
        return dash.no_update
    customdata = click_data["points"][0].get("customdata")
    if not customdata or customdata[0] != "shoaling" or len(customdata) < 4:
        return None
    river_name, mile = customdata[2], customdata[3]
    if not river_name or pd.isna(mile):
        return None
    if current and current.get("river_name") == river_name and current.get("mile") == mile:
        return None
    return {"river_name": river_name, "mile": mile}


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
        # shoaling carries extra trailing fields (river_name, mid_mile) for the
        # faint-mile-line click behavior -- only the memo text is shown here
        full_memo = fields[0]
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
    Output("survey-zoom-memo", "style"),
    Output("survey-zoom-memo", "children"),
    Input("selected-survey-store", "data"),
)
def render_current_gage(data):
    if not data:
        return CURRENT_GAGE_HIDDEN, [], ZOOM_MEMO_HIDDEN, []
    gage_name = data.get("gage_name", "Memphis")
    latest = river_stage_df[river_stage_df["gage"] == gage_name].sort_values("date")
    if latest.empty:
        return CURRENT_GAGE_HIDDEN, [], ZOOM_MEMO_HIDDEN, []
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
    memo = "If you can't see the depth map, make sure to zoom in completely on the survey point you selected."
    return CURRENT_GAGE_VISIBLE, content, ZOOM_MEMO_VISIBLE, memo


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

    source_label = "USGS" if RIVER_GAGES.get(gage_name, {}).get("source") == "usgs" else "NOAA/NWS"

    content = [
        html.Div(gage_name, style={"font-size": "18px", "font-weight": "bold", "margin-bottom": "4px"}),
        html.Div([
            html.Span("Current stage: ", style={"font-size": "13px", "color": "#444"}),
            html.Span(f"{current_stage:.1f} ft", style={"font-size": "16px", "font-weight": "bold"}),
            html.Span(today_note, style={"font-size": "11px", "color": "#888"}),
        ], style={"margin-bottom": "4px"}),
        html.Div(f"Source: {source_label}", style={"font-size": "11px", "color": "#888", "margin-bottom": "10px"}),
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


@app.callback(
    Output("compare-year-dropdown", "options"),
    Output("compare-year-dropdown", "value"),
    Input("year-slider", "value"),
    State("compare-year-dropdown", "value"),
)
def update_compare_year_options(primary_year, current_compare):
    # can't compare a year to itself -- drop it from the choices, and clear a stale
    # selection if the primary year was just changed to match it
    options = [{"label": str(y), "value": y} for y in years if y != primary_year]
    value = current_compare if current_compare != primary_year else None
    return options, value


# another callback for the barge rate plot
@app.callback(
    Output("barge-rate-plot", "figure"),
    Input("year-slider", "value"),
    Input("compare-year-dropdown", "value"),
)
def update_barge_rate_plot(year, compare_year):
    fig = go.Figure(data=_year_overlay_traces(
        barge_rates, "week_no", "stlrate_per_ton", year, "#d95f0e", "week", "$%{y:.2f}/ton",
        compare_year=compare_year,
    ))
    fig.update_layout(
        title=dict(
            text="St. Louis to New Orleans Spot Barge Rate",
            subtitle=dict(text="Source: U.S. Department of Agriculture Agricultural Marketing Service", font=dict(size=10, color="#999")),
        ),
        xaxis=dict(tickvals=MONTH_WEEK_TICKVALS, ticktext=MONTH_WEEK_TICKTEXT),
        yaxis_title="Barge Rate ($/ton)",
        yaxis=dict(range=[barge_rates['stlrate_per_ton'].min(), barge_rates['stlrate_per_ton'].max()], hoverformat=".2f"),
            height=300,legend=dict(
            x=0.02,y=0.98,xanchor="left",yanchor="top",traceorder="normal",
            font=dict(size=10),
            bgcolor="rgba(255,255,255,0.6)",bordercolor="black",borderwidth=1),
        margin=dict(l=50, r=20, t=55, b=40),
        hovermode="closest"
    )
    return fig


@app.callback(
    Output("barge-rate-nextmonth-plot", "figure"),
    Input("year-slider", "value"),
    Input("compare-year-dropdown", "value"),
)
def update_barge_rate_nextmonth_plot(year, compare_year):
    fig = go.Figure(data=_year_overlay_traces(
        barge_rates_nextmonth, "week_no", "fwd_rate_per_ton", year, "#8c564b", "week", "$%{y:.2f}/ton",
        compare_year=compare_year, month_label_col="contract_month_label",
    ))
    fig.update_layout(
        title=dict(
            text="Forward Barge Rate: 1 Month",
            subtitle=dict(text="Source: U.S. Department of Agriculture Agricultural Marketing Service", font=dict(size=10, color="#999")),
        ),
        xaxis=dict(tickvals=MONTH_WEEK_TICKVALS, ticktext=MONTH_WEEK_TICKTEXT),
        yaxis_title="Barge Rate ($/ton)",
        yaxis=dict(range=[barge_rates_nextmonth['fwd_rate_per_ton'].min(), barge_rates_nextmonth['fwd_rate_per_ton'].max()], hoverformat=".2f"),
            height=300,legend=dict(
            x=0.02,y=0.98,xanchor="left",yanchor="top",traceorder="normal",
            font=dict(size=10),
            bgcolor="rgba(255,255,255,0.6)",bordercolor="black",borderwidth=1),
        margin=dict(l=50, r=20, t=55, b=40),
        hovermode="closest"
    )
    return fig


@app.callback(
    Output("barge-rate-threemonth-plot", "figure"),
    Input("year-slider", "value"),
    Input("compare-year-dropdown", "value"),
)
def update_barge_rate_threemonth_plot(year, compare_year):
    fig = go.Figure(data=_year_overlay_traces(
        barge_rates_threemonth, "week_no", "fwd_rate_per_ton", year, "#c51b7d", "week", "$%{y:.2f}/ton",
        compare_year=compare_year, month_label_col="contract_month_label",
    ))
    fig.update_layout(
        title=dict(
            text="Forward Barge Rate: 3 Months",
            subtitle=dict(text="Source: U.S. Department of Agriculture Agricultural Marketing Service", font=dict(size=10, color="#999")),
        ),
        xaxis=dict(tickvals=MONTH_WEEK_TICKVALS, ticktext=MONTH_WEEK_TICKTEXT),
        yaxis_title="Barge Rate ($/ton)",
        yaxis=dict(range=[barge_rates_threemonth['fwd_rate_per_ton'].min(), barge_rates_threemonth['fwd_rate_per_ton'].max()], hoverformat=".2f"),
            height=300,legend=dict(
            x=0.02,y=0.98,xanchor="left",yanchor="top",traceorder="normal",
            font=dict(size=10),
            bgcolor="rgba(255,255,255,0.6)",bordercolor="black",borderwidth=1),
        margin=dict(l=50, r=20, t=55, b=40),
        hovermode="closest"
    )
    return fig


@app.callback(
    Output("corn-spread-plot", "figure"),
    Input("year-slider", "value"),
    Input("compare-year-dropdown", "value"),
)
def update_corn_spread_plot(year, compare_year):
    fig = go.Figure(data=_year_overlay_traces(
        corn_spread, "week_no", "il_gulf_corn_spread", year, "#1b9e77", "date", "$%{y:.2f}/bu",
        compare_year=compare_year,
    ))
    fig.update_layout(
        title=dict(
            text="Illinois–Gulf Corn Price Spread",
            subtitle=dict(text="Source: U.S. Department of Agriculture Agricultural Marketing Service", font=dict(size=10, color="#999")),
        ),
        xaxis=dict(tickvals=MONTH_WEEK_TICKVALS, ticktext=MONTH_WEEK_TICKTEXT),
        yaxis_title="Spread ($/bushel)",
        yaxis=dict(range=[corn_spread['il_gulf_corn_spread'].min()-0.1, corn_spread['il_gulf_corn_spread'].max()+0.1], hoverformat=".2f"),
            height=300,legend=dict(
            x=0.02,y=0.98,xanchor="left",yanchor="top",traceorder="normal",
            font=dict(size=10),
            bgcolor="rgba(255,255,255,0.6)",bordercolor="black",borderwidth=1),
        margin=dict(l=50, r=20, t=55, b=40),
        hovermode="closest"
    )
    return fig

#now a callback for corn price plot
@app.callback(
    Output("cornprice-plot", "figure"),
    Input("year-slider", "value"),
    Input("compare-year-dropdown", "value"),
)
def update_cornprice_plot(year, compare_year):
    fig = go.Figure(data=_year_overlay_traces(
        corn_price, "week_no", "gulf_corn_price", year, "#006837", "date", "$%{y:.2f}/bu",
        compare_year=compare_year,
    ))
    fig.update_layout(
        title=dict(
            text="Gulf Corn Price",
            subtitle=dict(text="Source: U.S. Department of Agriculture Agricultural Marketing Service", font=dict(size=10, color="#999")),
        ),
        xaxis=dict(tickvals=MONTH_WEEK_TICKVALS, ticktext=MONTH_WEEK_TICKTEXT),
        yaxis_title="Price ($/bushel)",
        yaxis=dict(range=[corn_price['gulf_corn_price'].min()-0.1, corn_price['gulf_corn_price'].max()+0.1], hoverformat=".2f"),
        height=300,legend=dict(
           x=0.02,y=0.98,xanchor="left",yanchor="top",traceorder="normal",
           font=dict(size=10),
           bgcolor="rgba(255,255,255,0.6)",bordercolor="black",borderwidth=1),
        margin=dict(l=50, r=20, t=55, b=40),
        hovermode="closest"
    )
    return fig

@app.callback(
    Output("soyprice-plot", "figure"),
    Input("year-slider", "value"),
    Input("compare-year-dropdown", "value"),
)
def update_soyprice_plot(year, compare_year):
    fig = go.Figure(data=_year_overlay_traces(
        soy_price, "week_no", "gulf_soy_price", year, "#f1a340", "date", "$%{y:.2f}/bu",
        compare_year=compare_year,
    ))
    fig.update_layout(
        title=dict(
            text="Gulf Soybean Price",
            subtitle=dict(text="Source: U.S. Department of Agriculture Agricultural Marketing Service", font=dict(size=10, color="#999")),
        ),
        xaxis=dict(tickvals=MONTH_WEEK_TICKVALS, ticktext=MONTH_WEEK_TICKTEXT),
        yaxis_title="Price ($/bushel)",
        yaxis=dict(range=[soy_price['gulf_soy_price'].min()-0.1, soy_price['gulf_soy_price'].max()+0.1], hoverformat=".2f"),
        height=300,legend=dict(
           x=0.02,y=0.98,xanchor="left",yanchor="top",traceorder="normal",
           font=dict(size=10),
           bgcolor="rgba(255,255,255,0.6)",bordercolor="black",borderwidth=1),
        margin=dict(l=50, r=20, t=55, b=40),
        hovermode="closest"
    )
    return fig


@app.callback(
    Output("memphis-stage-plot", "figure"),
    Input("year-slider", "value"),
    Input("compare-year-dropdown", "value"),
)
def update_memphis_stage_plot(year, compare_year):
    fig = go.Figure(data=_year_overlay_traces(
        memphis_stage, "week_no", "stage", year, "#2166ac", "date", "%{y:.2f} ft",
        compare_year=compare_year,
    ))
    fig.update_layout(
        title=dict(
            text="Memphis River Stage",
            subtitle=dict(text="Source: NOAA National Weather Service", font=dict(size=10, color="#999")),
        ),
        xaxis=dict(tickvals=MONTH_WEEK_TICKVALS, ticktext=MONTH_WEEK_TICKTEXT),
        yaxis_title="Stage (ft)",
        yaxis=dict(range=[memphis_stage['stage'].min()-1, memphis_stage['stage'].max()+1], hoverformat=".2f"),
        height=300,legend=dict(
           x=0.02,y=0.98,xanchor="left",yanchor="top",traceorder="normal",
           font=dict(size=10),
           bgcolor="rgba(255,255,255,0.6)",bordercolor="black",borderwidth=1),
        margin=dict(l=50, r=20, t=55, b=40),
        hovermode="closest"
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

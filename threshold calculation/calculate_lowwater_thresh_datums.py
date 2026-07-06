# -*- coding: utf-8 -*-
"""
Calculate the water-surface elevation at each gage station corresponding to
Memphis stage = -10 ft / St. Louis = -3 ft low-water anchors, then interpolate
to every river mile marker and save datum_info.csv.

Method: linear regression of each gage's daily stage against Memphis daily
stage over the full available record. The predicted stage at the anchor value
is converted to NAVD88 elevation using each gage's Datum88 offset.
"""

from pathlib import Path
import pandas as pd
import numpy as np

DATA_DIR = Path(__file__).resolve().parent
MEMPHIS_TARGET = -10.0  # ft relative to Memphis datum

# --------------------------------------------------------------------------
# READ STAGE DATA
# --------------------------------------------------------------------------
def read_csv_stage(filename, col_name):
    df = (pd.read_csv(DATA_DIR / filename, parse_dates=["time"])
            .rename(columns={"time": "date", "value": col_name})
            .assign(**{col_name: lambda d: pd.to_numeric(d[col_name], errors="coerce")}
            )[["date", col_name]])
    df["date"] = pd.to_datetime(df["date"]).dt.floor("D")
    return df

def read_xlsx_stage(filename, col_name, header_row):
    df = (pd.read_excel(DATA_DIR / filename, header=header_row, parse_dates=["Date / Time"])
            .rename(columns={"Date / Time": "date", "Stage (Ft)": col_name})
            .assign(**{col_name: lambda d: pd.to_numeric(d[col_name], errors="coerce")}
            ).iloc[:-1, :2])
    df["date"] = pd.to_datetime(df["date"]).dt.floor("D")
    return df

def drop_frozen_readings(df, col_name, min_run=5):
    """Drop runs of >= min_run identical consecutive daily readings - a stuck/
    frozen sensor signature (real river stage always drifts at least slightly
    day to day), not a real plateau. Found via caruthersville: a ~50-day span
    stuck at 49.70 ft while Memphis moved normally the whole time."""
    df = df.sort_values("date").reset_index(drop=True)
    same_as_prev = df[col_name] == df[col_name].shift(1)
    run_id = (~same_as_prev).cumsum()
    run_len = df.groupby(run_id)[col_name].transform("size")
    return df[run_len < min_run].reset_index(drop=True)

# Known-bad individual readings, confirmed by hand (physically implausible
# against the reference gage at the same date - e.g. a "flood" spike while the
# reference gage sits at its record low). Not caught by drop_frozen_readings
# since these are isolated spikes/dips, not stuck sensors.
KNOWN_BAD_READINGS = {
    "capegir": ["2025-12-13", "2025-12-14", "2023-06-02", "2023-06-03", "2023-06-04", "2023-06-05"],
    # 3-day spike to ~75-77 ft dropped into an otherwise smooth ~11-13 ft
    # trend (back to normal the very next day) - greenville is the predictor
    # for the whole southern group, so this contaminates arkcity/vicksburg/
    # natchez/brouge's regressions too, not just its own
    "greenville": ["2005-10-29", "2005-10-30", "2005-10-31"],
    # one-day dip to 3.28 ft sandwiched between 29.36 (previous day) and
    # 29.70 (next day) during a smooth multi-day rise
    "brouge": ["2011-12-13"],
    # 4 isolated one-day spikes/dips that revert to nearly the exact
    # bracketing value the very next day - memphis is the predictor for the
    # whole Memphis-anchored group, so this contaminates every gage from
    # wickliffe through rosedale, not just memphis's own regression:
    #   2005-01-01: 24.71, 24.71, 15.60, 24.71  (~9 ft one-day dip)
    #   2005-02-07: 21.50, 21.50, -4.84, 16.22   (~26 ft one-day plunge)
    #   2013-09-27: -3.64, 9.42, -2.49           (~13 ft one-day spike)
    #   2022-10-17: -9.69, 0.00, -10.65          (dropout to exactly 0.00;
    #     confirmed against wickliffe, which shows a totally normal day)
    "memphis": ["2005-01-01", "2005-02-07", "2013-09-27", "2022-10-17"],
    # hickman's "1.00" is a sentinel value MOST of the time (5 of 7
    # occurrences are dramatic one-day spikes/dips reverting the next day),
    # but not always - 2012-09-14 (1.42, 1.00, 0.43) and 2025-11-18 (0.35,
    # 1.00, 1.63) both fit a smooth surrounding trend and are real, so this
    # is date-based rather than a blanket value removal like caruthers/49.70.
    # Also a separate isolated spike to 30.56 ft on 2012-08-13 (0.97, 30.56, 1.51).
    "hickman": ["2000-01-17", "2001-09-25", "2002-09-02", "2005-12-02", "2006-03-29", "2012-08-13"],
}

def drop_known_bad(df, col_name):
    bad_dates = KNOWN_BAD_READINGS.get(col_name)
    if not bad_dates:
        return df
    return df[~df["date"].isin(pd.to_datetime(bad_dates))].reset_index(drop=True)

# Sentinel fallback values a gage reports when it's malfunctioning, confirmed
# by hand - caruthersville intermittently reports exactly 49.70 ft (52 times,
# Aug 27-Dec 10 2008) whenever its sensor drops out, with genuinely good real
# readings on the days in between (a real hydrograph rise-and-fall shows up in
# the Sep 8-Oct 16 stretch) - a run-length filter alone misses this because
# some of the bad stretches are only 1-4 days long. This exact value never
# recurs anywhere else in the record, so filtering on it directly is safe.
KNOWN_BAD_VALUES = {
    "caruthers": [49.70],
    # 4 isolated readings of exactly 0.00 ft (Apr 12, May 5-7 2018) while
    # downstream Baton Rouge shows a completely normal 34-37 ft that same day -
    # a dropout/sentinel value, not a genuine near-zero stage
    "greenville": [0.0],
    # newmadrid has two distinct sentinel values: 8.04 ft (21 occurrences,
    # Jan 2013-Nov 2014, mostly isolated dramatic mismatches against a smooth
    # surrounding trend, e.g. 26.17 -> 8.04 -> 27.36) and 56.04/56.05 ft
    # (4 occurrences, Dec 2013-Mar 2016, each a ~26-30 ft one-day spike above
    # a smooth rising trend, e.g. 25.19 -> 56.04 -> 25.88)
    "newmadrid": [8.04, 56.04, 56.05],
}

def drop_known_bad_values(df, col_name):
    bad_values = KNOWN_BAD_VALUES.get(col_name)
    if not bad_values:
        return df
    return df[~df[col_name].isin(bad_values)].reset_index(drop=True)


stlouis   = read_csv_stage("stlouis_stage.csv",             "stlouis")
chester   = read_csv_stage("chester_stage.csv",             "chester")
capegir   = read_xlsx_stage("capegir_stage.xlsx",           "capegir",   14)
wickliffe = read_xlsx_stage("wickliffe_ky_stage.xlsx",      "wickliffe", 12)
hickman   = read_xlsx_stage("hickman_ky_stage.xlsx",        "hickman",   11)
newmadrid = read_xlsx_stage("newmadrid_mo_stage.xlsx",      "newmadrid", 11)
tipton    = read_xlsx_stage("tiptonville_tn_stage.xlsx",    "tipton",    10)
caruthers = read_xlsx_stage("caruthersville_mo_stage.xlsx", "caruthers", 11)
osceola   = read_xlsx_stage("osceola_ar_stage.xlsx",        "osceola",   11)
memphis   = read_xlsx_stage("memphis_stage.xlsx",           "memphis",   28)
mhoonl    = read_xlsx_stage("mhoon_landing_ms_stage.xlsx",  "mhoonl",    11)
helena    = read_xlsx_stage("helena_ar_stage.xlsx",         "helena",    11)
friarsp   = read_xlsx_stage("friarspoint_ms_stage.xlsx",    "friarsp",   11)
rosedale  = read_xlsx_stage("rosedale_stage.xlsx",          "rosedale",  12)
arkcity   = read_xlsx_stage("arkcity_ar_stage.xlsx",        "arkcity",   11)
greenville= read_xlsx_stage("greenville_stage.xlsx",        "greenville",11)
lakeprov  = read_xlsx_stage("lakeprovidence_stage.xlsx",    "lakeprov",  10)
vicksburg = read_xlsx_stage("vicksburg_stage.xlsx",         "vicksburg", 11)
natchez   = read_xlsx_stage("natchez_stage.xlsx",           "natchez",   11)

# Baton Rouge has a different column layout — read manually
_br = pd.read_excel(DATA_DIR / "batonrouge_stage.xlsx", header=18, parse_dates=["Date / Time"])
_br = _br.rename(columns={"Date / Time": "date"})
_br["brouge"] = pd.to_numeric(_br.iloc[:, 1], errors="coerce")
_br = _br[["date", "brouge"]].iloc[:-1]
_br["date"] = pd.to_datetime(_br["date"]).dt.floor("D")
brouge = _br

# Drop stuck-sensor readings from every gage before any regression touches them -
# Memphis and St. Louis are the predictors for everything else, so contamination
# there would silently distort every other gage's fit too.
stlouis    = drop_frozen_readings(stlouis, "stlouis")
chester    = drop_frozen_readings(chester, "chester")
capegir    = drop_known_bad(drop_frozen_readings(capegir, "capegir"), "capegir")
wickliffe  = drop_frozen_readings(wickliffe, "wickliffe")
hickman    = drop_known_bad(drop_frozen_readings(hickman, "hickman"), "hickman")
newmadrid  = drop_known_bad_values(drop_frozen_readings(newmadrid, "newmadrid"), "newmadrid")
tipton     = drop_frozen_readings(tipton, "tipton")
caruthers  = drop_known_bad_values(drop_frozen_readings(caruthers, "caruthers"), "caruthers")
osceola    = drop_frozen_readings(osceola, "osceola")
memphis    = drop_known_bad(drop_frozen_readings(memphis, "memphis"), "memphis")
mhoonl     = drop_frozen_readings(mhoonl, "mhoonl")
helena     = drop_frozen_readings(helena, "helena")
friarsp    = drop_frozen_readings(friarsp, "friarsp")
rosedale   = drop_frozen_readings(rosedale, "rosedale")
arkcity    = drop_frozen_readings(arkcity, "arkcity")
greenville = drop_known_bad_values(drop_known_bad(drop_frozen_readings(greenville, "greenville"), "greenville"), "greenville")
lakeprov   = drop_frozen_readings(lakeprov, "lakeprov")
vicksburg  = drop_frozen_readings(vicksburg, "vicksburg")
natchez    = drop_frozen_readings(natchez, "natchez")
brouge     = drop_known_bad(drop_frozen_readings(brouge, "brouge"), "brouge")

# Order must match the rows in convert_datums.csv
GAGES = [
    ("stlouis",    stlouis),
    ("chester",    chester),
    ("capegir",    capegir),
    ("wickliffe",  wickliffe),
    ("hickman",    hickman),
    ("newmadrid",  newmadrid),
    ("tipton",     tipton),
    ("caruthers",  caruthers),
    ("osceola",    osceola),
    ("memphis",    memphis),
    ("mhoonl",     mhoonl),
    ("helena",     helena),
    ("friarsp",    friarsp),
    ("rosedale",   rosedale),
    ("arkcity",    arkcity),
    ("greenville", greenville),
    ("lakeprov",   lakeprov),
    ("vicksburg",  vicksburg),
    ("natchez",    natchez),
    ("brouge",     brouge),
]

# Gages above Cairo anchored to St. Louis = 0 ft; everything else to Memphis = -5 ft
UPPER_GAGES = {"stlouis", "chester", "capegir"}
SKIP_GAGES  = {"lakeprov"}  # backwater hysteresis — regression unreliable
STLOUIS_TARGET = -3.0

# Gages south of the Arkansas River confluence (33.76878, -91.11293, ~AHP mile
# 580) anchored to Greenville = 7 ft instead of Memphis - they're much closer
# to Greenville than to Memphis, so regressing against it directly is a
# tighter fit than extrapolating all the way from Memphis (e.g. R² for
# natchez/brouge goes from 0.81/0.75 vs Memphis to 0.95/0.94 vs Greenville).
GREENVILLE_GAGES = {"arkcity", "greenville", "vicksburg", "natchez", "brouge", "batonrouge"}  # convert_datums.csv's CityName is "batonrouge"; GAGES/merged_greenville use the shorter "brouge" - both need to be in this set since it's checked against each
GREENVILLE_TARGET = 7.0
CONFLUENCE_MILE = 580.0
# best travel-time lag (days) found by sweeping lags against Greenville and
# keeping whichever maximizes R² - same method used for the Memphis/St. Louis
# gages in the correlation-diagnostics notebook, applied here directly since
# it's a new group
GREENVILLE_LAG = {"arkcity": -1, "vicksburg": 1, "natchez": 2, "brouge": 4}

# --------------------------------------------------------------------------
# MERGE 1: lower-river gages with Memphis
# --------------------------------------------------------------------------
merged_lower = memphis.copy()
for name, df in GAGES:
    if name in UPPER_GAGES or name in GREENVILLE_GAGES or name in SKIP_GAGES or name == "memphis":
        continue
    merged_lower = merged_lower.merge(df[df.iloc[:, 1].between(-100, 100)].copy(), on="date", how="outer")
merged_lower = merged_lower.dropna(subset=["memphis"])

# --------------------------------------------------------------------------
# MERGE 2: upper-river gages with St. Louis
# --------------------------------------------------------------------------
merged_upper = stlouis[stlouis["stlouis"].between(-100, 100)].copy()
for name, df in [("chester", chester), ("capegir", capegir)]:
    merged_upper = merged_upper.merge(df[df.iloc[:, 1].between(-100, 100)].copy(), on="date", how="outer")
merged_upper = merged_upper.dropna(subset=["stlouis"])

# --------------------------------------------------------------------------
# MERGE 3: gages south of the confluence with Greenville, each lag-shifted first
# --------------------------------------------------------------------------
merged_greenville = {}
for name, df in [("arkcity", arkcity), ("vicksburg", vicksburg), ("natchez", natchez), ("brouge", brouge)]:
    shifted = df[df.iloc[:, 1].between(-100, 100)].copy()
    shifted["date"] = shifted["date"] - pd.Timedelta(days=GREENVILLE_LAG[name])
    m = greenville[greenville["greenville"].between(-100, 100)].merge(shifted, on="date", how="inner")
    merged_greenville[name] = m.dropna(subset=["greenville"])

# --------------------------------------------------------------------------
# REGRESSION
# --------------------------------------------------------------------------
def regress_predict(df, predictor, target, predict_at):
    pair = df[[predictor, target]].dropna()
    pair = pair[pair[predictor].between(-100, 100) & pair[target].between(-100, 100)]
    n = len(pair)
    if n < 30:
        return np.nan, np.nan, n
    slope, intercept = np.polyfit(pair[predictor], pair[target], 1)
    predicted = slope * predict_at + intercept
    ss_res = np.sum((pair[target] - (slope * pair[predictor] + intercept)) ** 2)
    ss_tot = np.sum((pair[target] - pair[target].mean()) ** 2)
    r2 = 1 - ss_res / ss_tot
    return predicted, r2, n

datums = pd.read_csv(DATA_DIR / "convert_datums.csv", header=0)
datums["thresh_el"] = np.nan
datums["r2"] = np.nan
datums["n_obs"] = np.nan

print("--- Upper-river gages (St. Louis = 0 ft anchor) ---")
for i, (name, _) in enumerate(GAGES):
    if name not in UPPER_GAGES:
        continue
    datum88 = datums.loc[i, "Datum88"]
    if name == "stlouis":
        predicted_stage, r2 = STLOUIS_TARGET, 1.0
        n = int(merged_upper["stlouis"].notna().sum())
    else:
        predicted_stage, r2, n = regress_predict(merged_upper, "stlouis", name, STLOUIS_TARGET)
    thresh_el = predicted_stage + datum88
    datums.loc[i, ["thresh_el", "r2", "n_obs"]] = thresh_el, round(r2, 4), n
    print(f"  {name:12s}  n={n:5d}  R²={r2:.4f}  predicted_stage={predicted_stage:6.2f} ft  thresh_el={thresh_el:.2f} ft NAVD88")

print("--- Lower-river gages (Memphis = -10 ft anchor) ---")
for i, (name, _) in enumerate(GAGES):
    if name in UPPER_GAGES or name in GREENVILLE_GAGES or name in SKIP_GAGES:
        continue
    datum88 = datums.loc[i, "Datum88"]
    if name == "memphis":
        predicted_stage, r2 = MEMPHIS_TARGET, 1.0
        n = int(merged_lower["memphis"].notna().sum())
    else:
        predicted_stage, r2, n = regress_predict(merged_lower, "memphis", name, MEMPHIS_TARGET)
    thresh_el = predicted_stage + datum88
    datums.loc[i, ["thresh_el", "r2", "n_obs"]] = thresh_el, round(r2, 4) if not np.isnan(r2) else np.nan, n
    print(f"  {name:12s}  n={n:5d}  R²={r2:.4f}  predicted_stage={predicted_stage:6.2f} ft  thresh_el={thresh_el:.2f} ft NAVD88")

print("--- Gages south of the Arkansas River confluence (Greenville = 7 ft anchor) ---")
for i, (name, _) in enumerate(GAGES):
    if name not in GREENVILLE_GAGES:
        continue
    datum88 = datums.loc[i, "Datum88"]
    if name == "greenville":
        predicted_stage, r2 = GREENVILLE_TARGET, 1.0
        n = int(merged_greenville["arkcity"]["greenville"].notna().sum())  # any of the greenville-merged frames carries the full greenville series
    else:
        predicted_stage, r2, n = regress_predict(merged_greenville[name], "greenville", name, GREENVILLE_TARGET)
    thresh_el = predicted_stage + datum88
    datums.loc[i, ["thresh_el", "r2", "n_obs"]] = thresh_el, round(r2, 4) if not np.isnan(r2) else np.nan, n
    lag_txt = f"{GREENVILLE_LAG[name]:+d}d" if name in GREENVILLE_LAG else "0d"
    print(f"  {name:12s}  n={n:5d}  R²={r2:.4f}  lag={lag_txt}  predicted_stage={predicted_stage:6.2f} ft  thresh_el={thresh_el:.2f} ft NAVD88")

# --------------------------------------------------------------------------
# INTERPOLATE — two pieces, seam at mile 950/951
# --------------------------------------------------------------------------
datumsave = datums[["CityName", "LAT", "LON", "MileMarker", "thresh_el", "r2", "n_obs"]]
datumsave.to_csv(DATA_DIR / "datum_thresholds_memphis_m10.csv", index=False)
print(f"\nSaved per-gage thresholds to {DATA_DIR / 'datum_thresholds_memphis_m10.csv'}")

# Official LWRP shape reference: Memphis District 2007 LWRP (exact, every 0.1
# mile, AHP 963.2-591.3), the Vicksburg District 1993 LWRP (digitized off the
# profile plot in Soar et al. 2007, whole-mile resolution, AHP 591-355,
# calibrated to NAVD88 using the mile 592-630 overlap with the Memphis table -
# see mvk_lwrp_1993_digitized.csv), and the St. Louis District 2014 LWRP
# (digitized off the profile plot in mvs_lwrp_2014.pdf, AHP 963-1148 - see
# mvs_lwrp_2014_digitized.csv). Each anchor keeps its own regressed thresh_el;
# the drop *between* anchors is distributed the way the real river's LWRP
# actually falls off, not linearly by mile.
_memphis_lwrp = pd.read_csv(DATA_DIR / "mvm_lwrp_2007_full.csv").rename(columns={"navd88_ft": "el"}).sort_values("ahp_mile")
_vicksburg_lwrp = pd.read_csv(DATA_DIR / "mvk_lwrp_1993_digitized.csv").rename(columns={"navd88_ft_approx": "el"}).sort_values("ahp_mile")
_stlouis_lwrp = pd.read_csv(DATA_DIR / "mvs_lwrp_2014_digitized.csv").rename(columns={"navd88_ft": "el"}).sort_values("ahp_mile")
_official_lwrp = (
    pd.concat([
        _memphis_lwrp,
        _vicksburg_lwrp[_vicksburg_lwrp["ahp_mile"] < _memphis_lwrp["ahp_mile"].min()],
        _stlouis_lwrp[_stlouis_lwrp["ahp_mile"] > _memphis_lwrp["ahp_mile"].max()],
    ])
    .sort_values("ahp_mile")
)
_OFFICIAL_MIN, _OFFICIAL_MAX = _official_lwrp["ahp_mile"].min(), _official_lwrp["ahp_mile"].max()

def _official_at(mile):
    if not (_OFFICIAL_MIN <= mile <= _OFFICIAL_MAX):
        return None
    return np.interp(mile, _official_lwrp["ahp_mile"], _official_lwrp["el"])

def shape_interp(mile_hi, el_hi, mile_lo, el_lo, miles):
    """thresh_el at each mile between two anchors, proportional to the official
    LWRP's actual elevation drop over that reach. Falls back to linear-by-mile
    wherever the official table doesn't cover both anchors of the reach."""
    off_hi, off_lo = _official_at(mile_hi), _official_at(mile_lo)
    use_shape = off_hi is not None and off_lo is not None and off_hi != off_lo
    out = {}
    for m in miles:
        if use_shape:
            frac = (off_hi - _official_at(m)) / (off_hi - off_lo)
        else:
            frac = (mile_hi - m) / (mile_hi - mile_lo)
        out[m] = el_hi - frac * (el_hi - el_lo)
    return out

def interp_piece(datum_rows, mile_range):
    """Fill the integer-mile grid between gage anchors using shape_interp."""
    anchors = datum_rows.dropna(subset=["thresh_el"]).sort_values("MileMarker")[["MileMarker", "thresh_el"]].astype(float)
    grid = np.arange(*mile_range, dtype=float)
    thresh_el = {}

    anchor_miles = anchors["MileMarker"].to_numpy()
    anchor_els = anchors["thresh_el"].to_numpy()
    for mile, elv in zip(anchor_miles, anchor_els):
        if mile in grid:
            thresh_el[mile] = elv

    for mile_lo, el_lo, mile_hi, el_hi in zip(anchor_miles[:-1], anchor_els[:-1], anchor_miles[1:], anchor_els[1:]):
        seg_miles = grid[(grid > mile_lo) & (grid < mile_hi)]
        if len(seg_miles) == 0:
            continue
        thresh_el.update(shape_interp(mile_hi, el_hi, mile_lo, el_lo, seg_miles))

    merged = pd.DataFrame({"MileMarker": list(thresh_el.keys()), "thresh_el": list(thresh_el.values())})
    return merged.dropna(subset=["thresh_el"])

lower_datums = datums[~datums["CityName"].isin(UPPER_GAGES) & ~datums["CityName"].isin(GREENVILLE_GAGES)]
upper_datums = datums[datums["CityName"].isin(UPPER_GAGES)]
greenville_datums = datums[datums["CityName"].isin(GREENVILLE_GAGES)]

# Lower piece: Rosedale → Wickliffe, miles 581–950 (used to run all the way to
# Baton Rouge before the Greenville plane took over everything south of the
# confluence)
lower_piece = interp_piece(lower_datums, (581, 951))
lower_piece = lower_piece[lower_piece["MileMarker"] <= 950]

# Greenville piece: Baton Rouge → Arkansas City, miles 228–555
greenville_piece = interp_piece(greenville_datums, (228, 555))
arkcity_row = greenville_datums[greenville_datums["CityName"] == "arkcity"].iloc[0]
greenville_piece = greenville_piece[greenville_piece["MileMarker"] <= arkcity_row["MileMarker"]]

# Upper piece: Cape Girardeau → St. Louis, miles 1005–1133
upper_piece = interp_piece(upper_datums, (951, 1134))
capegir_row = upper_datums[upper_datums["CityName"] == "capegir"].iloc[0]
upper_piece = upper_piece[upper_piece["MileMarker"] >= capegir_row["MileMarker"]]

# Gap: Wickliffe → Cape Girardeau, miles 951–1004. This isn't one reach to
# shape-match between two of your anchors - it's two different planes
# (Memphis -10 ft south of Cairo, St. Louis -3 ft north of it) that are
# designed to meet at Cairo (AHP 953), not interpolate through it. So each
# side is extended only as far as Cairo, shifted (not rescaled) by its own
# district's official LWRP shape from its nearest gage:
#   - wickliffe -> Cairo using the Memphis 2007 curve's own shape
#   - capegir -> Cairo using the St. Louis 2014 curve's own shape
# The two don't have to land on exactly the same value at Cairo (they're
# independently regressed), so both are kept and simply spliced at Cairo.
CAIRO_MILE = 953.0
wickliffe_row = lower_datums[lower_datums["CityName"] == "wickliffe"].iloc[0]

def shift_from_anchor(anchor_mile, anchor_el, official_series, miles):
    """thresh_el at each mile = anchor's own value + however much the official
    curve itself rises/falls between the anchor and that mile (a shift, not a
    proportional rescale - there's no second anchor to rescale against)."""
    off_anchor = np.interp(anchor_mile, official_series["ahp_mile"], official_series["el"])
    out = {}
    for m in miles:
        off_m = np.interp(m, official_series["ahp_mile"], official_series["el"])
        out[m] = anchor_el + (off_m - off_anchor)
    return out

memphis_side_grid = np.arange(951, CAIRO_MILE, dtype=float)
memphis_side = shift_from_anchor(wickliffe_row["MileMarker"], wickliffe_row["thresh_el"], _memphis_lwrp, memphis_side_grid)

stlouis_side_grid = np.arange(CAIRO_MILE, 1005, dtype=float)
stlouis_side = shift_from_anchor(capegir_row["MileMarker"], capegir_row["thresh_el"], _stlouis_lwrp, stlouis_side_grid)

gap_piece = pd.DataFrame({
    "MileMarker": list(memphis_side.keys()) + list(stlouis_side.keys()),
    "thresh_el": list(memphis_side.values()) + list(stlouis_side.values()),
})

# Gap: Rosedale → Arkansas City, miles 555–580. Same logic as the Cairo splice
# above, just for the Memphis (-10 ft) / Greenville (7 ft) boundary at the
# Arkansas River confluence (AHP 580, from the confluence's actual lat/lon,
# not either gage's own position - it lands much closer to Rosedale than to
# Arkansas City). Both sides shift from their nearest anchor using the same
# Vicksburg 1993 LWRP curve, since this is an internal split within Vicksburg
# District, not a real district boundary like Cairo.
rosedale_row = lower_datums[lower_datums["CityName"] == "rosedale"].iloc[0]

# north of/at the confluence (Memphis's plane) - rosedale is the nearest anchor
confluence_memphis_grid = np.arange(CONFLUENCE_MILE, 585, dtype=float)
confluence_memphis_side = shift_from_anchor(rosedale_row["MileMarker"], rosedale_row["thresh_el"], _vicksburg_lwrp, confluence_memphis_grid)

# south of the confluence (Greenville's plane) - arkcity is the nearest anchor
confluence_greenville_grid = np.arange(555, CONFLUENCE_MILE, dtype=float)
confluence_greenville_side = shift_from_anchor(arkcity_row["MileMarker"], arkcity_row["thresh_el"], _vicksburg_lwrp, confluence_greenville_grid)

confluence_piece = pd.DataFrame({
    "MileMarker": list(confluence_memphis_side.keys()) + list(confluence_greenville_side.keys()),
    "thresh_el": list(confluence_memphis_side.values()) + list(confluence_greenville_side.values()),
})

thresh_miles = pd.concat([greenville_piece, confluence_piece, lower_piece, gap_piece, upper_piece]).sort_values("MileMarker").drop_duplicates("MileMarker")

# Join with mile marker lat/lon
markers = pd.read_csv(DATA_DIR.parent / "update_bathym" / "usace_river_mile_markers.csv")
markers = markers[
    (markers["RIVER_NAME"] == "MISSISSIPPI-LO") | (markers["RIVER_NAME"] == "MISSISSIPPI-UP")
].copy()
markers["MILE_FULL"] = markers["MILE"].astype(float)
markers.loc[markers["RIVER_NAME"] == "MISSISSIPPI-UP", "MILE_FULL"] += 953
markers = markers[["MILE_FULL", "LAT", "LON"]].rename(columns={"MILE_FULL": "MileMarker"})
markers["MileMarker"] = markers["MileMarker"].astype(float)
# mile 953 is the exact Cairo boundary and appears in both the MISSISSIPPI-LO
# and MISSISSIPPI-UP marker files - keep just one
markers = markers.drop_duplicates("MileMarker")

thresh_miles_m = thresh_miles.merge(markers, on="MileMarker", how="inner")
out_path = DATA_DIR / "datum_info_memphis_m10.csv"
thresh_miles_m.to_csv(out_path, index=False)
print(f"Saved mile-marker thresholds to {out_path}")

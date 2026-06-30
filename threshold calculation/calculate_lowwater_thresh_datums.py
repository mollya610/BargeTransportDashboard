# -*- coding: utf-8 -*-
"""
Calculate the water-surface elevation at each gage station corresponding to
Memphis stage = -5 ft (a standard low-water threshold), then interpolate to
every river mile marker and save datum_info.csv.

Method: linear regression of each gage's daily stage against Memphis daily
stage over the full available record. The predicted stage at Memphis = -5 ft
is converted to NAVD88 elevation using each gage's Datum88 offset.
"""

from pathlib import Path
import pandas as pd
import numpy as np

DATA_DIR = Path(__file__).resolve().parent
MEMPHIS_TARGET = -5.0  # ft relative to Memphis datum

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
STLOUIS_TARGET = 0.0

# --------------------------------------------------------------------------
# MERGE 1: lower-river gages with Memphis
# --------------------------------------------------------------------------
merged_lower = memphis.copy()
for name, df in GAGES:
    if name in UPPER_GAGES or name == "memphis":
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

print("--- Lower-river gages (Memphis = -5 ft anchor) ---")
for i, (name, _) in enumerate(GAGES):
    if name in UPPER_GAGES:
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

# --------------------------------------------------------------------------
# INTERPOLATE — two pieces, seam at mile 950/951
# --------------------------------------------------------------------------
datumsave = datums[["CityName", "LAT", "LON", "MileMarker", "thresh_el", "r2", "n_obs"]]
datumsave.to_csv(DATA_DIR / "datum_thresholds_memphis_m5.csv", index=False)
print(f"\nSaved per-gage thresholds to {DATA_DIR / 'datum_thresholds_memphis_m5.csv'}")

def interp_piece(datum_rows, mile_range):
    """Outer-merge gage anchors with integer mile range, interpolate."""
    anchors = datum_rows.sort_values("MileMarker")[["MileMarker", "thresh_el"]].astype(float)
    grid = pd.DataFrame({"MileMarker": np.arange(*mile_range, dtype=float)})
    merged = pd.merge(anchors, grid, on="MileMarker", how="outer").sort_values("MileMarker")
    merged["thresh_el"] = merged["thresh_el"].interpolate(method="linear")
    return merged.dropna(subset=["thresh_el"])

lower_datums = datums[~datums["CityName"].isin(UPPER_GAGES)]
upper_datums = datums[datums["CityName"].isin(UPPER_GAGES)]

# Lower piece: Baton Rouge → Wickliffe, miles 229–950
lower_piece = interp_piece(lower_datums, (229, 951))
lower_piece = lower_piece[lower_piece["MileMarker"] <= 950]

# Upper piece: Cape Girardeau → St. Louis, miles 951–1133
# Gap (951–1004) filled by extrapolating the Cape Gir → Chester slope southward
capegir_row = upper_datums[upper_datums["CityName"] == "capegir"].iloc[0]
chester_row  = upper_datums[upper_datums["CityName"] == "chester"].iloc[0]
gap_slope = (chester_row["thresh_el"] - capegir_row["thresh_el"]) / (chester_row["MileMarker"] - capegir_row["MileMarker"])

upper_piece = interp_piece(upper_datums, (951, 1134))
gap_mask = upper_piece["MileMarker"] < capegir_row["MileMarker"]
upper_piece.loc[gap_mask, "thresh_el"] = (
    capegir_row["thresh_el"] + gap_slope * (upper_piece.loc[gap_mask, "MileMarker"] - capegir_row["MileMarker"])
)

thresh_miles = pd.concat([lower_piece, upper_piece]).sort_values("MileMarker").drop_duplicates("MileMarker")

# Join with mile marker lat/lon
markers = pd.read_csv(DATA_DIR.parent / "update_bathym" / "usace_river_mile_markers.csv")
markers = markers[
    (markers["RIVER_NAME"] == "MISSISSIPPI-LO") | (markers["RIVER_NAME"] == "MISSISSIPPI-UP")
].copy()
markers["MILE_FULL"] = markers["MILE"].astype(float)
markers.loc[markers["RIVER_NAME"] == "MISSISSIPPI-UP", "MILE_FULL"] += 953
markers = markers[["MILE_FULL", "LAT", "LON"]].rename(columns={"MILE_FULL": "MileMarker"})
markers["MileMarker"] = markers["MileMarker"].astype(float)

thresh_miles_m = thresh_miles.merge(markers, on="MileMarker", how="inner")
out_path = DATA_DIR / "datum_info_memphis_m5.csv"
thresh_miles_m.to_csv(out_path, index=False)
print(f"Saved mile-marker thresholds to {out_path}")

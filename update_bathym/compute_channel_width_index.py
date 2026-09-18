"""
Daily "channel width index": for Aug 1 - Dec 1 of each 2021-2025 season, the average
of the 3 narrowest navigable-width values (8_compute_width_by_stage.py's per-survey
width_by_stage table, looked up at that day's actual gage reading) among every
"active" survey system-wide that day.

ACTIVITY WINDOW
A confirmed survey is active starting on its own survey date, through Dec 31 of that
same calendar year (never carries into the next year) -- unless superseded earlier by
either of the following, whichever happens first:

  - SURVEY OVERLAP: a later same-year survey's convex hull (bathym_fixed.csv's
    `geometry`) overlaps this one's by more than 20% of THIS survey's own hull area.
    Obsolete as of that later survey's date.
  - DREDGING: a dredge_events_2021_2025 polygon intersects this survey's hull at all
    (no minimum overlap, unlike the 20% survey-vs-survey rule). Obsolete as of that
    event's start_date.

    The shapefile stores one dissolved MultiPolygon per year, not one polygon per
    event -- nearby/overlapping events already got unioned together, so a single
    polygon piece can correspond to more than one CSV event. Each piece's trigger
    date is the EARLIEST start_date among the CSV events whose center point falls
    inside it (conservative: flags obsolescence as soon as any contributing event
    began).

A survey with neither trigger stays active through Dec 31 of its year. Whichever
trigger date is earliest (of the two kinds, and among however many later surveys /
dredge events qualify) wins. The trigger day itself already counts as obsolete (active
while date < trigger_date, not <=) -- a clean handoff with no double-counting on the
transition day.

WIDTH LOOKUP
Each active survey's width on a given day comes from its own
data/WidthByStage/{survey_id}_width_by_stage.csv (survey_id's anchor gage from
make_combined_depth_polygons.py's _survey_gage), at the whole-foot stage closest to
that day's actual reading in river_stage_history.csv. A day whose actual stage falls
outside that table's tested range (see 8_compute_width_by_stage.py's STAGE_RANGES) is
clamped to the nearest tested edge rather than dropped, since the table's range is
already deliberately generous (historical min - 3ft to the 75th percentile) -- see
this script's WARN_ON_CLAMP counter in the printed summary for how often that happens.

Requires 8_compute_width_by_stage.py to have already run for a survey before it can
contribute to any day's index -- surveys without a width_by_stage.csv are excluded
(counted in n_missing_width_table, not silently dropped from that count) -- which will
be most of 2021-2025 until that historical backfill is done; until then this mostly
reports n_active=0 days, which is expected, not a bug.

Usage: python update_bathym/compute_channel_width_index.py
Writes channel_width_index.csv to the repo root: date, year, n_active,
n_missing_width_table, width_ft_1/2/3 and survey_id_1/2/3 (the three narrowest, blank
if fewer than 3 available), avg_lowest3_width_ft (blank if fewer than 3).
"""

from datetime import date, timedelta
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely import wkt as shapely_wkt

from make_combined_depth_polygons import _survey_gage

# ── CONFIG ────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent

BATHYM_FIXED_FILE = REPO_ROOT / "bathym_fixed.csv"
STAGE_HISTORY_FILE = REPO_ROOT / "river_stage_history.csv"
DREDGE_SHP_FILE = REPO_ROOT / "dredge_events_2021_2025" / "dredge_events_2021_2025.shp"
DREDGE_CSV_FILE = REPO_ROOT / "dredge_events_2021_2025.csv"
WIDTH_BY_STAGE_DIR = SCRIPT_DIR / "data" / "WidthByStage"

OUT_FILE = REPO_ROOT / "channel_width_index.csv"

YEARS = range(2021, 2026)
SEASON_START_MD = (8, 1)     # Aug 1
SEASON_END_MD = (12, 1)      # Dec 1, inclusive

OVERLAP_FRAC_THRESH = 0.20
N_LOWEST = 3


# ── LOAD SURVEYS ──────────────────────────────────────────────────────────────

def load_surveys():
    bf = pd.read_csv(BATHYM_FIXED_FILE)
    bf["year"] = bf["year"].astype(int)
    bf = bf[(bf["year"] >= min(YEARS)) & (bf["year"] <= max(YEARS)) & (bf["confirmed"] == "yes")].copy()
    bf["date"] = pd.to_datetime(bf["date"], utc=True).dt.tz_localize(None).dt.normalize()
    bf["survey_id"] = (
        bf["file"]
        .str.replace("_SurveyPoint.gpkg", "", regex=False)
        .str.replace("_w_datum.gpkg", "", regex=False)
        .str.replace(".gpkg", "", regex=False)
    )
    bf["hull"] = bf["geometry"].apply(shapely_wkt.loads)
    bf["gage"] = [
        _survey_gage(sid, mile) for sid, mile in zip(bf["survey_id"], bf["milemarker"])
    ]
    return bf.reset_index(drop=True)


# ── DREDGE POLYGON PIECES + TRIGGER DATES ────────────────────────────────────

def load_dredge_pieces():
    if not DREDGE_SHP_FILE.exists() or not DREDGE_CSV_FILE.exists():
        print(f"WARNING: dredge files not found ({DREDGE_SHP_FILE}, {DREDGE_CSV_FILE}), "
              f"skipping dredging obsolescence")
        return gpd.GeoDataFrame({"year": [], "trigger_date": []}, geometry=[])

    poly = gpd.read_file(DREDGE_SHP_FILE).to_crs("EPSG:4326")
    events = pd.read_csv(DREDGE_CSV_FILE)
    events["start_date"] = pd.to_datetime(events["start_date"]).dt.normalize()

    pieces = []
    for _, row in poly.iterrows():
        year = int(row["year"])
        geoms = list(row.geometry.geoms) if row.geometry.geom_type == "MultiPolygon" else [row.geometry]
        year_events = events[events["year"] == year]
        centers = gpd.GeoSeries(
            gpd.points_from_xy(year_events["center_lon"], year_events["center_lat"]), crs="EPSG:4326"
        )
        for piece in geoms:
            inside = centers.within(piece)
            matching_dates = year_events.loc[inside.values, "start_date"]
            if matching_dates.empty:
                continue  # no CSV event center actually falls in this piece -- no date to trigger on
            pieces.append({"year": year, "trigger_date": matching_dates.min(), "geometry": piece})

    return gpd.GeoDataFrame(pieces, geometry="geometry", crs="EPSG:4326") if pieces else \
        gpd.GeoDataFrame({"year": [], "trigger_date": []}, geometry=[])


# ── OBSOLESCENCE RESOLUTION ───────────────────────────────────────────────────

def compute_active_end(surveys, dredge_pieces):
    """Adds an `active_end` column (exclusive upper bound date) to `surveys`:
    the earliest obsolescence trigger date, or Jan 1 of the following year if
    never superseded."""
    active_end = {}
    for year, group in surveys.groupby("year"):
        year_dredge = dredge_pieces[dredge_pieces["year"] == year] if len(dredge_pieces) else dredge_pieces
        for idx, row in group.iterrows():
            triggers = []

            later = group[group["date"] > row["date"]]
            if len(later):
                overlap_area = later["hull"].apply(lambda h: row["hull"].intersection(h).area)
                own_area = row["hull"].area
                if own_area > 0:
                    superseded = later[(overlap_area / own_area) > OVERLAP_FRAC_THRESH]
                    if len(superseded):
                        triggers.append(superseded["date"].min())

            if len(year_dredge):
                hits = year_dredge[year_dredge.geometry.intersects(row["hull"])]
                hits = hits[hits["trigger_date"] >= row["date"]]
                if len(hits):
                    triggers.append(hits["trigger_date"].min())

            year_end_exclusive = pd.Timestamp(year=year + 1, month=1, day=1)
            active_end[idx] = min(triggers) if triggers else year_end_exclusive

    surveys = surveys.copy()
    surveys["active_end"] = pd.Series(active_end)
    return surveys


# ── WIDTH LOOKUP ──────────────────────────────────────────────────────────────

def load_width_tables(survey_ids):
    """{survey_id: (sorted stage_ft array, width_ft array)} for every survey that
    already has a width_by_stage.csv; surveys without one are simply absent."""
    tables = {}
    for sid in survey_ids:
        fpath = WIDTH_BY_STAGE_DIR / f"{sid}_width_by_stage.csv"
        if not fpath.exists():
            continue
        df = pd.read_csv(fpath).sort_values("stage_ft")
        tables[sid] = (df["stage_ft"].to_numpy(), df["width_ft"].to_numpy())
    return tables


def width_at_stage(table, stage_ft):
    stages, widths = table
    clamped = np.clip(stage_ft, stages[0], stages[-1])
    was_clamped = clamped != stage_ft
    idx = int(np.argmin(np.abs(stages - clamped)))
    return float(widths[idx]), was_clamped


# ── MAIN ──────────────────────────────────────────────────────────────────────

def season_days(year):
    d = date(year, *SEASON_START_MD)
    end = date(year, *SEASON_END_MD)
    while d <= end:
        yield d
        d += timedelta(days=1)


def main():
    surveys = load_surveys()
    print(f"{len(surveys)} confirmed 2021-2025 survey(s) in {BATHYM_FIXED_FILE}")

    dredge_pieces = load_dredge_pieces()
    print(f"{len(dredge_pieces)} dredge polygon piece(s) with a resolvable trigger date")

    surveys = compute_active_end(surveys, dredge_pieces)

    width_tables = load_width_tables(surveys["survey_id"])
    n_with_table = surveys["survey_id"].isin(width_tables).sum()
    print(f"{n_with_table} of {len(surveys)} survey(s) have a width_by_stage.csv "
          f"(the rest need 4->7->8 rerun -- see module docstring)")

    stage_hist = pd.read_csv(STAGE_HISTORY_FILE, parse_dates=["date"])
    stage_hist["date"] = stage_hist["date"].dt.normalize()
    stage_lookup = {(r.date, r.gage): r.stage for r in stage_hist.itertuples()}

    rows = []
    n_clamped = 0
    for year in YEARS:
        year_surveys = surveys[surveys["year"] == year]
        for day in season_days(year):
            day_ts = pd.Timestamp(day)
            active = year_surveys[(year_surveys["date"] <= day_ts) & (day_ts < year_surveys["active_end"])]

            widths = []
            n_missing = 0
            for _, s in active.iterrows():
                table = width_tables.get(s["survey_id"])
                if table is None:
                    n_missing += 1
                    continue
                gage_stage = stage_lookup.get((day_ts, s["gage"]))
                if gage_stage is None:
                    n_missing += 1
                    continue
                stage_int = round(gage_stage)
                width_ft, was_clamped = width_at_stage(table, stage_int)
                n_clamped += was_clamped
                widths.append((s["survey_id"], width_ft))

            widths.sort(key=lambda w: w[1])
            lowest = widths[:N_LOWEST]

            row = {
                "date": day.isoformat(),
                "year": year,
                "n_active": len(active),
                "n_missing_width_table": n_missing,
            }
            for i in range(N_LOWEST):
                row[f"survey_id_{i+1}"] = lowest[i][0] if i < len(lowest) else ""
                row[f"width_ft_{i+1}"] = lowest[i][1] if i < len(lowest) else np.nan
            row["avg_lowest3_width_ft"] = (
                round(sum(w for _, w in lowest) / N_LOWEST, 1) if len(lowest) == N_LOWEST else np.nan
            )
            rows.append(row)

    out = pd.DataFrame(rows)
    out.to_csv(OUT_FILE, index=False)
    n_complete = out["avg_lowest3_width_ft"].notna().sum()
    print(f"Wrote {len(out)} day(s) to {OUT_FILE} ({n_complete} with >= {N_LOWEST} active surveys, "
          f"{n_clamped} survey-day lookups clamped to a table's tested stage range)")


if __name__ == "__main__":
    main()

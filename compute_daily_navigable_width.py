"""Build a daily time series of Mississippi River navigable width for each low-water
season (Aug 1 - Nov 30), one value per day, from the confirmed bathymetry surveys'
precomputed stage->width tables (update_bathym/8_compute_width_by_stage.py).

For a given day:
  1. A survey is "active" if it was taken in the same calendar year as the day and on
     or before that day -- i.e. only surveys already collected by that point in the
     season contribute, and a survey never carries over into the next year (each
     year's low-water season is judged only against surveys taken that same year).
  2. A survey drops out of the active set (for that day and every day after) once a
     dredging event lands inside its footprint's convex hull -- see the dredging
     assumptions below.
  3. Each still-active survey's own gage (Memphis or Greenville, by milemarker -- same
     mile-580 split app.py's _gage_info uses) gives that day's real stage reading
     (river_stage_history.csv), rounded to the nearest whole foot, which looks up that
     survey's width at that stage from its width_by_stage.csv (exact-row lookup, same
     convention as app.py's _width_at_stage -- no interpolation).
  4. That day's value is the average of the 5 narrowest active-survey widths (or fewer,
     if fewer than 5 surveys are active that day).

Key assumptions (flagged in case any of these don't match what Molly actually wants):
  - "Convex hull of the survey" is approximated from each survey's own depth-bin
    polygons (9_make_depth_polygons.py's output, unioned then convex-hulled) rather
    than its raw survey points, since raw points are deleted once a survey reaches that
    stage -- the depth polygons are the only footprint that reliably survives for every
    confirmed survey.
  - "Dredging event" means a row in the AIS-derived dredge_events_2021_2025.csv
    (per-event vessel/date/center point), not the notice_to_mariners/ shoaling notices
    (those are reported shoal risk, not confirmed dredging activity).
  - An event's location is its center point (center_lon/center_lat) -- the CSV has no
    per-event footprint, only a center + extent_m, and "within the convex hull" is
    answered directly by a point-in-polygon test against that center point.
  - An event invalidates a survey only if the event's start_date is AFTER the survey's
    own date (dredging before the survey is already reflected in it) -- from that day
    forward the survey stays inactive for the rest of its season.

Outputs (repo root):
  - daily_navigable_width.csv -- one row per (year, date): the final width_ft (average
    of the up-to-5 narrowest active surveys), n_active, n_used, and each gage's stage
    reading that day.
  - daily_navigable_width_detail.csv -- one row per (date, active survey) that had both
    a stage reading and a width lookup that day: its gage, stage_ft, width_ft, and
    whether it was one of the surveys used in that day's average -- the "which
    threshold gage each survey uses" audit trail.
"""
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Point

BASE = Path(__file__).parent
BATHYM_FIXED_FILE = BASE / "bathym_fixed.csv"
WIDTH_BY_STAGE_DIR = BASE / "update_bathym" / "data" / "WidthByStage"
DEPTH_POLY_DIR = BASE / "update_bathym" / "data" / "DepthPolygons"
RIVER_STAGE_FILE = BASE / "river_stage_history.csv"
DREDGE_EVENTS_FILE = BASE / "dredge_events_2021_2025.csv"

OUT_SUMMARY_FILE = BASE / "daily_navigable_width.csv"
OUT_DETAIL_FILE = BASE / "daily_navigable_width_detail.csv"

SEASON_START_MD = (8, 1)
SEASON_END_MD = (11, 30)
N_SMALLEST = 5

# mile 580 is the Arkansas River confluence -- same anchor-gage split app.py's
# _gage_info uses (mile >=951 would anchor to St. Louis instead, but no survey in
# bathym_fixed.csv currently reaches that far north; add that branch here too if one
# ever does).
MEMPHIS_ANCHOR_MILE = 580


def _gage_for_milemarker(milemarker):
    return "Memphis" if milemarker >= MEMPHIS_ANCHOR_MILE else "Greenville"


def load_active_surveys():
    """One row per confirmed, path-confirmed survey: survey_id, date, year, gage."""
    b = pd.read_csv(BATHYM_FIXED_FILE)
    b = b[(b["confirmed"] == "yes") & (b["path_confirmed"] == "yes")].copy()
    b["survey_id"] = b["file"].str.replace("_SurveyPoint.gpkg", "", regex=False)
    b["date"] = pd.to_datetime(b["date"]).dt.tz_localize(None).dt.normalize()
    b["year"] = b["date"].dt.year
    b["gage"] = b["milemarker"].apply(_gage_for_milemarker)
    return b[["survey_id", "date", "year", "milemarker", "gage"]]


def load_width_tables(survey_ids):
    """survey_id -> {stage_ft (int): width_ft} from 8_compute_width_by_stage.py's
    output. Missing tables (survey's raw gpkg was already gone before stage 8 could
    run) are just absent from the dict -- callers treat that survey as never
    contributing a width."""
    tables = {}
    for sid in survey_ids:
        path = WIDTH_BY_STAGE_DIR / f"{sid}_width_by_stage.csv"
        if not path.exists():
            continue
        df = pd.read_csv(path)
        tables[sid] = dict(zip(df["stage_ft"].round().astype(int), df["width_ft"]))
    return tables


def load_survey_hulls(survey_ids):
    """survey_id -> shapely convex-hull polygon (EPSG:4326) of its depth-bin polygons
    (9_make_depth_polygons.py's output) -- see module docstring for why this stands in
    for the survey's own raw-point footprint."""
    hulls = {}
    for sid in survey_ids:
        path = DEPTH_POLY_DIR / f"{sid}_depth_polygons.geojson"
        if not path.exists():
            continue
        gdf = gpd.read_file(path)
        if gdf.empty:
            continue
        hulls[sid] = gdf.union_all().convex_hull
    return hulls


def load_dredge_events():
    df = pd.read_csv(DREDGE_EVENTS_FILE, parse_dates=["start_date", "end_date"])
    df["start_date"] = df["start_date"].dt.tz_localize(None).dt.normalize()
    df["point"] = [Point(lon, lat) for lon, lat in zip(df["center_lon"], df["center_lat"])]
    return df[["start_date", "point"]]


def compute_invalidation_dates(surveys, hulls, dredge_events):
    """survey_id -> the earliest dredge-event start_date that lands inside that
    survey's hull AND falls after the survey's own date, or None if it's never
    invalidated. A survey with no hull (missing depth polygons) is treated as never
    invalidated -- there's nothing to test the event points against."""
    invalidation = {}
    for row in surveys.itertuples():
        hull = hulls.get(row.survey_id)
        if hull is None:
            invalidation[row.survey_id] = None
            continue
        later_events = dredge_events[dredge_events["start_date"] > row.date]
        inside = later_events[later_events["point"].apply(hull.contains)]
        invalidation[row.survey_id] = inside["start_date"].min() if not inside.empty else None
    return invalidation


def load_river_stage():
    df = pd.read_csv(RIVER_STAGE_FILE, parse_dates=["date"])
    df["date"] = df["date"].dt.tz_localize(None).dt.normalize()
    return {(r.gage, r.date): r.stage for r in df.itertuples()}


def season_dates(year):
    start = pd.Timestamp(year, *SEASON_START_MD)
    end = pd.Timestamp(year, *SEASON_END_MD)
    return pd.date_range(start, end, freq="D")


def main():
    surveys = load_active_surveys()
    width_tables = load_width_tables(surveys["survey_id"])
    hulls = load_survey_hulls(surveys["survey_id"])
    dredge_events = load_dredge_events()
    invalidation = compute_invalidation_dates(surveys, hulls, dredge_events)
    stage_lookup = load_river_stage()

    summary_rows = []
    detail_rows = []

    for year, year_surveys in surveys.groupby("year"):
        year = int(year)
        for day in season_dates(year):
            eligible = year_surveys[year_surveys["date"] <= day]
            still_valid = eligible["survey_id"].apply(
                lambda sid: invalidation[sid] is None or day < invalidation[sid]
            )
            active = eligible[still_valid]

            day_results = []
            for row in active.itertuples():
                stage = stage_lookup.get((row.gage, day))
                if stage is None:
                    continue
                table = width_tables.get(row.survey_id)
                if table is None:
                    continue
                width = table.get(round(stage))
                if width is None:
                    continue
                day_results.append((row.survey_id, row.gage, stage, width))

            day_results.sort(key=lambda t: t[3])
            used = day_results[:N_SMALLEST]
            used_ids = {sid for sid, *_ in used}
            day_width = float(np.mean([w for *_, w in used])) if used else np.nan

            summary_rows.append({
                "date": day.date(),
                "year": year,
                "stage_memphis_ft": stage_lookup.get(("Memphis", day)),
                "stage_greenville_ft": stage_lookup.get(("Greenville", day)),
                "n_active": len(active),
                "n_used": len(used),
                "width_ft": day_width,
            })
            for sid, gage, stage, width in day_results:
                detail_rows.append({
                    "date": day.date(), "year": year, "survey_id": sid, "gage": gage,
                    "stage_ft": stage, "width_ft": width, "used_in_avg": sid in used_ids,
                })

    pd.DataFrame(summary_rows).to_csv(OUT_SUMMARY_FILE, index=False)
    pd.DataFrame(detail_rows).to_csv(OUT_DETAIL_FILE, index=False)
    print(f"Saved {len(summary_rows)} days to {OUT_SUMMARY_FILE}")
    print(f"Saved {len(detail_rows)} survey-day rows to {OUT_DETAIL_FILE}")


if __name__ == "__main__":
    main()

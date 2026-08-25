"""
Combine every confirmed survey's depth-bin polygons for a given year into one set of
river-wide polygons (one per coarse display band), so the "River Depth" map layer can
plot ~6 traces instead of one per survey (~1,000+ for a year like 2026).

Where two surveys' areas overlap, the more recent survey wins: older surveys' polygons
are clipped to remove any area a newer survey also covers, before everything left is
unioned per display band. Run any time after make_depth_polygons.py (stage 5) has
produced the individual per-survey files for the year -- always rebuilds from scratch,
so it's safe to re-run whenever new surveys are confirmed/reviewed.

Every survey's depth is also shifted to reflect a target river stage before combining --
each per-survey depth_polygons.geojson holds, at every whole foot of depth, depth
computed relative to a fixed low-water reference plane (LWRP), anchored to one of three
gages depending on river mile (see calculate_lowwater_thresh_datums.py): Greenville=7ft
(mile <580), Memphis=-10ft (580-953), St. Louis=-3ft (>=953) -- the same mile split
calculate_lowwater_thresh_datums.py's Cairo/confluence splices produce. So a survey point
at 9ft under a Memphis-anchored (-10ft) LWRP, on a day when Memphis actually reads 5ft,
is really at 9+(5-(-10)) = 24ft today. The target stage is either today's actual reading
(the default, current-year-only "current conditions" file) or one of the fixed
LOW_WATER_YEARS stages (for a fixed historical scenario applied to the current year's
surveys) -- see combine_year's stage param. Whole-foot source resolution (from
make_depth_polygons.py) is what makes this shift exact rather than an approximation, and
only after shifting does a point get bucketed into the coarse DISPLAY_BINS band actually
drawn on the map. A year with no target stage (a past year, or the current year if
"today" and no stage feed is available) is combined at its native LWRP depth --
0ft offset, still bucketed into DISPLAY_BINS.

Usage: python update_bathym/make_combined_depth_polygons.py [--year YYYY]
"""

import argparse
from datetime import date
from pathlib import Path

import geopandas as gpd
import pandas as pd
import shapely
from shapely import make_valid
from shapely.ops import unary_union

# Coordinates get snapped to this grid right after load, before any difference/union --
# GEOS's overlay noding can throw "non-noded intersection" on inputs that are each
# individually valid but sit within floating-point epsilon of each other (e.g. two
# neighboring surveys' buffered circles touching at ~1e-10m off). 1cm is far finer than
# anything downstream (40m point buffers, 25m simplify) needs, so this has no visible
# effect on the output.
PRECISION_GRID_M = 0.01

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DEPTH_POLY_DIR = SCRIPT_DIR / "data" / "DepthPolygons"
BATHYM_FIXED = REPO_ROOT / "bathym_fixed.csv"
STAGE_HISTORY_FILE = REPO_ROOT / "river_stage_history.csv"

UTM_CRS = "EPSG:26915"  # same projected CRS make_depth_polygons.py buffers/dissolves in

# Same three low-water anchors app.py's GAGE_THRESHOLDS uses for the gage panel -- keep
# these two in sync if the thresholds are ever recalculated
# (threshold calculation/calculate_lowwater_thresh_datums.py).
GAGE_THRESHOLDS = {"St. Louis": -3, "Memphis": -10, "Greenville": 7}
# River-mile boundaries between anchor zones, in the "combined mile" system used
# throughout the bathymetry pipeline (LM mile as-is; UM mile + 953). Mirrors the
# Cairo (953) and Arkansas River confluence (580) splices in
# calculate_lowwater_thresh_datums.py -- south of the confluence is Greenville's plane,
# Cairo to the confluence is Memphis's, north of Cairo is St. Louis's.
CONFLUENCE_MILE = 580
CAIRO_MILE = 953

# Memphis's lowest reading each year in river_stage_history.csv (2022-2025, the most
# recent historic low-water years), and that same day's Greenville/St. Louis readings.
# Each verified against its neighboring days -- a smooth local minimum, not an isolated
# sensor spike/dropout; 2022 excludes the 2022-10-17 "0.00" dropout specifically -- same
# known-bad reading app.py's LOW_WATER_YEARS calc filters out, see
# calculate_lowwater_thresh_datums.py's KNOWN_BAD_READINGS. Fixed historical facts, so
# hardcoded here rather than recomputed from the CSV every run -- keep in sync with
# app.py's LOW_WATER_YEAR_LABELS if these ever need revisiting.
LOW_WATER_YEARS = {
    2022: {"date": "October 20, 2022",  "stage": {"St. Louis": -2.25, "Memphis": -10.74, "Greenville": 5.95}},
    2023: {"date": "October 17, 2023",  "stage": {"St. Louis": 0.50,  "Memphis": -11.97, "Greenville": 5.59}},
    2024: {"date": "November 3, 2024",  "stage": {"St. Louis": 0.71,  "Memphis": -10.31, "Greenville": 5.92}},
    2025: {"date": "October 20, 2025",  "stage": {"St. Louis": -0.57, "Memphis": -8.83,  "Greenville": 8.95}},
}

# Coarse bands actually drawn on the map. Applied here, after a survey's exact
# whole-foot depths (from make_depth_polygons.py) have been shifted to a target stage --
# not in make_depth_polygons.py itself, see module docstring.
DISPLAY_BINS = [
    (20,   None,  "20+ ft"),
    (15,   20,    "15-20 ft"),
    (12,   15,    "12-15 ft"),
    (9,    12,    "9-12 ft"),
    (5,    9,     "5-9 ft"),
    (None, 5,     "<5 ft"),
]
DISPLAY_BIN_ORDER = {label: i for i, (_, _, label) in enumerate(DISPLAY_BINS)}


def assign_display_bin(depth):
    for lo, hi, label in DISPLAY_BINS:
        if lo is not None and depth < lo:
            continue
        if hi is not None and depth >= hi:
            continue
        return label
    return None


def _load_today_stage():
    """Latest available stage per gage (independently -- a gap in one gage's feed
    doesn't block using the other two's most recent readings)."""
    if not STAGE_HISTORY_FILE.exists():
        return {}
    hist = pd.read_csv(STAGE_HISTORY_FILE, parse_dates=["date"])
    latest = hist.sort_values("date").groupby("gage").tail(1)
    return dict(zip(latest["gage"], latest["stage"]))


def _survey_gage(survey_id, mile):
    """Which of the three anchor gages a survey's depth polygons were computed
    relative to, from its combined river mile (see module docstring)."""
    is_lm = survey_id.upper().startswith("LM_")
    combined_mile = mile if is_lm else mile + 953
    if combined_mile < CONFLUENCE_MILE:
        return "Greenville"
    if combined_mile < CAIRO_MILE:
        return "Memphis"
    return "St. Louis"


def _survey_offset(survey_id, mile, today_stage):
    """ft to add to every depth in this survey to reflect today's actual river stage,
    or None if today's stage isn't available for this survey's anchor gage."""
    gage = _survey_gage(survey_id, mile)
    if gage not in today_stage:
        return None
    return today_stage[gage] - GAGE_THRESHOLDS[gage]


def _display_bin(depth, offset):
    """depth is a per-survey file's exact whole-foot value (see make_depth_polygons.py);
    offset is None when no target stage is available for this survey's gage, treated as
    0ft -- native LWRP depth, still bucketed into a display band."""
    return assign_display_bin(depth + (offset or 0))

# This layer is a river-wide overview drawn at zoom levels where a single survey's ~40m
# point buffer is already sub-pixel (see app.py's _add_depth_polygon_traces comment) --
# so it can take a much coarser tolerance than the 5m make_depth_polygons.py uses for
# per-survey detail views. 25m cut the combined file from ~223k coordinate pairs across
# ~17k disjoint islands (most of them tiny buffer-circle leftovers under a few hundred
# m^2) down to a small fraction of that, with no visible difference at any zoom this
# layer is actually used at.
SIMPLIFY_M = 25
# Final per-bin islands smaller than this are dropped as visual clutter -- almost all of
# the ~17k raw islands are isolated leftover buffer circles this small, not meaningful
# river coverage (see MIN_SLIVER_AREA_M2 usage below vs. the near-zero threshold used
# for pure topology repair in _clean_polygonal's other call sites).
MIN_ISLAND_AREA_M2 = 3000


TOPOLOGY_MIN_AREA_M2 = 1  # purely for dropping degenerate make_valid() crumbs, not visual declutter


def _clean_polygonal(geom, min_area=TOPOLOGY_MIN_AREA_M2):
    """Repair a geometry and strip it down to just its Polygon/MultiPolygon parts --
    make_valid() on a self-intersecting shape can return a GeometryCollection mixing in
    stray LineStrings/Points, and simplify() can collapse a thin sliver to <4 coords.
    min_area is in the geometry's own CRS units (m^2 in UTM_CRS, deg^2 after to_crs(4326))."""
    geom = make_valid(geom)
    if geom.geom_type == "GeometryCollection":
        polys = [g for g in geom.geoms if g.geom_type in ("Polygon", "MultiPolygon") and g.area > min_area]
        geom = unary_union(polys) if polys else None
    elif geom.geom_type == "MultiPolygon":
        polys = [g for g in geom.geoms if g.area > min_area]
        geom = unary_union(polys) if polys else None
    return geom


def _survey_id_from_file(file_col):
    return (
        file_col
        .str.replace("_SurveyPoint.gpkg", "", regex=False)
        .str.replace("_w_datum.gpkg", "", regex=False)
        .str.replace(".gpkg", "", regex=False)
    )


def combine_year(year, stage="today", out_suffix=""):
    """stage="today" (default) shifts toward today's actual river stage, but only for
    the current calendar year -- a past year's combined file has no "today" to shift
    toward, so it's combined as-is. stage can also be a literal {gage: ft} dict (e.g.
    LOW_WATER_2022_STAGE) to shift toward a fixed historical stage regardless of year --
    used for the "2022 Low Water" scenario file. out_suffix keeps a non-default scenario
    file from overwriting the main one for the same year."""
    bathy = pd.read_csv(BATHYM_FIXED)
    bathy = bathy[bathy["confirmed"].fillna("yes").str.lower() == "yes"]
    bathy["year"] = bathy["year"].astype(int)
    bathy = bathy[bathy["year"] == year].copy()
    bathy["survey_id"] = _survey_id_from_file(bathy["file"])
    bathy["date"] = pd.to_datetime(bathy["date"])

    surveys = [
        (row["survey_id"], row["date"], DEPTH_POLY_DIR / f"{row['survey_id']}_depth_polygons.geojson")
        for _, row in bathy.iterrows()
    ]
    surveys = [s for s in surveys if s[2].exists()]
    if not surveys:
        print(f"No confirmed {year} surveys with depth-polygon files found -- nothing to combine.")
        return None

    # newest first, so each survey's footprint gets subtracted from every older one
    surveys.sort(key=lambda s: s[1], reverse=True)

    if isinstance(stage, dict):
        today_stage = stage
    elif stage == "today" and year == date.today().year:
        today_stage = _load_today_stage()
    else:
        # a past year has no "today" to shift toward -- stays a static snapshot
        today_stage = {}
    missing_gages = set()

    covered = None  # union of every newer survey's footprint processed so far (UTM_CRS)
    bin_geoms = {}  # depth_bin -> [surviving geometries after newer-survey clipping]

    for survey_id, survey_date, poly_path in surveys:
        gdf = gpd.read_file(poly_path).to_crs(UTM_CRS)
        gdf["geometry"] = gdf.geometry.apply(
            lambda g: shapely.set_precision(make_valid(g), PRECISION_GRID_M)
        )
        footprint = make_valid(unary_union(gdf.geometry.values))
        offset = _survey_offset(survey_id, gdf["mile"].iloc[0], today_stage) if today_stage else None
        if today_stage and offset is None:
            missing_gages.add(_survey_gage(survey_id, gdf["mile"].iloc[0]))
        for _, prow in gdf.iterrows():
            geom = prow.geometry
            if covered is not None:
                geom = shapely.set_precision(make_valid(geom.difference(covered)), PRECISION_GRID_M)
            if geom.is_empty:
                continue
            depth_bin = _display_bin(prow["depth_bin"], offset)
            bin_geoms.setdefault(depth_bin, []).append(geom)
        covered = footprint if covered is None else make_valid(unary_union([covered, footprint]))

    if missing_gages:
        print(f"No current stage reading for {', '.join(sorted(missing_gages))} -- those surveys kept at their static LWRP depth.")
    bin_order = {label: DISPLAY_BIN_ORDER[label] for label in bin_geoms}

    records = []
    for depth_bin, geoms in bin_geoms.items():
        merged = _clean_polygonal(unary_union(geoms))
        if merged is None or merged.is_empty:
            continue
        # visual declutter happens here, after simplify -- simplify can shrink/merge
        # islands further, so filtering small ones first would leave some that end up
        # tiny anyway
        merged = _clean_polygonal(merged.simplify(SIMPLIFY_M), min_area=MIN_ISLAND_AREA_M2)
        if merged is None or merged.is_empty:
            continue
        records.append({"depth_bin": depth_bin, "bin_order": bin_order[depth_bin], "geometry": merged})

    out_gdf = gpd.GeoDataFrame(records, geometry="geometry", crs=UTM_CRS).to_crs(4326)
    # to_crs's meters->degrees reprojection can itself introduce tiny self-intersections/
    # degenerate rings even in geometry that was valid in UTM_CRS -- clean up once more
    # here, with a near-zero (not meters-scaled) area floor since we're in degrees now.
    out_gdf["geometry"] = out_gdf.geometry.apply(lambda g: _clean_polygonal(g, min_area=1e-10))
    out_gdf = out_gdf[out_gdf.geometry.notna() & ~out_gdf.geometry.is_empty]
    out_gdf = out_gdf.sort_values("bin_order").reset_index(drop=True)

    out_path = DEPTH_POLY_DIR / f"{year}_combined_depth_polygons{out_suffix}.geojson"
    if out_path.exists():
        out_path.unlink()
    # the reprojected lon/lat coords default to ~15 decimal digits (sub-millimeter) --
    # 6 decimals (~11cm) is already far finer than these ~40m-buffered polygons need,
    # and roughly halves the file's on-disk/network size for free
    out_gdf.to_file(out_path, driver="GeoJSON", COORDINATE_PRECISION=6)
    stage_note = (
        f", shifted to today's stage ({', '.join(f'{g}={s:g}ft' for g, s in sorted(today_stage.items()))})"
        if today_stage else ""
    )
    print(
        f"Wrote {out_path} -- {len(out_gdf)} depth-bin polygons combined from "
        f"{len(surveys)} surveys ({surveys[-1][0]} oldest .. {surveys[0][0]} newest){stage_note}"
    )
    return out_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=date.today().year)
    args = parser.parse_args()
    combine_year(args.year)
    if args.year == date.today().year:
        # "20XX Low Water" scenario files for app.py's River Depth scenario toggle --
        # only meaningful for the current year's surveys.
        for low_year, info in LOW_WATER_YEARS.items():
            combine_year(args.year, stage=info["stage"], out_suffix=f"_{low_year}lowwater")

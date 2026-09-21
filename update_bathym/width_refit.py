"""
General-purpose fix for any survey over 7_compute_navigable_width.py's
DOWNSAMPLE_THRESHOLD_PTS=150,000 whose blob-pathway width/connectivity came back wrong
(usually a false 0-width chunk / false "not connected") because pathfinding+width
measurement ran against a 20m grid-downsampled point set instead of the real points --
same bug class documented in the LM_08_MVF/LM_15_CVB merge work and confirmed a 3rd
time 2026-08-31 on LM_23_OSCX_20220629_CS_796_791_SORT_LWRP_INTERP (Molly spotted the
bottleneck star sitting on visibly ~19ft-deep water; full-res check found 33 real
points averaging 18.99ft right there vs. 0 points in the downsampled set's 50ft
width-scan buffer at the same spot). Fixed and reconfirmed connected, 919ft through-
width (was 0/not-connected).

This script keeps the EXISTING path geometry (the downsampled hill-climb usually
threads a reasonable route -- it's the WIDTH measurement that's wrong, not necessarily
the path) and re-measures every chunk's width against a fresh, per-chunk
FULL-RESOLUTION local read (bbox pre-filter, same WIDTH_SCAN_MARGIN_M=550m convention
as 7b_review_navigable_path.py's apply_waypoints()) -- never against the whole
point set in one un-indexed .within() scan, which is what hung on LM_08_MVF's
2.37M-point merged blob.

Usage: python3 width_refit.py <survey_id>
  survey_id is the bare id, e.g. LM_23_OSCX_20220629_CS_796_791_SORT_LWRP_INTERP (no
  _SurveyPoint.gpkg suffix). Requires 7_compute_navigable_width.py to have already run
  on it (reads its existing vessel_path_polyline_pending as the path to re-measure).
"""

import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely.wkt
from shapely.geometry import LineString, Point

import width_blob
from importlib import import_module
_stage7 = import_module("OLD_compute_navigable_width")

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
NAVD88_DIR = SCRIPT_DIR / "data" / "NAVD88Files"
OUT_DIR = _stage7.OUT_DIR
PROFILE_FILE = _stage7.PROFILE_FILE
BATHYM_FIXED_FILE = REPO_ROOT / "bathym_fixed.csv"

UTM_CRS = _stage7.UTM_CRS
FT_PER_M = _stage7.FT_PER_M
WIDTH_TARGET_DEPTH_FT = _stage7.WIDTH_TARGET_DEPTH_FT
WIDTH_SCAN_MARGIN_M = 550.0  # 500m calculate_width_at_chunk scan + margin, same as 7b


def main(SURVEY_ID):
    FILE_NAME = f"{SURVEY_ID}_SurveyPoint.gpkg"
    bf = pd.read_csv(BATHYM_FIXED_FILE)
    mask = bf["file"] == FILE_NAME
    if not mask.any():
        raise SystemExit(f"{FILE_NAME} not found in {BATHYM_FIXED_FILE}")
    row = bf.loc[mask].iloc[0]

    path_wkt = row["vessel_path_polyline_pending"]
    if pd.isna(path_wkt) or not str(path_wkt).strip():
        raise SystemExit("No existing vessel_path_polyline_pending to re-measure -- "
                          "run 7_compute_navigable_width.py on this survey first.")
    path_line_wgs84 = shapely.wkt.loads(path_wkt)
    path_line_utm = gpd.GeoSeries([path_line_wgs84], crs="EPSG:4326").to_crs(UTM_CRS).iloc[0]
    path_xy = np.array(path_line_utm.coords)
    print(f"{SURVEY_ID}: existing path has {len(path_xy)} vertices ({len(path_xy) - 1} chunks)")

    print(f"{SURVEY_ID}: loading full-resolution survey points...")
    gdf = gpd.read_file(NAVD88_DIR / FILE_NAME)  # EPSG:3857
    gdf_utm = gdf.to_crs(UTM_CRS)
    xs = gdf_utm.geometry.x.values
    ys = gdf_utm.geometry.y.values
    print(f"  {len(gdf_utm)} points loaded")

    cum_dist_m = np.concatenate([[0.0], np.cumsum(np.linalg.norm(np.diff(path_xy, axis=0), axis=1))])

    rows = []
    for i in range(len(path_xy) - 1):
        point_a = path_xy[i]
        point_b = path_xy[i + 1]
        mid_x = (point_a[0] + point_b[0]) / 2
        mid_y = (point_a[1] + point_b[1]) / 2

        # bbox pre-filter (fast numpy mask) before handing a SMALL local subset to
        # calculate_width_at_chunk -- never run its .within() scan against the full
        # 434,950-point set directly, that's what hung on LM_08_MVF's similarly large
        # merged blob.
        local_mask = (np.abs(xs - mid_x) <= WIDTH_SCAN_MARGIN_M) & (np.abs(ys - mid_y) <= WIDTH_SCAN_MARGIN_M)
        local_gdf = gdf_utm.loc[local_mask]

        w = width_blob.calculate_width_at_chunk(local_gdf, point_a, point_b, depth_threshold=WIDTH_TARGET_DEPTH_FT)

        station_m = (cum_dist_m[i] + cum_dist_m[i + 1]) / 2
        angle = w["perpendicular_angle"]
        dx, dy = np.cos(angle), np.sin(angle)
        width_left_m = w["width_left"] / FT_PER_M
        width_right_m = w["width_right"] / FT_PER_M
        rows.append({
            "station_id": i,
            "station_ft": round(station_m * FT_PER_M, 1),
            "width_ft": round(w["total_width"], 1),
            "geometry": LineString([
                (mid_x - width_left_m * dx, mid_y - width_left_m * dy),
                (mid_x + width_right_m * dx, mid_y + width_right_m * dy),
            ]),
            "lon_lat_source": Point(mid_x, mid_y),
        })
        if (i + 1) % 10 == 0 or i == len(path_xy) - 2:
            print(f"  chunk {i + 1}/{len(path_xy) - 1}: width {w['total_width']:.0f} ft"
                  f"{' (LOCAL SET EMPTY)' if local_gdf.empty else ''}")

    _stage7.flag_width_outliers(rows)

    widths = [r["width_ft"] for r in rows]
    connected = all(w > 0 for w in widths)
    through_width_ft = round(min(widths), 1)
    break_station_ft = None if connected else min(rows, key=lambda r: r["width_ft"])["station_ft"]
    bottleneck_row = min(rows, key=lambda r: r["width_ft"])
    bottleneck_pt_utm = bottleneck_row["lon_lat_source"]
    bottleneck_pt_wgs84 = gpd.GeoSeries([bottleneck_pt_utm], crs=UTM_CRS).to_crs("EPSG:4326").iloc[0]

    print()
    if connected:
        print(f"{SURVEY_ID}: RE-FIT connected, through-width {through_width_ft:.0f} ft "
              f"(was 0 ft / not connected before)")
    else:
        print(f"{SURVEY_ID}: RE-FIT still NOT connected, breaks at station {break_station_ft:.0f} ft")

    # ── write bathym_fixed.csv (pending + live, since path_confirmed is already "yes") ──
    for col in ("vessel_path_connected", "vessel_path_polyline", "vessel_path_method",
                "vessel_path_connected_pending", "vessel_path_polyline_pending", "vessel_path_method_pending"):
        if col in bf.columns and bf[col].dtype != object:
            bf[col] = bf[col].astype(object)

    connected_str = "yes" if connected else "no"
    for suffix in ("", "_pending"):
        bf.loc[mask, f"vessel_path_connected{suffix}"] = connected_str
        bf.loc[mask, f"vessel_path_width_ft{suffix}"] = through_width_ft
        bf.loc[mask, f"vessel_path_break_ft{suffix}"] = break_station_ft
        bf.loc[mask, f"vessel_path_bottleneck_lon{suffix}"] = round(bottleneck_pt_wgs84.x, 6)
        bf.loc[mask, f"vessel_path_bottleneck_lat{suffix}"] = round(bottleneck_pt_wgs84.y, 6)
        # polyline/method unchanged -- same path geometry, only widths were re-measured
    bf.to_csv(BATHYM_FIXED_FILE, index=False)
    print(f"Updated {BATHYM_FIXED_FILE}")

    # ── rewrite NavigableWidth transects.geojson ──
    out_gdf = gpd.GeoDataFrame(rows, geometry="geometry", crs=UTM_CRS)
    lon_lat = gpd.GeoSeries(out_gdf["lon_lat_source"], crs=UTM_CRS).to_crs("EPSG:4326")
    out_gdf["lon"] = lon_lat.x.round(6)
    out_gdf["lat"] = lon_lat.y.round(6)
    out_gdf = out_gdf.drop(columns=["lon_lat_source"])
    out_gdf["survey_id"] = SURVEY_ID
    out_gdf["date"] = str(row["date"])
    out_gdf["method"] = "blob"
    out_path = OUT_DIR / f"{SURVEY_ID}_transects.geojson"
    out_gdf.to_crs("EPSG:4326").to_file(out_path, driver="GeoJSON")
    print(f"Rewrote {out_path}")

    # ── replace this survey's rows in navigable_width_profile.csv ──
    if PROFILE_FILE.exists():
        profile = pd.read_csv(PROFILE_FILE)
        profile = profile[profile["survey_id"] != SURVEY_ID]
    else:
        profile = pd.DataFrame()
    survey_date = str(row["date"])
    year = pd.to_datetime(survey_date, utc=True).year
    new_profile_rows = [{
        "survey_id": SURVEY_ID, "date": survey_date, "year": year,
        "mile": round(float(row["milemarker"]), 1), "method": "blob",
        "station_id": r["station_id"], "station_ft": r["station_ft"],
        "width_ft": r["width_ft"], "flag": r["flag"], "lon": r["lon"], "lat": r["lat"],
    } for r in out_gdf.to_dict("records")]
    profile = pd.concat([profile, pd.DataFrame(new_profile_rows)], ignore_index=True)
    profile.to_csv(PROFILE_FILE, index=False)
    print(f"Replaced {SURVEY_ID}'s rows in {PROFILE_FILE} ({len(new_profile_rows)} new rows)")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: python3 width_refit.py <survey_id>")
    main(sys.argv[1])

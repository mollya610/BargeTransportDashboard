"""Historic Conditions map with individual per-survey dots and click-to-identify
detail (banner + per-survey depth legend + current gage reading), exactly as
app.py's Historic Conditions tab worked before it switched to combined
depth-polygon + narrow-point layers for the whole year. Useful for pinpointing
which survey is which.

Reuses app.py's data loading and several of its still-unchanged callbacks
(notice/gage click detail) directly, rather than duplicating the whole app.
Run on a separate port so both can be open at once:

    python app_survey_map.py          # http://localhost:8051
"""
import os
import textwrap

import dash
from dash import dcc, html, Input, Output, State
import geopandas as gpd
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from shapely import wkt

import app as base

# Per-survey bathymetry data and risk classification -- app.py stopped loading
# bathym_fixed.csv once Current Conditions dropped its vessel_path_width_ft-based
# Navigation Bottleneck marker (that data source is being redone), but this page's whole
# job is showing individual surveys with a risk tier, so it still needs it. Loaded here
# instead of in app.py to keep the dependency scoped to the one page that uses it.
bathy = pd.read_csv("bathym_fixed.csv")
if "confirmed" in bathy.columns:
    bathy = bathy[bathy["confirmed"].fillna("yes").str.lower() == "yes"]
bathy["year"] = bathy["year"].astype(int)

# Risk classification: 6_review_surveys.py's manual at_risk field is retired in favor of
# 7_compute_navigable_width.py's objective vessel_path_connected/width_ft (whether a
# continuous WIDTH_TARGET_DEPTH_FT-deep path exists across the reach, and how wide it
# is). Falls back to the legacy at_risk column for any survey stage 7 hasn't measured
# yet, and to "low" if neither is available.


def _risk_from_vessel_path(row):
    connected = str(row.get("vessel_path_connected", "")).strip().lower()
    if connected == "no":
        return "high"
    if connected != "yes":
        return None  # not yet measured by 7_compute_navigable_width.py
    width_ft = row.get("vessel_path_width_ft")
    if pd.isna(width_ft):
        return None
    if width_ft < base.NAVIGABLE_WIDTH_HIGH_FT:
        return "high"
    if width_ft < base.NAVIGABLE_WIDTH_LOW_FT:
        return "medium"
    return "low"


if "vessel_path_connected" in bathy.columns:
    _vessel_risk = bathy.apply(_risk_from_vessel_path, axis=1)
else:
    _vessel_risk = pd.Series(None, index=bathy.index, dtype=object)
_legacy_risk = bathy["at_risk"] if "at_risk" in bathy.columns else pd.Series(None, index=bathy.index, dtype=object)
bathy["at_risk_eff"] = _vessel_risk.fillna(_legacy_risk).fillna("low")

# get center point for bathym measures
bathy["geometry"] = bathy["geometry"].apply(wkt.loads)
bathy = gpd.GeoDataFrame(bathy, geometry="geometry", crs="EPSG:4326")
bathy["rep_point"] = bathy.geometry.representative_point()
bathy["LON"] = bathy["rep_point"].apply(lambda p: p.x)
bathy["LAT"] = bathy["rep_point"].apply(lambda p: p.y)
bathy = pd.DataFrame(bathy.drop(columns=["geometry", "rep_point"]))

# for at-risk surveys, plot the dot at the actual problem spot within the surveyed area
# instead of the survey's overall center: 7_compute_navigable_width.py's bottleneck point
# (where the navigable path is narrowest or breaks entirely) when available, falling back
# to 6_review_surveys.py's manually marked problem_lon/lat for surveys stage 7 hasn't
# measured yet. Full (low-risk) surveys always show at their overall center.
_bottleneck_lon = pd.to_numeric(bathy["vessel_path_bottleneck_lon"], errors="coerce") if "vessel_path_bottleneck_lon" in bathy.columns else pd.Series(np.nan, index=bathy.index)
_bottleneck_lat = pd.to_numeric(bathy["vessel_path_bottleneck_lat"], errors="coerce") if "vessel_path_bottleneck_lat" in bathy.columns else pd.Series(np.nan, index=bathy.index)
_legacy_lon = pd.to_numeric(bathy["problem_lon"], errors="coerce") if "problem_lon" in bathy.columns else pd.Series(np.nan, index=bathy.index)
_legacy_lat = pd.to_numeric(bathy["problem_lat"], errors="coerce") if "problem_lat" in bathy.columns else pd.Series(np.nan, index=bathy.index)
problem_lon = _bottleneck_lon.fillna(_legacy_lon)
problem_lat = _bottleneck_lat.fillna(_legacy_lat)
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
DEPTH_POLY_FILES = {
    f.stem.replace("_depth_polygons", "")
    for f in base._DEPTH_POLY_DIR.glob("*_depth_polygons.geojson")
} if base._DEPTH_POLY_DIR.exists() else set()

RISK_BINS = [
    ("Low Risk", "#2e7d32", 9),
    ("Medium Risk", "#fb8c00", 12),
    ("High Risk", "#e53935", 16),
]

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
GAGE_FREQ_HIDDEN = {"display": "none"}
GAGE_FREQ_VISIBLE = {
    "position": "absolute", "bottom": "15px", "left": "15px", "right": "330px",
    "zIndex": "24", "background": "rgba(255,255,255,0.97)",
    "border-radius": "8px",
    "box-shadow": "0 2px 10px rgba(0,0,0,0.4)",
    "padding": "10px 40px 6px 14px",
    "font-family": "Arial, sans-serif",
}

# Original "Riverbed Surveys" legend entry (risk-tier dots), ahead of app.py's
# unchanged stage/dredging/shoaling/draft entries.
_ORIGINAL_BATHY_OPTION = {
    "label": html.Span([
        html.Div([
            html.Span("Riverbed Surveys", style={"font-size": "16px"}),
            base._layer_info_icon(
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
}
FULL_LAYER_OPTIONS = [_ORIGINAL_BATHY_OPTION] + base.FULL_LAYER_OPTIONS[1:]

app = dash.Dash(__name__)
app.title = "Riverbed Survey Map"

app.layout = html.Div(
    style={"font-family": "'DM Sans', sans-serif"},
    children=[
        dcc.Store(id="notice-detail-store", data=None),
        dcc.Store(id="selected-gage-store", data=None),
        dcc.Store(id="selected-shoaling-mile-store", data=None),
        dcc.Store(id="selected-survey-store", data=None),
        dcc.Store(id="gage-freq-store", data=None),

        html.Div(
            id="map-container",
            style={"position": "relative", "width": "100%", "height": "100vh"},
            children=[
                dcc.Graph(id="map", style={"height": "100%", "width": "100%"}, config={"displayModeBar": False}),

                # Notice click-detail box, shared by dredging/shoaling/draft/other
                html.Div(
                    id="notice-detail-box",
                    style=base.NOTICE_DETAIL_HIDDEN,
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

                # River stage detail panel -- appears when a gage dot is clicked
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

                # Survey depth banner + legend -- stacked top-right
                html.Div(
                    id="survey-panels-stack",
                    style={
                        "position": "absolute", "top": "15px", "right": "15px", "zIndex": "25",
                        "display": "flex", "flex-direction": "column", "align-items": "flex-end",
                        "gap": "10px",
                    },
                    children=[
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
                        html.Div(
                            id="survey-legend-box",
                            style={"display": "none"},
                            children=[
                                html.Div(id="survey-legend-content"),
                                html.Button("", id="gage-freq-link", n_clicks=0, style={"display": "none"}),
                            ]
                        ),
                        html.Div(
                            id="current-gage-box",
                            style={"display": "none"},
                        ),
                        html.Div(
                            id="survey-zoom-memo",
                            style={"display": "none"},
                        ),
                    ]
                ),

                # Gage-frequency panel -- appears when the survey legend's
                # "how often does the gage reach X ft?" link is clicked
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

                # Controls overlay: year dropdown + layer legend
                html.Div(
                    id="map-controls-stack",
                    style={
                        "position": "absolute", "top": "15px", "left": "15px", "zIndex": "10",
                        "display": "flex", "flex-direction": "column", "align-items": "flex-start",
                        "gap": "10px",
                    },
                    children=[
                        html.Div(
                            id="map-controls",
                            style={
                                "display": "flex", "flex-direction": "column", "gap": "10px",
                                "background": "rgba(255,255,255,0.9)",
                                "padding": "10px 15px", "border-radius": "8px",
                                "box-shadow": "0 1px 4px rgba(0,0,0,0.3)",
                                "font-family": "'DM Sans', sans-serif",
                            },
                            children=[
                                html.Div(
                                    style={"width": "220px"},
                                    children=[
                                        html.Label("Select Year"),
                                        dcc.Dropdown(
                                            id="year-slider",
                                            # base.years excludes the current (in-progress) year --
                                            # this secondary app is for identifying individual
                                            # surveys, so include it too, unlike Historic Conditions
                                            options=[{"label": str(y), "value": y} for y in base.years + [base.thisyear]],
                                            value=base.DEFAULT_HISTORIC_YEAR,
                                            clearable=False,
                                            style={"height": "40px", "font-size": "15px"},
                                        ),
                                    ],
                                ),
                                html.Div(
                                    style={"width": "240px"},
                                    children=[
                                        html.Label("Layers", style={"font-weight": "bold", "margin-bottom": "6px", "display": "block"}),
                                        dcc.Checklist(
                                            id="layer-toggle-full",
                                            options=FULL_LAYER_OPTIONS,
                                            value=["bathy", "stage"],
                                            inputStyle=base.FULL_LAYER_INPUT_STYLE,
                                            labelStyle=base.FULL_LAYER_LABEL_STYLE,
                                        ),
                                    ],
                                ),
                            ],
                        ),
                    ],
                ),
            ],
        ),
    ],
)


@app.callback(
    Output("map", "figure"),
    Input("year-slider", "value"),
    Input("layer-toggle-full", "value"),
    Input("selected-survey-store", "data"),
    Input("selected-shoaling-mile-store", "data"),
)
def update_map(year, layers, selected_survey, selected_shoaling_mile):
    if year is None:
        year = base.DEFAULT_HISTORIC_YEAR
    layers = layers or []

    fig = go.Figure()
    df_b = bathy[bathy["year"] == year]
    # UM (Upper Mississippi) survey dots, north of Cairo, are only shown for 2026 onward
    if year < 2026:
        df_b = df_b[~df_b["survey_id"].str.startswith("UM")]
    # only show surveys that have a depth-polygon file -- clicking a dot with none does
    # nothing (see handle_survey_click), which reads as broken, so don't plot it at all
    df_b = df_b[df_b["survey_id"].isin(DEPTH_POLY_FILES)]
    # hide the dot for whichever survey is currently showing its polygon overlay, but if
    # it's High risk, keep its marker up (faded) at the problem point so it's not lost
    # under the polygon
    selected_at_risk_row = None
    if selected_survey:
        sid = selected_survey.get("survey_id")
        df_b = df_b[df_b["survey_id"] != sid]
        match = bathy[(bathy["survey_id"] == sid) & (bathy["at_risk_eff"] == "high")]
        if not match.empty:
            selected_at_risk_row = match.iloc[0]
    df_n = base.notices[base.notices["year"] == year]

    fig.add_trace(go.Scattermap(
        lon=base.river_lons_arr, lat=base.river_lats_arr, mode="lines",
        line=dict(color="#2166ac", width=2), name="Mississippi River",
        hoverinfo="none", showlegend=False,
    ))
    for display, r_lons, r_lats in base.extra_river_data:
        if r_lons:
            fig.add_trace(go.Scattermap(
                lon=r_lons, lat=r_lats, mode="lines",
                line=dict(color="#2166ac", width=1), name=display,
                hoverinfo="none", showlegend=False,
            ))

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
            start_lonlat = base._nearest_mile_lonlat(row["river_name"], row["mm_low"])
            end_lonlat = base._nearest_mile_lonlat(row["river_name"], row["mm_high"])
            if start_lonlat is None or end_lonlat is None:
                continue
            i0 = base._nearest_river_index(*start_lonlat)
            i1 = base._nearest_river_index(*end_lonlat)
            lo_idx, hi_idx = min(i0, i1), max(i0, i1)
            seg_lons = base.river_lons_arr[lo_idx:hi_idx + 1]
            seg_lats = base.river_lats_arr[lo_idx:hi_idx + 1]
            if len(seg_lons) < 2:
                continue

            is_upcoming = row["is_upcoming"]
            status = "upcoming" if is_upcoming else "active"
            date_start_str = row["date_start"].strftime("%b %d, %Y") if pd.notna(row["date_start"]) else ""
            header = (
                f"Draft Restriction begins {date_start_str}" if is_upcoming
                else "Active Draft Restriction"
            )
            full_memo = row["full_memo"] if pd.notna(row["full_memo"]) else ""
            customdata = [["draft", full_memo]] * len(seg_lons)
            northbound = base._wrap_two_lines(row["northbound"]) if pd.notna(row["northbound"]) else "—"
            southbound = "<br>".join(textwrap.wrap(str(row["southbound"]), width=35)) if pd.notna(row["southbound"]) else "—"
            hovertext = (
                f"<b>{header}</b><br>"
                f"{row['mm_label']}<br>"
                f"Start: {date_start_str or '—'}<br>"
                f"Southbound: {southbound}<br>"
                f"Northbound: {northbound}<br>"
                f"<i>Click for full USCG Memo</i>"
            )
            fig.add_trace(go.Scattermap(
                lon=seg_lons, lat=seg_lats, mode="lines",
                line=dict(color=base.CATEGORY_COLORS["draft"], width=6),
                opacity=base.DRAFT_ANNOUNCED_OPACITY if is_upcoming else base.DRAFT_IN_PLACE_OPACITY,
                legendgroup=f"draft-{status}",
                showlegend=not shown_legend[status],
                name="Draft Restriction" + (" (upcoming)" if is_upcoming else ""),
                hoverinfo="text", hovertext=hovertext, customdata=customdata,
            ))
            shown_legend[status] = True

    icon_layers = []

    if "bathy" in layers:
        risk_masks = {
            "Low Risk": df_b["at_risk_eff"] == "low",
            "Medium Risk": df_b["at_risk_eff"] == "medium",
            "High Risk": df_b["at_risk_eff"] == "high",
        }

        def _gage_info(m):
            if m >= 951:
                return "St. Louis", -3, "St. Louis gage is at -3ft"
            if m >= 580:
                return "Memphis", -10, "Memphis gage is at -10ft"
            return "Greenville", 7, "Greenville gage is at 7ft"

        for label, color, size in RISK_BINS:
            df_bin = df_b[risk_masks[label]].copy()
            if df_bin.empty:
                continue
            df_bin["date_fmt"] = pd.to_datetime(df_bin["date"]).dt.strftime("%B %-d, %Y")
            df_bin["click_hint"] = df_bin["survey_id"].apply(
                lambda sid: "<i>Click for depth map and details</i>" if sid in DEPTH_POLY_FILES else ""
            )
            gage_info = df_bin["milemarker"].apply(_gage_info)
            df_bin["gage_name"] = gage_info.apply(lambda t: t[0])
            df_bin["gage_value"] = gage_info.apply(lambda t: t[1])
            df_bin["gage_label"] = gage_info.apply(lambda t: t[2])
            df_bin["gage_uncertainty"] = df_bin["milemarker"].apply(base._uncertainty_for_mile)
            custom = df_bin[["date_fmt", "depth", "survey_id", "click_hint", "gage_label", "gage_name", "gage_value", "gage_uncertainty"]].copy()
            custom.insert(0, "_type", "bathy")
            is_high_risk = label == "High Risk"
            fig.add_trace(go.Scattermap(
                lon=df_bin["LON"], lat=df_bin["LAT"], mode="markers",
                marker=dict(size=size, color=color, opacity=0.0 if is_high_risk else 1.0),
                showlegend=True, legendgroup="depth_survey",
                legendgrouptitle_text="Survey Locations:<br>Navigation Risk under Low Water",
                legendrank=10, customdata=custom.values, name=label,
                hovertemplate=(
                    "<b><span style='font-size:16px'>Riverbed Survey</span></b><br>"
                    "<span style='font-size:14px'>%{customdata[1]}</span><br>"
                    "%{customdata[4]}<extra></extra>"
                )
            ))
            if is_high_risk:
                icon_layers.append({
                    "sourcetype": "geojson",
                    "source": {
                        "type": "FeatureCollection",
                        "features": [
                            {"type": "Feature", "geometry": {"type": "Point", "coordinates": [row["LON"], row["LAT"]]}}
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

    # dredging/shoaling markers
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
                customdatas.append([category, full_memo, r.get("river_name"), r.get("mid_mile")])
            else:
                customdatas.append([category, full_memo])
            if instructions:
                wrapped = "<br>".join(textwrap.wrap(instructions, width=55))
                lines.append(f"<span style='font-size:11px'>{wrapped}</span>")
            lines.append("<i>Click for full USCG Memo</i>")
            hovertexts.append("<br>".join(lines))

        if category == "dredging":
            fig.add_trace(go.Scattermap(
                lon=df_cat["lon"], lat=df_cat["lat"], mode="markers",
                marker=dict(size=20, color=base.CATEGORY_COLORS[category], opacity=0),
                showlegend=True, legendrank=1, name=base.CATEGORY_LABELS[category],
                hoverinfo="text", hovertext=hovertexts, customdata=customdatas,
            ))
            icon_layers.append({
                "sourcetype": "geojson",
                "source": {
                    "type": "FeatureCollection",
                    "features": [
                        {"type": "Feature", "geometry": {"type": "Point", "coordinates": [row["lon"], row["lat"]]}}
                        for _, row in df_cat.iterrows()
                    ],
                },
                "type": "symbol",
                "symbol": {"icon": "dredge-icon", "iconsize": 2.5},
            })
        else:
            fig.add_trace(go.Scattermap(
                lon=df_cat["lon"], lat=df_cat["lat"], mode="markers",
                marker=dict(size=20, color=base.CATEGORY_COLORS[category], opacity=0),
                showlegend=True, legendrank=2, name=base.CATEGORY_LABELS[category],
                hoverinfo="text", hovertext=hovertexts, customdata=customdatas,
            ))
            icon_layers.append({
                "sourcetype": "geojson",
                "source": {
                    "type": "FeatureCollection",
                    "features": [
                        {"type": "Feature", "geometry": {"type": "Point", "coordinates": [row["lon"], row["lat"]]}}
                        for _, row in df_cat.iterrows()
                    ],
                },
                "type": "symbol",
                "symbol": {"icon": "shoaling-icon", "iconsize": 3},
            })

    df_other = df_n[(df_n["category"] == "other") & (df_n["is_active_flag"])].dropna(subset=["lat", "lon"])
    if not df_other.empty:
        hovertexts = [
            f"<b><span style='font-size:16px'>{base.CATEGORY_ICONS['other']} Navigation Warning</span></b>"
            f"<br>{r['other_notes']}<br><i>Click for details</i>"
            for _, r in df_other.iterrows()
        ]
        customdatas = [["other", r["other_notes"], r["full_memo"]] for _, r in df_other.iterrows()]
        fig.add_trace(go.Scattermap(
            lon=df_other["lon"], lat=df_other["lat"], mode="markers",
            marker=dict(size=13, color=base.CATEGORY_COLORS["other"], opacity=0.85),
            showlegend=True, legendrank=3, name=base.CATEGORY_LABELS["other"],
            hoverinfo="text", hovertext=hovertexts, customdata=customdatas,
        ))

    if "stage" in layers:
        gage_names = list(base.RIVER_GAGES.keys())
        gage_source_labels = {"usgs": "USGS", "nws": "NOAA/NWS"}
        fig.add_trace(go.Scattermap(
            lon=[info["lon"] for info in base.RIVER_GAGES.values()],
            lat=[info["lat"] for info in base.RIVER_GAGES.values()],
            mode="markers+text",
            marker=dict(size=18, color="#1565c0", opacity=0),
            text=gage_names, textposition="top right", textfont=dict(size=13, color="white"),
            showlegend=False,
            customdata=[["gage", gage_name] for gage_name in gage_names],
            hovertext=[
                f"<b>{gage_name} River Stage</b><br>"
                f"Source: {gage_source_labels.get(info['source'], info['source'].upper())}<br>"
                f"<i>Click for current reading</i>"
                for gage_name, info in base.RIVER_GAGES.items()
            ],
            hoverinfo="text", name="River Stage Gages",
        ))
        icon_layers.append({
            "sourcetype": "geojson",
            "source": {
                "type": "FeatureCollection",
                "features": [
                    {"type": "Feature", "geometry": {"type": "Point", "coordinates": [info["lon"], info["lat"]]}}
                    for info in base.RIVER_GAGES.values()
                ],
            },
            "type": "symbol",
            "symbol": {"icon": "gage-icon", "iconsize": 3},
        })

    if selected_shoaling_mile:
        brackets = base._mile_brackets(selected_shoaling_mile.get("river_name"), selected_shoaling_mile.get("mile"))
        if brackets:
            lo, hi = brackets
            fig.add_trace(go.Scattermap(
                lon=[lo["LON"], hi["LON"]], lat=[lo["LAT"], hi["LAT"]], mode="lines",
                line=dict(color="rgba(255,255,255,0.4)", width=2),
                hoverinfo="none", showlegend=False,
            ))
            fig.add_trace(go.Scattermap(
                lon=[lo["LON"], hi["LON"]], lat=[lo["LAT"], hi["LAT"]], mode="markers+text",
                marker=dict(size=7, color="rgba(255,255,255,0.55)"),
                text=[f"MM {lo['MILE']:g}", f"MM {hi['MILE']:g}"],
                textposition="top center", textfont=dict(size=11, color="rgba(255,255,255,0.7)"),
                hoverinfo="none", showlegend=False,
            ))

    # depth polygon overlay for clicked survey
    if selected_survey:
        sid = selected_survey.get("survey_id", "")
        poly_path = base._DEPTH_POLY_DIR / f"{sid}_depth_polygons.geojson"
        if poly_path.exists():
            base._add_depth_polygon_traces(fig, poly_path)

    if "dredging" in layers and year in base.AIS_DREDGE_BY_YEAR:
        ais = base.AIS_DREDGE_BY_YEAR[year]
        fig.add_trace(go.Scattermap(
            lon=ais["lons"], lat=ais["lats"], mode="lines",
            line=dict(color="white", width=1.5), name="Dredge Activity (AIS)",
            legendrank=1.5, hoverinfo="none",
        ))
        fig.add_trace(go.Scattermap(
            lon=ais["point_lons"], lat=ais["point_lats"], mode="markers",
            marker=dict(size=20, color=base.CATEGORY_COLORS["dredging"], opacity=0),
            name="Dredge Events (AIS)", showlegend=False,
            hoverinfo="text", hovertext=ais["point_hovertext"],
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

    fig.update_layout(
        map=dict(
            style="carto-darkmatter",
            zoom=base.DEFAULT_MAP_ZOOM,
            center=base.DEFAULT_MAP_CENTER,
            layers=icon_layers,
        ),
        margin=dict(l=0, r=0, t=0, b=0),
        uirevision="keep-map",
        showlegend=False,
    )
    return fig


# Notice/gage click detail is unchanged from app.py -- reuse those callback
# functions directly instead of duplicating them.
app.callback(
    Output("notice-detail-store", "data"),
    Input("map", "clickData"),
    Input("notice-detail-close", "n_clicks"),
    prevent_initial_call=True,
)(base.handle_notice_click)

app.callback(
    Output("selected-shoaling-mile-store", "data"),
    Input("map", "clickData"),
    Input("notice-detail-close", "n_clicks"),
    State("selected-shoaling-mile-store", "data"),
    prevent_initial_call=True,
)(base.handle_shoaling_mile_click)

app.callback(
    Output("notice-detail-box", "style"),
    Output("notice-detail-content", "children"),
    Input("notice-detail-store", "data"),
)(base.render_notice_detail)

app.callback(
    Output("selected-gage-store", "data"),
    Input("map", "clickData"),
    Input("gage-detail-close", "n_clicks"),
    State("selected-gage-store", "data"),
    prevent_initial_call=True,
)(base.handle_gage_click)

app.callback(
    Output("gage-detail-box", "style"),
    Output("gage-detail-content", "children"),
    Output("gage-stage-plot", "figure"),
    Input("selected-gage-store", "data"),
)(base.render_gage_panel)


# --------------------------------------------------
# SURVEY DEPTH POLYGON CLICK (retired from app.py's Historic Conditions tab;
# kept here so individual surveys can still be identified)
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
    latest = base.river_stage_df[base.river_stage_df["gage"] == gage_name].sort_values("date")
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
        for bin_label, color in base.DEPTH_POLY_COLORS.items()
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
    df = base.river_stage_df[
        (base.river_stage_df["gage"] == gage_name) & (base.river_stage_df["date"] >= cutoff)
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


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 8051)),
        debug=False,
    )

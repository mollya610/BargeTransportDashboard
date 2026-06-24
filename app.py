import os
import glob
import textwrap
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

# get navigation notices (dredging/shoaling/draft restriction/other) from the manually
# maintained notices_<year>.xlsx workbook(s) and locate them via mile markers or, for
# dredging at a named location instead of a mile marker, a manually entered lat/lon
CATEGORY_LABELS = {"dredging": "Dredging", "shoaling": "Shoaling", "draft": "Draft Restriction"}
CATEGORY_COLORS = {"dredging": "#1b9e77", "shoaling": "#756bb1", "draft": "#8c510a"}
DRAFT_UPCOMING_OPACITY = 0.25
DRAFT_ACTIVE_OPACITY = 0.55

# the workbook uses short river codes rather than the full names in the mile-marker table
RIVER_CODE_MAP = {"LMR": "MISSISSIPPI-LO", "UMR": "MISSISSIPPI-UP", "ARK": "ARKANSAS"}

mile_lookup = (
    pd.read_csv("update_bathym/usace_river_mile_markers.csv")
    .groupby(["RIVER_NAME", "MILE"])[["LON", "LAT"]].mean()
)
# ordered points per river, used to draw draft-restriction notices as a shaded line along
# every mile marker they cover instead of a single dot
river_mile_points = mile_lookup.reset_index().sort_values(["RIVER_NAME", "MILE"])

notices = pd.concat(
    [pd.read_excel(f) for f in sorted(glob.glob("notices_*.xlsx"))],
    ignore_index=True
)
notices["date_published"] = pd.to_datetime(notices["date_published"])
notices["date_start"] = pd.to_datetime(notices["date_start"])
notices["date_end"] = pd.to_datetime(notices["date_end"])
notices["year"] = notices["date_published"].dt.year
notices["date_str"] = notices["date_published"].dt.strftime("%Y-%m-%d")
notices["is_active_flag"] = notices["active"].fillna("N").str.upper().eq("Y")
notices["river_name"] = notices["river"].map(RIVER_CODE_MAP)


def _format_mm(row):
    lo, hi = row["mm_low"], row["mm_high"]
    if pd.isna(lo) and pd.isna(hi):
        return None
    if pd.isna(hi) or lo == hi:
        return f"MM {lo:g}"
    return f"MM {lo:g}–{hi:g}"


notices["mm_label"] = notices.apply(_format_mm, axis=1)
notices["mid_mile"] = notices[["mm_low", "mm_high"]].mean(axis=1)

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


# now get river line 
rivers = gpd.read_file('rivers_shapefile/rivers.shp')
rivers = rivers.set_crs('EPSG:4326')
mississippi = rivers[rivers['PNAME'] == 'MISSISSIPPI R']
river_line = mississippi.union_all()
river_line = linemerge(river_line)
lons = []
lats = []
x, y = river_line.xy
lons = list(x)
lats = list(y)

# --------------------------------------------------
# DASH APP
# --------------------------------------------------

app = dash.Dash(__name__)
app.title = "Mississippi River Bathymetry & Dredging"

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

# Style constants for the draft-restriction click detail box, floating over the map
# (sits below the top-right warning icon so the two never overlap)
DRAFT_DETAIL_HIDDEN = {"display": "none"}
DRAFT_DETAIL_VISIBLE = {
    "position": "absolute", "top": "65px", "right": "15px", "zIndex": "25",
    "width": "320px", "background": "rgba(255,255,255,0.97)",
    "padding": "16px 18px", "border-radius": "8px",
    "box-shadow": "0 2px 10px rgba(0,0,0,0.4)",
    "font-family": "Arial, sans-serif",
}

# Style constants for the "other" warning icon, top-right corner of the map
WARNING_ICON_HIDDEN = {"display": "none"}
WARNING_ICON_VISIBLE = {
    "position": "absolute", "top": "15px", "right": "15px", "zIndex": "12",
    "font-size": "30px", "cursor": "default",
    "filter": "drop-shadow(0 1px 3px rgba(0,0,0,0.5))",
}

app.layout = html.Div(
    style={"width": "100%", "margin": "0", "padding": "0"},
    children=[

        # Title bar
        html.Div(
            style={
                "width": "100%",
                "padding": "15px 0",
                "background": "#1b3a5c",
                "text-align": "center",
            },
            children=[
                html.H2(
                    "Mississippi River Bathymetry & Dredging",
                    style={"margin": 0, "color": "white"}
                )
            ]
        ),

        dcc.Store(id="plots-panel-store", data=False),
        dcc.Store(id="draft-detail-store", data=None),

        ##################################
        # Map fills the full width; controls and plots panel float on top of it
        html.Div(
            style={"position": "relative", "width": "100%", "height": "92vh"},
            children=[

                # Map, edge to edge
                dcc.Graph(id="map", style={"height": "100%", "width": "100%"}),

                # Warning icon for active "other" notices (e.g. tropical storms), top-right
                html.Div(
                    "⚠️",
                    id="other-warning-icon",
                    title="",
                    style=WARNING_ICON_HIDDEN
                ),

                # Draft-restriction click detail box, top-right (below the warning icon)
                html.Div(
                    id="draft-detail-box",
                    style=DRAFT_DETAIL_HIDDEN,
                    children=[
                        html.Button(
                            "✕", id="draft-detail-close",
                            style={
                                "position": "absolute", "top": "8px", "right": "10px",
                                "border": "none", "background": "none", "cursor": "pointer",
                                "font-size": "16px", "color": "#888",
                            }
                        ),
                        html.Div(id="draft-detail-content")
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

                        # Layers dropdown (multi-select)
                        html.Div(
                            style={"width": "220px"},
                            children=[
                                html.Label("Layers"),
                                dcc.Dropdown(
                                    id="layer-toggle",
                                    options=[
                                        {"label": "Bathymetry", "value": "bathy"},
                                        {"label": "Dredging", "value": "dredging"},
                                        {"label": "Shoaling", "value": "shoaling"},
                                        {"label": "Draft Restriction", "value": "draft"},
                                    ],
                                    value=["bathy", "dredging", "shoaling", "draft"],
                                    multi=True,
                                    clearable=False,
                                    style={"font-size": "14px"}
                                )
                            ]
                        )
                    ]
                ),

                # Colorbar overlay, grouped with the map's legend in the bottom-left corner
                html.Div(
                    style={
                        "position": "absolute",
                        "bottom": "50px",
                        "left": "10px",
                        "zIndex": "10",
                        "width": "160px",
                        "background": "rgba(255,255,255,0.8)",
                        "padding": "4px 10px 6px 6px",
                        "border-radius": "0",
                        "box-shadow": "0 1px 4px rgba(0,0,0,0.3)"
                    },
                    children=[
                        html.Div(
                            "Depth (ft)",
                            style={
                                "font-family": "Arial, sans-serif",
                                "font-size": "12px",
                                "color": "#444",
                                "margin-bottom": "2px"
                            }
                        ),
                        dcc.Graph(
                            id="colorbar",
                            figure={
                                "data": [
                                    go.Scatter(
                                        x=[None],
                                        y=[None],
                                        mode='markers',
                                        marker=dict(
                                            colorscale="YlOrRd",
                                            cmin=0,
                                            cmax=40,
                                            colorbar=dict(
                                                tickfont=dict(size=12, family="Arial, sans-serif"),
                                                orientation="h",
                                                thickness=8,
                                                len=0.9,
                                                y=1, yanchor="top",
                                            ),
                                            size=0
                                        ),
                                        showlegend=False
                                    )
                                ],
                                "layout": go.Layout(
                                    margin=dict(l=0, r=0, t=0, b=0),
                                    height=35,
                                    paper_bgcolor="rgba(0,0,0,0)",
                                    plot_bgcolor="rgba(0,0,0,0)",
                                )
                            },
                            config={"displayModeBar": False},
                            style={"height": "35px"}
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
    Input("layer-toggle", "value")
)
def update_map(year, layers):

    fig = go.Figure()
    df_b = bathy[bathy['year']==year]
    df_n = notices[notices['year']==year]
    # plot river
    fig.add_trace(
    go.Scattermap(
        lon=lons,
        lat=lats,
        mode="lines",
        line=dict(
            color="#2166ac",
            width=2
        ),
        name="Mississippi River",
        hoverinfo="skip",
        showlegend=False
    )
)

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
            seg = river_mile_points[
                (river_mile_points["RIVER_NAME"] == row["river_name"]) &
                (river_mile_points["MILE"] >= row["mm_low"]) &
                (river_mile_points["MILE"] <= row["mm_high"])
            ]
            if seg.empty:
                continue

            is_upcoming = row["is_upcoming"]
            status = "upcoming" if is_upcoming else "active"
            date_start_str = row["date_start"].strftime("%b %d, %Y") if pd.notna(row["date_start"]) else ""
            date_end_str = row["date_end"].strftime("%b %d, %Y") if pd.notna(row["date_end"]) else ""
            header = (
                f"Draft Restriction begins {date_start_str}" if is_upcoming
                else "Active Draft Restriction"
            )
            # repeated per point so a click on any part of the line carries the full detail
            customdata = [[
                "draft", status, date_start_str, date_end_str,
                row["northbound"] if pd.notna(row["northbound"]) else "",
                row["southbound"] if pd.notna(row["southbound"]) else "",
                row["mm_label"] or "",
            ]] * len(seg)

            fig.add_trace(
                go.Scattermap(
                    lon=seg["LON"],
                    lat=seg["LAT"],
                    mode="lines",
                    line=dict(color=CATEGORY_COLORS["draft"], width=10),
                    opacity=DRAFT_UPCOMING_OPACITY if is_upcoming else DRAFT_ACTIVE_OPACITY,
                    legendgroup=f"draft-{status}",
                    showlegend=not shown_legend[status],
                    name="Draft Restriction" + (" (upcoming)" if is_upcoming else ""),
                    hoverinfo="text",
                    hovertext=f"<b>{header}</b><br>Mile Marker(s): {row['mm_label']}<br>Click for details",
                    customdata=customdata,
                )
            )
            shown_legend[status] = True

    #  bathym layer
    if "bathy" in layers:
        conditions = [
        df_b["depth"] > 30,(df_b["depth"] > 25) & (df_b["depth"] <= 30),
        (df_b["depth"] > 20) & (df_b["depth"] <= 25),(df_b["depth"] > 15) & (df_b["depth"] <= 20),
        df_b["depth"] <= 15]
        sizes = [8, 10, 12, 15, 18]
        df_b["marker_size"] = np.select(conditions, sizes)
        fig.add_trace(
            go.Scattermap(
                lon=df_b["LON"],
                lat=df_b["LAT"],
                mode="markers",
                marker=dict(
                    size=df_b["marker_size"],
                    color=df_b["depth"],
                    colorscale="YlOrRd",
                    reversescale=True,
                    cmin=0,
                    cmax=40,
                    #colorbar=dict(title="Depth (ft)"),
                    opacity=0.7,
                ),
                showlegend=False,
                customdata=df_b[["date"]],
                name="Bathymetry",
                hovertemplate=(
                    "Depth: %{marker.color:.1f} ft<br>"
                    "Date: %{customdata[0]}<extra></extra>"
                )
            )
        )

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
        for _, r in df_cat.iterrows():
            if category == "shoaling":
                lines = [f"<b><span style='font-size:16px'>Shoaling reported on {r['date_str']}</span></b>"]
                lo, hi = r["mm_low"], r["mm_high"]
                if pd.notna(lo) or pd.notna(hi):
                    mile_txt = f"Mile {lo:g}" if (pd.isna(hi) or lo == hi) else f"Miles {lo:g}–{hi:g}"
                    loc_detail = f", {r['location_details']}" if pd.notna(r.get("location_details")) else ""
                    lines.append(f"Location: {mile_txt}{loc_detail}")
                elif pd.notna(r.get("location_details")):
                    lines.append(f"Location: {r['location_details']}")
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
                lo, hi = r["mm_low"], r["mm_high"]
                mile_txt = None
                if pd.notna(lo) or pd.notna(hi):
                    mile_txt = f"Mile {lo:g}" if (pd.isna(hi) or lo == hi) else f"Miles {lo:g}–{hi:g}"
                loc_detail = r.get("location_details")
                if mile_txt and pd.notna(loc_detail):
                    lines.append(f"Location: {mile_txt}, {loc_detail}")
                elif mile_txt:
                    lines.append(f"Location: {mile_txt}")
                elif pd.notna(loc_detail):
                    lines.append(f"Location: {loc_detail}")
            if pd.notna(r.get("instructions")):
                wrapped = "<br>".join(textwrap.wrap(str(r["instructions"]), width=55))
                lines.append(f"<span style='font-size:11px'>{wrapped}</span>")
            hovertexts.append("<br>".join(lines))

        fig.add_trace(
            go.Scattermap(
                lon=df_cat["lon"],
                lat=df_cat["lat"],
                mode="markers",
                marker=dict(
                    size=10,
                    color=CATEGORY_COLORS[category],
                    opacity=0.85,
                ),
                showlegend=True,
                name=CATEGORY_LABELS[category],
                hoverinfo="text",
                hovertext=hovertexts,
            )
        )

    # map layout 
    fig.update_layout(
        mapbox=dict(
            style="basic",
            zoom=8.5,
            center=dict(lat=32.5, lon=-91.1),
        ),
        margin=dict(l=0, r=0, t=0, b=0),
        uirevision="keep-map",
        legend=dict(
            bgcolor="rgba(255,255,255,0.8)",
            x=0.005, xanchor="left",
            y=0.19, yanchor="bottom"
        )
    )
    
    return fig


# --------------------------------------------------
# DRAFT RESTRICTION CLICK DETAIL
# --------------------------------------------------

@app.callback(
    Output("draft-detail-store", "data"),
    Input("map", "clickData"),
    Input("draft-detail-close", "n_clicks"),
    prevent_initial_call=True
)
def handle_draft_click(click_data, n_close):
    if dash.ctx.triggered_id == "draft-detail-close":
        return None
    if click_data and click_data.get("points"):
        customdata = click_data["points"][0].get("customdata")
        if customdata and customdata[0] == "draft":
            _, status, date_start_str, date_end_str, northbound, southbound, mm_label = customdata
            return {
                "status": status,
                "date_start": date_start_str,
                "date_end": date_end_str,
                "northbound": northbound,
                "southbound": southbound,
                "mm_label": mm_label,
            }
    return dash.no_update


@app.callback(
    Output("draft-detail-box", "style"),
    Output("draft-detail-content", "children"),
    Input("draft-detail-store", "data")
)
def render_draft_detail(data):
    if not data:
        return DRAFT_DETAIL_HIDDEN, []

    if data["status"] == "active":
        header = "ACTIVE DRAFT RESTRICTION"
        header_color = CATEGORY_COLORS["draft"]
    else:
        header = f"DRAFT RESTRICTION BEGINS ON {data['date_start'].upper()}"
        header_color = "#b08968"

    children = [
        html.H3(header, style={"margin": "0 0 10px 0", "color": header_color, "font-size": "18px"}),
        html.P(f"Mile Marker(s): {data['mm_label']}") if data["mm_label"] else None,
        html.P([html.B("Start: "), data["date_start"] or "—"]),
        html.P([html.B("End: "), data["date_end"] or "Until further notice"]),
        html.Hr(),
        html.P([html.B("Northbound: "), data["northbound"] or "—"]),
        html.P([html.B("Southbound: "), data["southbound"] or "—"]),
    ]
    return DRAFT_DETAIL_VISIBLE, children


# --------------------------------------------------
# "OTHER" WARNING ICON
# --------------------------------------------------

@app.callback(
    Output("other-warning-icon", "style"),
    Output("other-warning-icon", "title"),
    Input("year-slider", "value")
)
def update_warning_icon(year):
    df_other = notices[
        (notices["category"] == "other")
        & (notices["year"] == year)
        & (notices["is_active_flag"])
    ]
    if df_other.empty:
        return WARNING_ICON_HIDDEN, ""
    tooltip = "\n\n".join(df_other["other_notes"].dropna().astype(str).tolist())
    return WARNING_ICON_VISIBLE, tooltip


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

"""Quick standalone viewer for the dredge_events_2021_2025 shapefile -- one row per
year (2021-2024), each a MultiPolygon of that year's individual dredge-event
footprints aggregated together, plus a vessel list and event count per year.
Overlaid with per-event center points from dredge_events_2021_2025.csv, which
carry per-event attributes (vessel, MMSI, dates, duration) the polygon layer
doesn't have -- hover a dot for that event's detail.
One-off preview so Molly can see the year-over-year footprint before deciding
how/whether to merge it into the Dredging layer. Toggle between years with the
radio buttons above the map.
"""
import os

import geopandas as gpd
import pandas as pd
import plotly.graph_objects as go
import dash
from dash import dcc, html, Input, Output

gdf = gpd.read_file("dredge_events_2021_2025").to_crs(4326)
gdf = gdf.sort_values("year").reset_index(drop=True)

points_df = pd.read_csv("dredge_events_2021_2025.csv")

# Fixed categorical order (not cycled) -- dataviz palette dark-mode steps: blue, aqua, yellow, green
YEAR_COLORS = {
    2021: "#3987e5",
    2022: "#199e70",
    2023: "#c98500",
    2024: "#008300",
}


def _geom_to_lonlat(geom):
    """Explode a MultiPolygon into parallel lon/lat lists (None-separated) for one Scattermap fill trace."""
    lons, lats = [], []
    polys = list(geom.geoms) if geom.geom_type == "MultiPolygon" else [geom]
    for poly in polys:
        for coord in poly.exterior.coords:
            lons.append(coord[0])
            lats.append(coord[1])
        lons.append(None)
        lats.append(None)
    return lons, lats


# Precompute per-year traces so toggling is instant
YEAR_DATA = {}
for _, row in gdf.iterrows():
    year = int(row["year"])
    lons, lats = _geom_to_lonlat(row.geometry)
    vessels = [v.strip() for v in str(row["vessels"]).split(",") if v.strip()]

    pts = points_df[points_df["year"] == year]
    point_hovertext = [
        f"<b>{p.vessel_name}</b><br>"
        f"MMSI {int(p.mmsi) if pd.notna(p.mmsi) else 'unknown'}<br>"
        f"{p.start_date} to {p.end_date}<br>"
        f"Duration: {p.duration_hrs:.1f} hrs<br>"
        f"AIS points: {p.num_points}"
        for p in pts.itertuples()
    ]

    YEAR_DATA[year] = {
        "lons": lons,
        "lats": lats,
        "num_events": int(row["num_events"]),
        "num_polygons": len(row.geometry.geoms) if row.geometry.geom_type == "MultiPolygon" else 1,
        "vessels": vessels,
        "point_lons": pts["center_lon"].tolist(),
        "point_lats": pts["center_lat"].tolist(),
        "point_hovertext": point_hovertext,
    }

bounds = gdf.total_bounds  # minx, miny, maxx, maxy
center = dict(lon=(bounds[0] + bounds[2]) / 2, lat=(bounds[1] + bounds[3]) / 2)


def build_figure(year):
    d = YEAR_DATA[year]
    color = YEAR_COLORS[year]
    fig = go.Figure(go.Scattermap(
        lon=d["lons"],
        lat=d["lats"],
        mode="lines",
        fill="toself",
        fillcolor=color,
        opacity=0.55,
        line=dict(color=color, width=1),
        name=str(year),
        hoverinfo="text",
        hovertext=f"<b>{year}</b><br>{d['num_events']} dredge events<br>{d['num_polygons']} footprint polygons",
    ))
    fig.add_trace(go.Scattermap(
        lon=d["point_lons"],
        lat=d["point_lats"],
        mode="markers",
        marker=dict(size=8, color="white"),
        name=f"{year} events",
        hoverinfo="text",
        hovertext=d["point_hovertext"],
        showlegend=False,
    ))
    fig.update_layout(
        map=dict(style="carto-darkmatter", center=center, zoom=6),
        margin=dict(l=0, r=0, t=40, b=0),
        title=f"Dredge event footprints -- {year} ({d['num_events']} events, {d['num_polygons']} polygons)",
        showlegend=False,
        height=750,
    )
    return fig


app = dash.Dash(__name__, assets_folder="no_assets_for_preview")
app.layout = html.Div([
    dcc.RadioItems(
        id="year-radio",
        options=[{"label": str(y), "value": y} for y in sorted(YEAR_DATA)],
        value=max(YEAR_DATA),
        inline=True,
        style={"fontFamily": "Arial, sans-serif", "fontSize": "16px", "padding": "10px"},
        inputStyle={"marginRight": "6px", "marginLeft": "16px"},
    ),
    dcc.Graph(id="dredge-map", style={"height": "80vh"}),
    html.Div(id="vessel-list", style={
        "fontFamily": "Arial, sans-serif", "fontSize": "13px", "padding": "0 16px 16px",
        "color": "#333",
    }),
])


@app.callback(
    Output("dredge-map", "figure"),
    Output("vessel-list", "children"),
    Input("year-radio", "value"),
)
def update_year(year):
    d = YEAR_DATA[year]
    vessel_text = f"Vessels active in {year} ({len(d['vessels'])}): " + ", ".join(d["vessels"])
    return build_figure(year), vessel_text


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8051)), debug=False)

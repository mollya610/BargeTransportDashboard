import os
import dash
from dash import dcc, html, Input, Output
import geopandas as gpd
import plotly.graph_objects as go
from shapely.ops import linemerge
from datetime import date
import numpy as np
import requests
import pandas as pd
from shapely import wkt

# LOAD DATA
bathy = pd.read_csv("clean_bathymetry.csv")
dredge = pd.read_csv("dredge_data_2022.csv")

# Ensure year is int
bathy["year"] = bathy["year"].astype(int)
dredge["year"] = 2022

years = sorted(bathy["year"].unique())
years = [int(y) for y in years]

# get center point for bathym measures 
bathy["geometry"] = bathy["geometry"].apply(wkt.loads)
bathy = gpd.GeoDataFrame(bathy, geometry="geometry", crs="EPSG:4326")
bathy["rep_point"] = bathy.geometry.representative_point()
bathy["LON"] = bathy["rep_point"].apply(lambda p: p.x)
bathy["LAT"] = bathy["rep_point"].apply(lambda p: p.y)

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

app.layout = html.Div(
    style={"width": "95%", "margin": "auto"},
    children=[
        html.H2("Mississippi River Bathymetry & Dredging"),

        ##################################
        # Parent Div: splits page into left and right columns
        html.Div(
            style={"display": "flex", "gap": "20px"}, 
            children=[

                ################ 
                # LEFT COLUMN: controls + map
                html.Div(  
                    style={"flex": "5","display": "flex", "flex-direction": "column", "gap": "20px","height": "95vh"},
                    children=[

                        # Top controls row
                        html.Div(
                            style={"display": "flex", "gap": "30px", "margin-bottom": "10px", "align-items": "center"},
                            children=[
                                # Year dropdown
                                html.Div(
                                    style={"width": "90px"},
                                    children=[
                                        html.Label("Select Year"),
                                        dcc.Dropdown(
                                            id="year-slider",
                                            options=[{"label": str(y), "value": y} for y in years],
                                            value=years[0],
                                            clearable=False,
                                            style={"height": "40px", "font-size": "15px"}
                                        )
                                    ]
                                ),

                                # Layers checklist
                                html.Div(
                                    style={"width": "150px"},
                                    children=[
                                        html.Label("Layers"),
                                        dcc.Checklist(
                                            id="layer-toggle",
                                            options=[
                                                {"label": "Bathymetry", "value": "bathy"},
                                                {"label": "Dredging", "value": "dredge"},
                                            ],
                                            value=["bathy", "dredge"],
                                            inline=True
                                        )
                                    ]
                                ),

                                # Colorbar
                                html.Div(
                                    style={"width": "200px"},
                                    children=[
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
                                                                title="Depth (ft)",
                                                                orientation="h",
                                                                thickness=10,
                                                                len=1.0,
                                                            ),
                                                            size=0
                                                        ),
                                                        showlegend=False
                                                    )
                                                ],
                                                "layout": go.Layout(
                                                    margin=dict(l=0, r=0, t=0, b=0),
                                                    height=50,
                                                )
                                            },
                                            config={"displayModeBar": False},
                                            style={"height": "60px"}
                                        )
                                    ]
                                )
                            ]
                        ),

                        # Map below controls
                        html.Div(
                            style={"height": "80vh"},
                            children=[
                                dcc.Graph(id="map", style={"height": "100%"})
                            ]
                        )

                    ]
                ),

                ################
                # RIGHT COLUMN: plots
                html.Div(
                    style={"flex": "4", "display": "flex", "flex-direction": "column", "gap": "20px","height": "90vh","overflow-y": "scroll"},
                    children=[
                        dcc.Graph(
                            id="barge-rate-plot",
                            style={"height": "300px"}  # fills the column
                        ),
                        dcc.Graph(
                            id="water-plot",
                            style={"height": "300px"}  # fills the column
                        ),
                        dcc.Graph(
                            id="cornprice-plot",
                            style={"height": "300px"}  # fills the column
                        ),
                        dcc.Graph(
                            id="soyprice-plot",
                            style={"height": "300px"}  # fills the column
                        )
                        # Additional plots can be added as more children
                    ]
                )

            ]
        )
    ]
)



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
    if year == thisyear: 
        df_b = bathy[(bathy["date"] >= start_date) &(bathy["date"] <= end_date)]
        df_d = dredge[(dredge["date"] >= start_date) &(dredge["date"] <= end_date)]
    else: 
        df_b = bathy[bathy['year']==year]
        df_d = dredge[dredge['year']==year]
    conditions = [
    df_b["depth"] > 30,(df_b["depth"] > 25) & (df_b["depth"] <= 30),
    (df_b["depth"] > 20) & (df_b["depth"] <= 25),(df_b["depth"] > 15) & (df_b["depth"] <= 20),
    df_b["depth"] <= 15]
    sizes = [8, 10, 12, 15, 18] 
    df_b["marker_size"] = np.select(conditions, sizes)
    
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
    
    #  bathym layer 
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

    # dredge layer 
    fig.add_trace(
        go.Scattermap(
            lon=df_d["LON"],
            lat=df_d["LAT"],
            mode="markers",
            marker=dict(
                size=5,
                color="green",
                #symbol="^",
                opacity=0.9,
            ),
            customdata=df_d[["BaseDateTime"]],
            showlegend=False,
            name="Dredging Locations",
            hovertemplate=(
                "Dredging Site<br>"
                "Date: %{customdata[0]}<extra></extra>"
            )
        )
    )

    # map layout 
    fig.update_layout(
        mapbox=dict(
            style="carto-positron",
            zoom=7,
            center=dict(lat=38.5, lon=-90.5),
        ),
        margin=dict(l=0, r=0, t=0, b=0),
        uirevision="keep-map",
        legend=dict(bgcolor="rgba(255,255,255,0.8)")
    )
    
    return fig

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

# -*- coding: utf-8 -*-
"""
Created on Wed Sep 10 14:11:05 2025

@author: malcor
"""

import os
import pandas as pd
import geopandas as gpd
import requests
import matplotlib.pyplot as plt
from functools import reduce
import numpy as np
import statsmodels.formula.api as sm
import seaborn as sns
from shapely.geometry import Point
import contextily as ctx
import zipfile
import io


#### READ IN WATER LEVEL DATA 
os.chdir('C:/Users/malcor/OneDrive - University of North Carolina at Chapel Hill/Research/Data/USACE_StageData') 
stlouis = pd.read_csv('stlouis_stage.csv',parse_dates=['time']).rename(columns={'time':'date','value':'stlouis'}).assign(stlouis=lambda d: pd.to_numeric(d['stlouis'], errors='coerce'))[['date','stlouis']] 
stlouis['date'] = pd.to_datetime(stlouis['date']) 
chester = pd.read_csv('chester_stage.csv',parse_dates=['time']).rename(columns={'time':'date','value':'chester'}).assign(chester=lambda d: pd.to_numeric(d['chester'], errors='coerce'))[['date','chester']] 
chester['date'] = pd.to_datetime(chester['date']) 
capegir = pd.read_excel('capegir_stage.xlsx',header=14,parse_dates=['Date / Time']).rename(columns={'Date / Time':'date','Stage (Ft)':'capegir'}).assign(capegir=lambda d: pd.to_numeric(d['capegir'], errors='coerce'))[:-1] 
capegir['date'] = pd.to_datetime(capegir['date']) 
newmadrid = pd.read_excel('newmadrid_mo_stage.xlsx',header=11,parse_dates=['Date / Time']).rename(columns={'Date / Time':'date','Stage (Ft)':'newmadrid'}).assign(newmadrid=lambda d: pd.to_numeric(d['newmadrid'], errors='coerce'))[:-1] 
newmadrid['date'] = pd.to_datetime(newmadrid['date']) 
hickman = pd.read_excel('hickman_ky_stage.xlsx',header=11,parse_dates=['Date / Time']).rename(columns={'Date / Time':'date','Stage (Ft)':'hickman'}).assign(hickman=lambda d: pd.to_numeric(d['hickman'], errors='coerce'))[:-1] 
hickman['date'] = pd.to_datetime(hickman['date']) 
wickliffe = pd.read_excel('wickliffe_ky_stage.xlsx',header=12,parse_dates=['Date / Time']).rename(columns={'Date / Time':'date','Stage (Ft)':'wickliffe'}).assign(wickliffe=lambda d: pd.to_numeric(d['wickliffe'], errors='coerce'))[:-1] 
wickliffe['date'] = pd.to_datetime(wickliffe['date']) 
tipton = pd.read_excel('tiptonville_tn_stage.xlsx',header=10,parse_dates=['Date / Time']).rename(columns={'Date / Time':'date','Stage (Ft)':'tipton'}).assign(tipton=lambda d: pd.to_numeric(d['tipton'], errors='coerce'))[:-1] 
tipton['date'] = pd.to_datetime(tipton['date']) 
caruthers = pd.read_excel('caruthersville_mo_stage.xlsx',header=11,parse_dates=['Date / Time']).rename(columns={'Date / Time':'date','Stage (Ft)':'caruthers'}).assign(caruthers=lambda d: pd.to_numeric(d['caruthers'], errors='coerce'))[:-1] 
caruthers['date'] = pd.to_datetime(caruthers['date']) 
memphis = pd.read_excel('memphis_stage.xlsx',header=28,parse_dates=['Date / Time']).rename(columns={'Date / Time':'date','Stage (Ft)':'memphis'}).assign(memphis=lambda d: pd.to_numeric(d['memphis'], errors='coerce'))[:-1] 
memphis['date'] = pd.to_datetime(memphis['date']) 
helena = pd.read_excel('helena_ar_stage.xlsx',header=11,parse_dates=['Date / Time']).rename(columns={'Date / Time':'date','Stage (Ft)':'helena'}).assign(helena=lambda d: pd.to_numeric(d['helena'], errors='coerce'))[:-1] 
helena['date'] = pd.to_datetime(helena['date']) 
friarsp = pd.read_excel('friarspoint_ms_stage.xlsx',header=11,parse_dates=['Date / Time']).rename(columns={'Date / Time':'date','Stage (Ft)':'friarsp'}).assign(friarsp=lambda d: pd.to_numeric(d['friarsp'], errors='coerce'))[:-1] 
friarsp['date'] = pd.to_datetime(friarsp['date']) 
arkcity = pd.read_excel('arkcity_ar_stage.xlsx',header=11,parse_dates=['Date / Time']).rename(columns={'Date / Time':'date','Stage (Ft)':'arkcity'}).assign(arkcity=lambda d: pd.to_numeric(d['arkcity'], errors='coerce'))[:-1] 
arkcity['date'] = pd.to_datetime(arkcity['date']) 
osceola = pd.read_excel('osceola_ar_stage.xlsx',header=11,parse_dates=['Date / Time']).rename(columns={'Date / Time':'date','Stage (Ft)':'osceola'}).assign(osceola=lambda d: pd.to_numeric(d['osceola'], errors='coerce'))[:-1] 
osceola['date'] = pd.to_datetime(osceola['date']) 
mhoonl = pd.read_excel('mhoon_landing_ms_stage.xlsx',header=11,parse_dates=['Date / Time']).rename(columns={'Date / Time':'date','Stage (Ft)':'mhoonl'}).assign(mhoonl=lambda d: pd.to_numeric(d['mhoonl'], errors='coerce'))[:-1] 
mhoonl['date'] = pd.to_datetime(mhoonl['date']) 
rosedale = pd.read_excel('rosedale_stage.xlsx',header=12,parse_dates=['Date / Time']).rename(columns={'Date / Time':'date','Stage (Ft)':'rosedale'}).assign(rosedale=lambda d: pd.to_numeric(d['rosedale'], errors='coerce'))[:-1] 
rosedale['date'] = pd.to_datetime(rosedale['date']) 
greenville = pd.read_excel('greenville_stage.xlsx',header=11,parse_dates=['Date / Time']).rename(columns={'Date / Time':'date','Stage (Ft)':'greenville'}).assign(greenville=lambda d: pd.to_numeric(d['greenville'], errors='coerce'))[:-1] 
greenville['date'] = pd.to_datetime(greenville['date']) 
lakeprov = pd.read_excel('lakeprovidence_stage.xlsx',header=10,parse_dates=['Date / Time']).rename(columns={'Date / Time':'date','Stage (Ft)':'lakeprov'}).assign(lakeprov=lambda d: pd.to_numeric(d['lakeprov'], errors='coerce'))[:-1] 
lakeprov['date'] = pd.to_datetime(lakeprov['date']) 
vicksburg = pd.read_excel('vicksburg_stage.xlsx',header=11,parse_dates=['Date / Time']).rename(columns={'Date / Time':'date','Stage (Ft)':'vicksburg'}).assign(vicksburg=lambda d: pd.to_numeric(d['vicksburg'], errors='coerce'))[:-1] 
vicksburg['date'] = pd.to_datetime(vicksburg['date']) 
natchez = pd.read_excel('natchez_stage.xlsx',header=11,parse_dates=['Date / Time']).rename(columns={'Date / Time':'date','Stage (Ft)':'natchez'}).assign(natchez=lambda d: pd.to_numeric(d['natchez'], errors='coerce'))[:-1] 
natchez['date'] = pd.to_datetime(natchez['date']) 
brouge = pd.read_excel('batonrouge_stage.xlsx',header=18,parse_dates=['Date / Time']).rename(columns={'Date / Time':'date','Stage (Ft)':'brouge'}).assign(brouge=lambda d: pd.to_numeric(d['stage'], errors='coerce'))[:-1]
brouge['date'] = pd.to_datetime(brouge['date'])
# clean dfs 
capegir = capegir.iloc[:, :2]
newmadrid = newmadrid.iloc[:, :2]
hickman = hickman.iloc[:, :2]
wickliffe = wickliffe.iloc[:, :2]
tipton = tipton.iloc[:, :2]
caruthers = caruthers.iloc[:, :2]
memphis = memphis.iloc[:, :2]
helena = helena.iloc[:, :2]
friarsp = friarsp.iloc[:, :2]
arkcity = arkcity.iloc[:, :2]
osceola = osceola.iloc[:, :2]
mhoonl = mhoonl.iloc[:, :2]
rosedale = rosedale.iloc[:, :2]
greenville = greenville.iloc[:, :2]
lakeprov = lakeprov.iloc[:, :2]
vicksburg = vicksburg.iloc[:, :2]
natchez = natchez.iloc[:, :2]
brouge = brouge.iloc[:, :2]

dfs = [stlouis,chester,capegir,wickliffe,hickman,newmadrid,tipton,caruthers,osceola,memphis,mhoonl,helena,friarsp,rosedale,arkcity,greenville,lakeprov,vicksburg,natchez,brouge]
for df in dfs:
    df['date'] = df['date'].dt.floor('D')


# CALCULATE THRESH FOR EVERY GAGE
os.chdir('C:/Users/malcor/OneDrive - University of North Carolina at Chapel Hill/Research/Data/Stage_Datum_Info') 
datums = pd.read_csv('convert_datums.csv',header=0)
datums['thresh_el'] = np.zeros(len(datums))
for i,df in enumerate(dfs): 
    df = df[(df.iloc[:, 1]>-100)&(df.iloc[:, 1]<100)]
    df['w_el'] = df.iloc[:, 1]+ datums['Datum88'].iloc[i]
    measure = df[df['date'].dt.month==10]
    measure = measure[(measure['date'].dt.year == 2022)|(measure['date'].dt.year == 2023)]
    thresh = measure['w_el'].mean()
    datums['thresh_el'].iloc[i] = thresh
    plt.figure()
    for y in range(2020,2025):
        stage = df[df['date'].dt.year == y]
        stage['day'] = stage['date'].dt.dayofyear
        plt.plot(stage['day'],stage['w_el'],label=y)
    plt.axhline(y=thresh, color="red", linestyle="--", linewidth=1.2, label=f"Thresh {thresh:.2f}")
    plt.grid()
    plt.title(datums['CityName'].iloc[i])
    plt.legend()


datumsave = datums[['CityName','LAT','LON','MileMarker','thresh_el']]
datumsave.to_csv('datum_thresholds.csv')

datum_mile = datums.sort_values(by='MileMarker').reset_index()[['MileMarker','thresh_el']]
datum_mile['MileMarker'] = datum_mile['MileMarker'].astype(float)
full_range = pd.DataFrame({'MileMarker':np.arange(229,1132,1)}).astype(float)
thresh_miles = pd.merge(datum_mile,full_range,how='outer',on='MileMarker')
thresh_miles['thresh_el'] = thresh_miles['thresh_el'].interpolate(method="linear")
thresh_miles = thresh_miles.dropna()
# now download and join with milemarkers 
os.chdir('C:/Users/malcor/OneDrive - University of North Carolina at Chapel Hill/Research/Data/Stage_Datum_Info')
markers = pd.read_csv('usace_river_mile_markers.csv')
markers = markers[(markers['RIVER_NAME'] == 'MISSISSIPPI-LO')|(markers['RIVER_NAME'] == 'MISSISSIPPI-UP')]
markers['MILE_FULL'] = markers['MILE']
markers.loc[markers['RIVER_NAME'] == 'MISSISSIPPI-UP','MILE_FULL'] += 953
markers = markers[['MILE_FULL','LAT','LON']].rename(columns={'MILE_FULL':'MileMarker'})
markers['MileMarker'] = markers['MileMarker'].astype(float) 
thresh_miles_m = pd.merge(thresh_miles,markers,on='MileMarker',how='inner')
thresh_miles_m.to_csv('datum_info.csv')








        
        
        
        
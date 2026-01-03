import requests
import zipfile
import os
import io
import time
import geopandas as gpd

# GET ALL IDS 
url = "https://services7.arcgis.com/n1YM8pTrFmm7L4hs/arcgis/rest/services/eHydro_Survey_Data/FeatureServer/0/query"
params = {"f": "json","where": "1=1", 
    "outFields": "surveyjobidpk","resultOffset": 0,
    "resultRecordCount": 2000,
    "returnGeometry": "false"}
all_ids = []
while True:
    r = requests.get(url, params=params)
    r.raise_for_status()
    data = r.json()
    feats = data.get("features", [])
    if not feats:
        break
    all_ids.extend([f["attributes"]["surveyjobidpk"] for f in feats])
    params["resultOffset"] += params["resultRecordCount"]    
lm_ids_all = [i for i in all_ids if str(i).startswith("LM")]
um_ids_all = [i for i in all_ids if str(i).startswith("UM")]

# SEE IF ANY IDS ARE NEW 
old_lm = pd.read_csv('lm_ids_done.csv')
old_um = pd.read_csv('um_ids_done.csv')
old_lm_set = set(old_lm['ID'])
old_um_set = set(old_um['ID'])
new_lm_ids = [i for i in lm_ids_all if i not in old_lm_set]
new_um_ids = [i for i in um_ids_all if i not in old_um_set]
print(f'there are {len(new_lm_ids)} new lower mspi river surveys')
print(f'there are {len(new_um_ids)} new upper mspi river surveys')

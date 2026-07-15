"""Fetch the last ~10 years of final US corn and soybean production from the
NASS QuickStats API and save them to a history CSV.

QuickStats requires a free API key -- get one instantly at
https://quickstats.nass.usda.gov/api and set it as the NASS_API_KEY
environment variable (a GitHub Actions secret in the daily pipeline).

This is the *final*, survey-confirmed production for each marketing year
(published every January in NASS's Crop Production Annual Summary), which
is why it always lags about a year behind the current WASDE estimate --
the current/upcoming marketing year's number comes from fetch_wasde.py
instead, since NASS hasn't measured a not-yet-harvested crop.
"""
import os
from pathlib import Path

import pandas as pd
import requests

API_URL = "https://quickstats.nass.usda.gov/api/api_GET/"
OUT_CSV = Path("grain_production_history.csv")
NUM_YEARS = 10

SHORT_DESC = {
    "corn": "CORN, GRAIN - PRODUCTION, MEASURED IN BU",
    "soybean": "SOYBEANS - PRODUCTION, MEASURED IN BU",
}


def _fetch(short_desc, year_ge, year_le, api_key):
    params = {
        "key": api_key,
        "short_desc": short_desc,
        "agg_level_desc": "NATIONAL",
        "year__GE": year_ge,
        "year__LE": year_le,
        "format": "JSON",
    }
    resp = requests.get(API_URL, params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    if "error" in payload:
        raise ValueError(f"NASS QuickStats error for '{short_desc}': {payload['error']}")
    return payload["data"]


def fetch_production_history():
    api_key = os.environ.get("NASS_API_KEY")
    if not api_key:
        raise RuntimeError("NASS_API_KEY environment variable is not set")

    this_year = pd.Timestamp.now().year
    year_le = this_year - 1
    year_ge = this_year - NUM_YEARS

    by_year = {}
    for crop, short_desc in SHORT_DESC.items():
        records = _fetch(short_desc, year_ge, year_le, api_key)
        for rec in records:
            year = int(rec["year"])
            value = float(str(rec["Value"]).replace(",", ""))
            by_year.setdefault(year, {})[f"{crop}_production_million_bu"] = value / 1_000_000

    rows = [{"year": year, **values} for year, values in sorted(by_year.items())]
    return pd.DataFrame(rows)


def main():
    try:
        df = fetch_production_history()
    except Exception as e:
        print(f"Grain production history: ERROR — {e}")
        return
    df.to_csv(OUT_CSV, index=False)
    print(f"Saved {len(df)} years of production history -> {OUT_CSV}")


if __name__ == "__main__":
    main()

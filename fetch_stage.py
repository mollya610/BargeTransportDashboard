"""Fetch daily river stage for each gage and append to river_stage_history.csv.

Run from the GitHub Actions daily pipeline. On first run, seeds the CSV with
the last 30 days of history. On subsequent runs, appends today's reading and
deduplicates so re-runs are safe.
"""
import requests
import pandas as pd
from pathlib import Path

CSV_PATH = Path("river_stage_history.csv")

GAGES = {
    "St. Louis": {"source": "usgs", "site": "07010000"},
    "Memphis":   {"source": "nws",  "site": "MEMT1"},
    "Greenville":{"source": "nws",  "site": "GEEM6"},
}


def fetch_usgs(site_no, days=30):
    end = pd.Timestamp.today().normalize()
    start = end - pd.Timedelta(days=days - 1)
    url = (
        "https://waterservices.usgs.gov/nwis/dv/"
        f"?sites={site_no}&parameterCd=00065"
        f"&startDT={start.date()}&endDT={end.date()}"
        "&format=json&siteStatus=all"
    )
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    ts = r.json()["value"]["timeSeries"]
    if not ts:
        return pd.DataFrame(columns=["date", "stage"])
    vals = ts[0]["values"][0]["value"]
    df = pd.DataFrame(vals)[["dateTime", "value"]].rename(
        columns={"dateTime": "date", "value": "stage"}
    )
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df["stage"] = pd.to_numeric(df["stage"], errors="coerce")
    return df.dropna(subset=["stage"])


def fetch_nws(gauge_id, days=30):
    url = f"https://api.water.noaa.gov/nwps/v1/gauges/{gauge_id}/stageflow"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    data = r.json().get("observed", {}).get("data", [])
    if not data:
        return pd.DataFrame(columns=["date", "stage"])
    df = pd.DataFrame(data)[["validTime", "primary"]].rename(
        columns={"validTime": "date", "primary": "stage"}
    )
    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
    df["stage"] = pd.to_numeric(df["stage"], errors="coerce")
    df = df.dropna(subset=["stage"])
    df["date"] = df["date"].dt.normalize()
    # hourly → daily: take the last reading of each day
    df = df.groupby("date")["stage"].last().reset_index()
    cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=days - 1)
    return df[df["date"] >= cutoff]


def main():
    existing = pd.read_csv(CSV_PATH, parse_dates=["date"]) if CSV_PATH.exists() \
        else pd.DataFrame(columns=["date", "gage", "stage"])

    new_rows = []
    for gage_name, info in GAGES.items():
        try:
            df = fetch_usgs(info["site"]) if info["source"] == "usgs" \
                else fetch_nws(info["site"])
            df["gage"] = gage_name
            new_rows.append(df[["date", "gage", "stage"]])
            print(f"  {gage_name}: {len(df)} rows")
        except Exception as e:
            print(f"  {gage_name}: ERROR — {e}")

    if not new_rows:
        print("No data fetched — CSV unchanged.")
        return

    combined = pd.concat([existing] + new_rows, ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"]).dt.normalize()
    combined = combined.drop_duplicates(subset=["date", "gage"], keep="last")
    combined = combined.sort_values(["gage", "date"]).reset_index(drop=True)
    combined.to_csv(CSV_PATH, index=False, date_format="%Y-%m-%d")
    print(f"Saved {len(combined)} total rows to {CSV_PATH}")


if __name__ == "__main__":
    main()

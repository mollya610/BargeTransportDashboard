"""Build a day-of-year river stage climatology from USACE historical records.

Reads the 2000-2025 stage histories in `threshold calculation/` (downloaded
from USACE rivergages) and computes, per gage, the average/p25/p75 stage
for a +/-3 day window around each day of year. Output is consumed by app.py
to compare the current reading against the historical norm for that week,
instead of a trailing rolling average.
"""
import numpy as np
import pandas as pd
from pathlib import Path

BASE = Path(__file__).parent
THRESH_DIR = BASE / "threshold calculation"
WINDOW_DAYS = 3


def load_memphis():
    df = pd.read_excel(THRESH_DIR / "memphis_stage.xlsx", header=None)
    df = df.iloc[:, :2]
    df.columns = ["date", "stage"]
    df["date"] = pd.to_datetime(df["date"], format="mixed", errors="coerce")
    df["stage"] = pd.to_numeric(df["stage"], errors="coerce")
    df = df.dropna(subset=["date", "stage"])
    df["gage"] = "Memphis"
    return df[["date", "gage", "stage"]]


def load_greenville():
    df = pd.read_excel(THRESH_DIR / "greenville_stage.xlsx", header=None)
    df.columns = ["date", "stage"]
    df["date"] = pd.to_datetime(df["date"], format="mixed", errors="coerce")
    df["stage"] = pd.to_numeric(df["stage"], errors="coerce")
    df = df.dropna(subset=["date", "stage"])
    df["gage"] = "Greenville"
    return df[["date", "gage", "stage"]]


def load_stlouis():
    df = pd.read_csv(THRESH_DIR / "stlouis_stage.csv")
    df = df[df["parameter_code"] == 65]
    df["date"] = pd.to_datetime(df["time"]).dt.normalize()
    df["stage"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["date", "stage"])
    df["gage"] = "St. Louis"
    return df[["date", "gage", "stage"]]


def build_climatology(df):
    df = df.copy()
    df["doy"] = df["date"].dt.dayofyear
    df.loc[df["doy"] == 366, "doy"] = 365  # fold leap day into day 365

    rows = []
    for gage, g in df.groupby("gage"):
        by_doy = g.groupby("doy")["stage"].apply(list).to_dict()
        for doy in range(1, 366):
            window = [((doy - 1 + off) % 365) + 1 for off in range(-WINDOW_DAYS, WINDOW_DAYS + 1)]
            vals = np.array([v for d in window for v in by_doy.get(d, [])])
            if vals.size == 0:
                continue
            rows.append({
                "gage": gage,
                "day_of_year": doy,
                "avg_stage": vals.mean(),
                "p25_stage": np.percentile(vals, 25),
                "p75_stage": np.percentile(vals, 75),
                "n_obs": vals.size,
            })
    return pd.DataFrame(rows)


def main():
    combined = pd.concat([load_memphis(), load_greenville(), load_stlouis()], ignore_index=True)
    combined = combined.drop_duplicates(subset=["date", "gage"])

    clim = build_climatology(combined)
    out_path = BASE / "river_stage_climatology.csv"
    clim.to_csv(out_path, index=False)

    print(f"Saved {len(clim)} rows to {out_path}")
    for gage, g in combined.groupby("gage"):
        print(f"  {gage}: {g['date'].dt.year.min()}-{g['date'].dt.year.max()}, {len(g)} obs")


if __name__ == "__main__":
    main()

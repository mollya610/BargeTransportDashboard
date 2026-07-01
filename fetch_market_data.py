"""Fetch USDA barge freight rate and corn/soy price spread data, and append
any new data points to local CSV history files.

Run from the GitHub Actions daily pipeline. The USDA workbooks get revised
weekly, so each run re-downloads them and merges new rows into the history
CSVs, deduplicating by date so re-runs are safe.
"""
import requests
import pandas as pd
from pathlib import Path

BARGE_CSV = Path("barge_rates_history.csv")
CORN_CSV = Path("corn_price_history.csv")
SOY_CSV = Path("soy_price_history.csv")


def _download(url, dest):
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    if b"<!DOCTYPE" in response.content[:100] or b"<html" in response.content[:100]:
        raise ValueError(f"USDA returned HTML instead of an xlsx file ({url})")
    with open(dest, "wb") as f:
        f.write(response.content)


def fetch_barge_rates():
    url = "https://www.ams.usda.gov/sites/default/files/media/GTRFigure10Table9.xlsx"
    _download(url, "freight_rates_southbound.xlsx")

    freight_rates = pd.read_excel("freight_rates_southbound.xlsx", sheet_name="Table 9_data", header=2, usecols=range(5))
    barge_rates = freight_rates.rename(columns={"All Points": "week", "ST LOUIS": "stlrate_per_ton"})
    barge_rates = barge_rates.drop(index=[0, 1])
    barge_rates = barge_rates.loc[:, ("week", "stlrate_per_ton")]
    barge_rates["week"] = pd.to_datetime(barge_rates["week"])
    barge_rates["stlrate_per_ton"] = (barge_rates["stlrate_per_ton"] * 3.99) / 100
    return barge_rates.dropna(subset=["week", "stlrate_per_ton"])


def fetch_corn_soy_prices():
    url = "https://www.ams.usda.gov/sites/default/files/media/GTRTable2A_B.xlsx"
    _download(url, "price_spreads_futures_usda.xlsx")

    corn_soy_spread = pd.read_excel("price_spreads_futures_usda.xlsx", sheet_name="Data", header=1, usecols=range(9))
    corn_soy_spread = corn_soy_spread[
        corn_soy_spread["Origin--destination"].isin(["IL--Gulf", "IL–Gulf", "IA–Gulf", "IA--Gulf"])
    ]

    corn_spread = corn_soy_spread[corn_soy_spread["Commodity"] == "Corn"].rename(
        columns={"Unnamed: 0": "date", "Destination Price": "gulf_corn_price"}
    )
    corn_price = corn_spread.loc[:, ("date", "gulf_corn_price")].copy()
    corn_price["date"] = pd.to_datetime(corn_price["date"])
    corn_price = corn_price.dropna(subset=["date", "gulf_corn_price"])

    soy_spread = corn_soy_spread.rename(columns={"Unnamed: 0": "date", "Destination Price": "gulf_soy_price"})
    soy_spread["date"] = soy_spread["date"].shift(1)
    soy_spread = soy_spread[soy_spread["Commodity"] == "Soybean"]
    soy_spread["date"] = soy_spread["date"].shift(1)
    soy_price = soy_spread.loc[:, ("date", "gulf_soy_price")].copy()
    soy_price["date"] = pd.to_datetime(soy_price["date"])
    soy_price = soy_price.dropna(subset=["date", "gulf_soy_price"])

    return corn_price, soy_price


def _merge_and_save(csv_path, new_df, key_col):
    existing = pd.read_csv(csv_path, parse_dates=[key_col]) if csv_path.exists() else pd.DataFrame(columns=new_df.columns)
    combined = pd.concat([existing, new_df], ignore_index=True)
    combined[key_col] = pd.to_datetime(combined[key_col])
    combined = combined.drop_duplicates(subset=[key_col], keep="last")
    combined = combined.sort_values(key_col).reset_index(drop=True)
    combined.to_csv(csv_path, index=False, date_format="%Y-%m-%d")
    print(f"  Saved {len(combined)} total rows to {csv_path}")


def main():
    try:
        barge_rates = fetch_barge_rates()
        _merge_and_save(BARGE_CSV, barge_rates, "week")
    except Exception as e:
        print(f"Barge rates: ERROR — {e}")

    try:
        corn_price, soy_price = fetch_corn_soy_prices()
        _merge_and_save(CORN_CSV, corn_price, "date")
        _merge_and_save(SOY_CSV, soy_price, "date")
    except Exception as e:
        print(f"Corn/soy prices: ERROR — {e}")


if __name__ == "__main__":
    main()

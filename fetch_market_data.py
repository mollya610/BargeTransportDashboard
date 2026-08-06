"""Fetch USDA barge freight rate and corn/soy price spread data, and append
any new data points to local CSV history files.

Run from the GitHub Actions daily pipeline. The USDA workbooks get revised
weekly, so each run re-downloads them and merges new rows into the history
CSVs, deduplicating by date so re-runs are safe.
"""
import os

import requests
import pandas as pd
from pathlib import Path

BARGE_CSV = Path("barge_rates_history.csv")
NXTMONTH_CSV = Path("barge_rates_nextmonth_history.csv")
THREEMONTH_CSV = Path("barge_rates_threemonth_history.csv")
CORN_CSV = Path("corn_price_history.csv")
SOY_CSV = Path("soy_price_history.csv")
CORN_SPREAD_CSV = Path("corn_spread_history.csv")
FUTURES_CSV = Path("futures_dec_nov_history.csv")

MARS_API_URL = "https://marsapi.ams.usda.gov/services/v1.2/reports/3192"  # Illinois Grain Bids

_MONTH_NUM_TO_NAME = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}

_MONTH_ALIASES = {
    "jan": "Jan", "mar": "Mar", "march": "Mar", "may": "May",
    "jul": "Jul", "july": "Jul", "sep": "Sep", "sept": "Sep",
    "nov": "Nov", "dec": "Dec",
}


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


def _forward_rate_df(sheet_name):
    # NXTMONTH/THREEMONTH have a second, unused block of the same columns (DATE.1,
    # MONTH.1, ...) further right in the sheet -- naming the columns explicitly picks
    # up only the first (live) block.
    df = pd.read_excel(
        "freight_rates_southbound.xlsx", sheet_name=sheet_name, header=0,
        usecols=["DATE", "MONTH", "ST LOUIS"],
    )
    df = df.rename(columns={"DATE": "week", "MONTH": "contract_month", "ST LOUIS": "fwd_rate_per_ton"})
    df["week"] = pd.to_datetime(df["week"])
    df["fwd_rate_per_ton"] = (df["fwd_rate_per_ton"] * 3.99) / 100
    # the contract month is a bare 1-12 number with no year -- a contract month earlier
    # than the quote week's own month means it's rolled over into the following year
    # (e.g. a December quote for a January contract)
    contract_year = df["week"].dt.year + (df["contract_month"] < df["week"].dt.month).astype(int)
    df["contract_month_label"] = df["contract_month"].map(_MONTH_NUM_TO_NAME) + " " + contract_year.astype(str)
    return df.dropna(subset=["week", "fwd_rate_per_ton"]).loc[:, ["week", "contract_month_label", "fwd_rate_per_ton"]]


def fetch_forward_barge_rates():
    url = "https://www.ams.usda.gov/sites/default/files/media/GTRFigure10Table9.xlsx"
    _download(url, "freight_rates_southbound.xlsx")
    return _forward_rate_df("NXTMONTH"), _forward_rate_df("THREEMONTH")


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

    # this same sheet already carries a precomputed origin-minus-destination spread
    # ("Price spreads") -- Illinois only, not the IA--Gulf rows also present above
    il_gulf_corn = corn_soy_spread[
        (corn_soy_spread["Commodity"] == "Corn")
        & (corn_soy_spread["Origin--destination"].isin(["IL--Gulf", "IL–Gulf"]))
    ].rename(columns={"Unnamed: 0": "date", "Price spreads": "il_gulf_corn_spread"})
    corn_spread_df = il_gulf_corn.loc[:, ("date", "il_gulf_corn_spread")].copy()
    corn_spread_df["date"] = pd.to_datetime(corn_spread_df["date"])
    corn_spread_df["il_gulf_corn_spread"] = pd.to_numeric(corn_spread_df["il_gulf_corn_spread"], errors="coerce")
    corn_spread_df = corn_spread_df.dropna(subset=["date", "il_gulf_corn_spread"])

    soy_spread = corn_soy_spread.rename(columns={"Unnamed: 0": "date", "Destination Price": "gulf_soy_price"})
    soy_spread["date"] = soy_spread["date"].shift(1)
    soy_spread = soy_spread[soy_spread["Commodity"] == "Soybean"]
    soy_spread["date"] = soy_spread["date"].shift(1)
    soy_price = soy_spread.loc[:, ("date", "gulf_soy_price")].copy()
    soy_price["date"] = pd.to_datetime(soy_price["date"])
    soy_price = soy_price.dropna(subset=["date", "gulf_soy_price"])

    return corn_price, soy_price, corn_spread_df


def _normalize_month(value):
    if pd.isna(value):
        return None
    return _MONTH_ALIASES.get(str(value).strip().lower(), str(value).strip().title())


def fetch_dec_nov_futures():
    """Pull the December-corn / November-soybean new-crop futures price
    history off the "Futures" sheet of the workbook fetch_corn_soy_prices()
    already downloads. That sheet tracks whichever contract month is the
    current "new crop" reference each week (Dec for corn, Nov for soybeans,
    both rolling onto that label for the same ~Aug-Nov stretch each year).

    Soybean futures have no December contract, so on rows where the sheet's
    own "Sybn Month" label reads "Dec" (a labeling artifact in recent report
    vintages) it's still the Nov new-crop soybean price -- the corn column's
    "Dec" tag is what actually identifies the new-crop window, so that's
    used as the master filter for both series.
    """
    df = pd.read_excel("price_spreads_futures_usda.xlsx", sheet_name="Futures", header=None)
    sub = df.iloc[8:, [0, 1, 6, 7, 9]].copy()
    sub.columns = ["date", "contract", "chic_corn", "chic_sybn", "corn_month"]
    sub = sub.dropna(subset=["date"])
    sub["date"] = pd.to_datetime(sub["date"])

    corn_month = sub["corn_month"].where(sub["corn_month"].notna(), sub["contract"]).apply(_normalize_month)
    new_crop = sub[corn_month == "Dec"]

    futures = new_crop.rename(columns={"chic_corn": "corn_dec_futures", "chic_sybn": "soy_nov_futures"})
    futures = futures.loc[:, ("date", "corn_dec_futures", "soy_nov_futures")]
    return futures.dropna(subset=["date"]).drop_duplicates(subset=["date"])


def _mars_detail_rows(date_range, api_key):
    """Query the MARS API (USDA AMS's MyMarketNews API) for the Illinois Grain
    Bids report's structured "Report Detail" rows over `date_range`
    (e.g. "07/01/2026:07/07/2026", or a single "MM/DD/YYYY").  Each row is one
    location's cash bid, expressed as a basis (in cents/bu) off a named
    futures contract month -- e.g. "basis Min Futures Month": "December (Z)".
    """
    resp = requests.get(
        MARS_API_URL,
        params={"q": f"report_begin_date={date_range}", "allSections": "true"},
        auth=(api_key, ""),
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    sections = payload if isinstance(payload, list) else [payload]
    detail = next((s for s in sections if s.get("reportSection") == "Report Detail"), None)
    return detail["results"] if detail else []


def _derive_futures_price(rows, commodity, month_name):
    """Back out the CBOT settlement price from a basis quote: basis = cash -
    futures, so futures = cash - basis. Verified against the same day's
    published "Futures Settlements" price to the exact cent before relying on
    this (e.g. Mississippi River corn basis of 2c off a $4.6625 cash bid gives
    4.6625 - 0.02 = 4.6425, the actual Dec-26 corn settlement that day) --
    every location quoting off the same contract month backs out to the same
    settlement price, so any matching row works; average in case of noise.
    """
    matches = [
        r["price Min"] - r["basis Min"] / 100
        for r in rows
        if r.get("commodity") == commodity and month_name in (r.get("basis Min Futures Month") or "")
        and r.get("price Min") is not None and r.get("basis Min") is not None
    ]
    return sum(matches) / len(matches) if matches else None


def fetch_futures_settlements():
    """Pull today's Dec-corn / Nov-soybean CBOT settlement price, derived from
    the Illinois Grain Bids report's basis quotes via the MARS API. Illinois
    is used only because it matches the IL--Gulf basis convention used
    elsewhere in this pipeline, not because the price is Illinois-specific --
    the underlying CBOT settlement is identical nationwide.

    Unlike the "Futures" sheet in fetch_dec_nov_futures(), this report lists
    basis off every active contract month year-round, so it has a
    Dec-corn/Nov-soybean price even outside the Aug-Nov window that sheet
    tracks -- this is what actually fills in the current year before that
    window opens.
    """
    api_key = os.environ.get("MARS_API_KEY")
    if not api_key:
        raise RuntimeError("MARS_API_KEY environment variable is not set")

    # query a trailing week rather than assuming today's edition has posted --
    # covers weekends/holidays and the case where the pipeline runs before
    # today's report is published
    today = pd.Timestamp.now()
    date_range = f"{(today - pd.Timedelta(days=7)).strftime('%m/%d/%Y')}:{today.strftime('%m/%d/%Y')}"
    rows = _mars_detail_rows(date_range, api_key)
    if not rows:
        raise ValueError(f"No Illinois Grain Bids data returned for {date_range}")

    latest_date = max(rows, key=lambda r: pd.to_datetime(r["report_date"]))["report_date"]
    rows = [r for r in rows if r["report_date"] == latest_date]

    corn_dec = _derive_futures_price(rows, "Corn", "December")
    soy_nov = _derive_futures_price(rows, "Soybeans", "November")
    return pd.DataFrame([{
        "date": pd.to_datetime(rows[0]["report_date"]),
        "corn_dec_futures": corn_dec,
        "soy_nov_futures": soy_nov,
    }])


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
        nextmonth_rates, threemonth_rates = fetch_forward_barge_rates()
        _merge_and_save(NXTMONTH_CSV, nextmonth_rates, "week")
        _merge_and_save(THREEMONTH_CSV, threemonth_rates, "week")
    except Exception as e:
        print(f"Forward barge rates: ERROR — {e}")

    try:
        corn_price, soy_price, corn_spread = fetch_corn_soy_prices()
        _merge_and_save(CORN_CSV, corn_price, "date")
        _merge_and_save(SOY_CSV, soy_price, "date")
        _merge_and_save(CORN_SPREAD_CSV, corn_spread, "date")
    except Exception as e:
        print(f"Corn/soy prices: ERROR — {e}")

    try:
        futures = fetch_dec_nov_futures()
        _merge_and_save(FUTURES_CSV, futures, "date")
    except Exception as e:
        print(f"Dec/Nov futures: ERROR — {e}")

    try:
        settlement = fetch_futures_settlements()
        _merge_and_save(FUTURES_CSV, settlement, "date")
    except Exception as e:
        print(f"Dec/Nov futures (daily settlement): ERROR — {e}")


if __name__ == "__main__":
    main()

"""One-time backfill of Dec-corn / Nov-soybean futures history from the MARS
API (USDA AMS's MyMarketNews API), from as far back as the Illinois Grain
Bids report goes (Feb 2020) through today.

This is NOT part of the daily pipeline -- run it once locally:

    export MARS_API_KEY="..."
    python3 backfill_futures_history.py

It merges into the same futures_dec_nov_history.csv that fetch_market_data.py
maintains going forward, so re-running it is safe (dedupes by date).
"""
import os

import pandas as pd

from fetch_market_data import FUTURES_CSV, MARS_API_URL, _derive_futures_price, _merge_and_save
import requests

EARLIEST_DATE = pd.Timestamp("2020-02-24")  # first date this report's archive has data


def _year_chunks(start, end):
    year = start.year
    while year <= end.year:
        chunk_start = max(start, pd.Timestamp(year, 1, 1))
        chunk_end = min(end, pd.Timestamp(year, 12, 31))
        yield chunk_start, chunk_end
        year += 1


def fetch_year_detail_rows(chunk_start, chunk_end, api_key):
    date_range = f"{chunk_start.strftime('%m/%d/%Y')}:{chunk_end.strftime('%m/%d/%Y')}"
    resp = requests.get(
        MARS_API_URL,
        params={"q": f"report_begin_date={date_range}", "allSections": "true"},
        auth=(api_key, ""),
        timeout=60,
    )
    resp.raise_for_status()
    payload = resp.json()
    sections = payload if isinstance(payload, list) else [payload]
    detail = next((s for s in sections if s.get("reportSection") == "Report Detail"), None)
    return detail["results"] if detail else []


def main():
    api_key = os.environ.get("MARS_API_KEY")
    if not api_key:
        raise RuntimeError("MARS_API_KEY environment variable is not set")

    today = pd.Timestamp.now().normalize()
    all_rows = []
    for chunk_start, chunk_end in _year_chunks(EARLIEST_DATE, today):
        print(f"Fetching {chunk_start.date()} to {chunk_end.date()}...")
        rows = fetch_year_detail_rows(chunk_start, chunk_end, api_key)
        print(f"  {len(rows)} detail rows")
        all_rows.extend(rows)

    by_date = {}
    for r in all_rows:
        by_date.setdefault(r["report_date"], []).append(r)

    records = []
    for report_date, rows in by_date.items():
        corn_dec = _derive_futures_price(rows, "Corn", "December")
        soy_nov = _derive_futures_price(rows, "Soybeans", "November")
        if corn_dec is None and soy_nov is None:
            continue
        records.append({
            "date": pd.to_datetime(report_date),
            "corn_dec_futures": corn_dec,
            "soy_nov_futures": soy_nov,
        })

    backfill = pd.DataFrame(records).sort_values("date")
    print(f"\nDerived {len(backfill)} days of Dec-corn/Nov-soy prices")
    _merge_and_save(FUTURES_CSV, backfill, "date")


if __name__ == "__main__":
    main()

"""Fetch the latest WASDE report and pull the current-year US corn and
soybean production estimates out of it.

The USDA/NAL publication archive lists every monthly WASDE release; the
last ".xls" link on that page is always the newest report. That workbook
has one sheet per report page -- the corn balance sheet is on the "CORN"
sub-table of "Page 12" and the soybean balance sheet is the "SOYBEANS"
sub-table at the top of "Page 15". Both list a "Production" row in
million bushels, with the rightmost column holding the current month's
estimate.

Run from the GitHub Actions daily pipeline, alongside fetch_market_data.py.
"""
import re
import time
from pathlib import Path

import pandas as pd
import requests

PUBLICATION_URL = "https://esmis.nal.usda.gov/publication/world-agricultural-supply-and-demand-estimates"
XLS_PATH = Path("wasde_latest.xls")
OUT_CSV = Path("wasde_production_estimate.csv")

HEADERS = {"User-Agent": "Mozilla/5.0"}

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5


def _get_with_retries(url):
    """GET with retries on transient failures (5xx, timeouts, connection errors).

    USDA's site has intermittently 502'd on the daily pipeline run; 4xx
    errors are not retried since a retry won't fix a bad URL.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=30, headers=HEADERS)
            resp.raise_for_status()
            return resp
        except (requests.exceptions.HTTPError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as exc:
            is_client_error = (
                isinstance(exc, requests.exceptions.HTTPError)
                and exc.response is not None
                and exc.response.status_code < 500
            )
            if is_client_error or attempt == MAX_RETRIES:
                raise
            wait = RETRY_BACKOFF_SECONDS * attempt
            print(f"  GET {url} failed ({exc}); retrying in {wait}s "
                  f"(attempt {attempt}/{MAX_RETRIES})")
            time.sleep(wait)


def _latest_release_url():
    resp = _get_with_retries(PUBLICATION_URL)
    hrefs = re.findall(r'href="([^"]*release-files[^"]*\.xls)"', resp.text)
    if not hrefs:
        raise ValueError("No WASDE .xls release links found on publication page")
    return "https://esmis.nal.usda.gov" + hrefs[-1]


def _download_latest():
    url = _latest_release_url()
    resp = _get_with_retries(url)
    XLS_PATH.write_bytes(resp.content)


def _production_estimate(sheet_name, section_label):
    df = pd.read_excel(XLS_PATH, sheet_name=sheet_name, header=None)

    section_rows = df.index[df[0].astype(str).str.strip() == section_label]
    if len(section_rows) == 0:
        raise ValueError(f"Section '{section_label}' not found on {sheet_name}")
    start = section_rows[0]

    marketing_year = str(df.iloc[start, 4]).replace("Proj.", "").replace("Est.", "").strip()

    # "Production" appears once per commodity sub-table; restrict the search
    # to the rows just below this section's header so we don't pick up a
    # different commodity's production row further down the same sheet
    sub = df.iloc[start:start + 25]
    prod_rows = sub.index[sub[0].astype(str).str.strip() == "Production"]
    if len(prod_rows) == 0:
        raise ValueError(f"No Production row found under '{section_label}' on {sheet_name}")

    production_million_bu = float(df.iloc[prod_rows[0], 4])
    return marketing_year, production_million_bu


def main():
    _download_latest()

    report_label = str(pd.read_excel(XLS_PATH, sheet_name="Page 12", header=None).iloc[0, 0]).strip()

    corn_year, corn_production = _production_estimate("Page 12", "CORN")
    soy_year, soy_production = _production_estimate("Page 15", "SOYBEANS")

    assert corn_year == soy_year, f"Marketing year mismatch: corn={corn_year} soy={soy_year}"

    out = pd.DataFrame([{
        "report_label": report_label,
        "marketing_year": corn_year,
        "corn_production_million_bu": corn_production,
        "soybean_production_million_bu": soy_production,
    }])
    out.to_csv(OUT_CSV, index=False)
    print(f"Saved {report_label} estimate ({corn_year}): "
          f"corn={corn_production} soy={soy_production} million bu -> {OUT_CSV}")


if __name__ == "__main__":
    main()

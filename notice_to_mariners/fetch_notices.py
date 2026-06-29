import re
import io
import argparse
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import requests
import pandas as pd

# --- CONFIG -----------------
REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

LAST_RUN_FILE = SCRIPT_DIR / "last_run_date.csv"
OUTPUT_FILE = DATA_DIR / "relevant_notices.csv"

# columns written directly into notices_<year>.xlsx - new rows land with confirmed blank
# so they don't appear on the map until Molly opens the file and sets confirmed=Y
NOTICE_COLUMNS = [
    "date_published", "cancelled_date", "replaced_date", "active?", "message_id",
    "category", "other_notes", "dredge", "river", "mm_low", "mm_high", "lat", "lon",
    "date_start", "date_end", "northbound", "southbound", "location_details",
    "instructions", "full_memo", "confirmed",
]

# the categorize() regexes flag everything that mentions these words, but the xlsx
# schema wants one bucket per notice - dredging/shoaling/draft take priority since
# they're the most specific; anything else (cancellation-only matches, or no match at
# all, e.g. tropical storm advisories) falls back to "other"
CATEGORY_PRIORITY = [("dredging", "dredging"), ("shoaling", "shoaling"), ("draft restriction", "draft")]

BASE_URL = "https://www.navcen.uscg.gov"
SEARCH_PATH = "/broadcast-notice-to-mariners-search-results"
EXPORT_PATH = "/broadcast-notice-to-mariners-message-export-csv"
ITEMS_PER_PAGE = 50  # max accepted by the site's "items per page" select besides "All", which 500s
GUID_CHUNK_SIZE = 50
HEADERS = {"User-Agent": "Mozilla/5.0"}

# search-results rows hide each notice's internal CMS id in a bare <div class="bnm_message" id="...">,
# which custom.js normally reads client-side to build checkboxes - no JS rendering needed to scrape it
GUID_RE = re.compile(r'class="bnm_message" id="(\d+)"')
NEXT_PAGE_RE = re.compile(r'title="Go to next page"')

CATEGORY_PATTERNS = {
    "dredging": re.compile(r"dredg", re.IGNORECASE),
    "shoaling": re.compile(r"shoal", re.IGNORECASE),
    "draft restriction": re.compile(r"\bdrafts?\b", re.IGNORECASE),
    # a cancellation notice often just says "BNM 0015-26 IS CANCELLED" without
    # restating "dredging"/"shoaling"/"draft" - catch those too so they aren't
    # silently dropped by the categories != "" filter below
    "cancellation": re.compile(r"\bcancel", re.IGNORECASE),
}

# mile markers show up in notice text as "MM 348.7 ARK", "NM 125.0", "MILE 44.4",
# or a bare river abbreviation immediately before a number ("ARK 348.7", "LMR 519.8").
# Numbers are capped at 3 integer digits (these waterways top out under 1000) so that
# clause numbering glued onto a marker with no space/period ("MM 5181. COAST GUARD...",
# i.e. "MM 518" + "1.") doesn't get swallowed into the marker itself. The trailing river
# abbreviation is a fixed whitelist, not a generic [A-Z]+ catch-all, so ordinary words
# right after a number ("MM 483 shall", "MM 869 to") don't get mistaken for one.
RIVER_ABBR = r"(?:LMR|UMR|ARK|RED|ATCH|WHITE|AHP)"
MILE_MARKER_PATTERNS = [
    re.compile(rf"\b(?:MM|NM|MILE(?:\s*MARKER)?)\s*\.?\d{{1,3}}(?:\.\d)?(?:\s*-\s*\d{{1,3}}(?:\.\d)?)?(?:\s*{RIVER_ABBR}\b)?", re.IGNORECASE),
    re.compile(rf"\b{RIVER_ABBR}\s+\d{{1,3}}(?:\.\d)?(?:\s*-\s*\d{{1,3}}(?:\.\d)?)?", re.IGNORECASE),
]

# maps a river abbreviation found in/near a mile marker to the RIVER_NAME value used in
# usace_river_mile_markers.csv, so app.py can join straight onto it for lat/lon
RIVER_NAME_MAP = {
    "LMR": "MISSISSIPPI-LO", "AHP": "MISSISSIPPI-LO", "UMR": "MISSISSIPPI-UP",
    "ARK": "ARKANSAS", "RED": "RED", "WHITE": "WHITE", "ATCH": "ATCHAFALAY",
}
# fallback when a marker has no river abbreviation of its own (e.g. bare "MM 319") -
# inferred from a river name mentioned anywhere else in the notice body, defaulting to
# the Lower Mississippi since that's the primary river for this district/sector
DEFAULT_RIVER_FROM_BODY = [
    (re.compile(r"ARKANSAS RIVER", re.IGNORECASE), "ARK"),
    (re.compile(r"RED RIVER", re.IGNORECASE), "RED"),
    (re.compile(r"WHITE RIVER", re.IGNORECASE), "WHITE"),
    (re.compile(r"ATCHAFALAYA", re.IGNORECASE), "ATCH"),
]
NUMBER_RE = re.compile(r"\d{1,3}(?:\.\d)?")
RIVER_TOKEN_RE = re.compile(RIVER_ABBR, re.IGNORECASE)
# ----------------------------


def get_guids_for_daterange(district, sector, start, end):
    guids = []
    page = 0
    while True:
        params = {
            "district": district,
            "sector": sector,
            "date-range": f"{start}--{end}",
            "items_per_page": ITEMS_PER_PAGE,
            "page": page,
        }
        r = requests.get(BASE_URL + SEARCH_PATH, params=params, headers=HEADERS)
        r.raise_for_status()
        guids.extend(GUID_RE.findall(r.text))
        if not NEXT_PAGE_RE.search(r.text):
            break
        page += 1
    return guids


def get_notices(guids):
    chunks = []
    for i in range(0, len(guids), GUID_CHUNK_SIZE):
        chunk_guids = guids[i:i + GUID_CHUNK_SIZE]
        params = {"guid": "+".join(chunk_guids), "format": "csv"}
        r = requests.get(BASE_URL + EXPORT_PATH, params=params, headers=HEADERS)
        r.raise_for_status()
        chunks.append(pd.read_csv(io.StringIO(r.text)))
    return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()


def categorize(body):
    matches = [name for name, pattern in CATEGORY_PATTERNS.items() if pattern.search(str(body))]
    return ";".join(matches)


def extract_mile_markers(body):
    body = str(body)
    found = []
    for pattern in MILE_MARKER_PATTERNS:
        found.extend(m.group(0).strip() for m in pattern.finditer(body))
    seen = set()
    unique = []
    for m in found:
        key = m.upper()
        if key not in seen:
            seen.add(key)
            unique.append(m)
    return ";".join(unique)


def resolve_river_mile(body, mile_markers):
    """Collapse the raw mile-marker matches for one notice into a single
    (river_name, river_abbr, mile_min, mile_max) location, picking whichever river
    abbreviation shows up on the most matches (multi-river notices are rare and usually
    noise)."""
    if not mile_markers:
        return None, None, None, None

    default_abbr = "LMR"
    for pattern, abbr in DEFAULT_RIVER_FROM_BODY:
        if pattern.search(body):
            default_abbr = abbr
            break

    bounds = {}
    counts = {}
    for raw in mile_markers.split(";"):
        numbers = [float(n) for n in NUMBER_RE.findall(raw)]
        if not numbers:
            continue
        river_match = RIVER_TOKEN_RE.search(raw)
        abbr = river_match.group(0).upper() if river_match else default_abbr
        lo, hi = bounds.get(abbr, (min(numbers), max(numbers)))
        bounds[abbr] = (min(lo, *numbers), max(hi, *numbers))
        counts[abbr] = counts.get(abbr, 0) + 1

    if not bounds:
        return None, None, None, None
    abbr = max(counts, key=counts.get)
    mile_min, mile_max = bounds[abbr]
    return RIVER_NAME_MAP.get(abbr, "MISSISSIPPI-LO"), abbr, mile_min, mile_max


def extract_category(categories):
    """Collapse the (possibly multi-valued) categories field into the single
    dredging/shoaling/draft/other bucket the notices_<year>.xlsx schema expects."""
    cats = str(categories)
    for needle, label in CATEGORY_PRIORITY:
        if needle in cats:
            return label
    return "other"


def build_notice_rows(matched):
    """Best-effort draft rows for every newly-fetched notice. Auto-extracted fields
    (dates/IDs/category/river/mile markers/raw text) are filled in; the rest
    (dredge vessel name, date_start/date_end, northbound/southbound, lat/lon, active?)
    are left blank for Molly to fill in. confirmed is blank so rows don't appear on the
    map until she sets it to Y."""
    rows = pd.DataFrame(index=matched.index)
    rows["date_published"] = pd.to_datetime(matched["date_time_published_gmt"])
    rows["cancelled_date"] = pd.NaT
    rows["replaced_date"] = pd.NaT
    rows["active?"] = np.nan
    rows["message_id"] = matched["message_id"]
    rows["category"] = matched["categories"].apply(extract_category)
    rows["other_notes"] = np.nan
    rows["dredge"] = np.nan
    rows["river"] = matched["river_abbr"]
    rows["mm_low"] = matched["mile_marker_min"]
    rows["mm_high"] = matched["mile_marker_max"]
    rows["lat"] = np.nan
    rows["lon"] = np.nan
    rows["date_start"] = pd.NaT
    rows["date_end"] = pd.NaT
    rows["northbound"] = np.nan
    rows["southbound"] = np.nan
    rows["location_details"] = matched["geographic_area"]
    rows["instructions"] = np.nan
    rows["full_memo"] = matched["notes"]
    rows["confirmed"] = np.nan
    return rows[NOTICE_COLUMNS]


def print_notification_summary(matched, new_rows):
    """Console 'notification' of every new notice found this run, Lower Mississippi
    sector notices called out separately since those are the ones Molly most wants to
    see, with everything else (mostly District 8 Gulf-wide advisories) listed after."""
    if matched.empty:
        return
    # new_rows shares matched's index (build_notice_rows builds it row-for-row from
    # matched) - join on that, not message_id, since NAVCEN sometimes reuses the same
    # message_id for a notice's later cancellation
    category_by_idx = new_rows["category"]

    def _print_rows(df):
        for idx, row in df.iterrows():
            snippet = str(row["notes"]).strip().replace("\n", " ")[:150]
            print(f"  [{row['message_id']}] {row['date_time_published_gmt']} "
                  f"({category_by_idx.loc[idx]}) - {row['geographic_area']}")
            print(f"      {snippet}...")

    lmr = matched[matched["originator"] == "Lower Mississippi"]
    other = matched[matched["originator"] != "Lower Mississippi"]

    print(f"\n=== {len(lmr)} new Lower Mississippi notice(s) ===")
    _print_rows(lmr)
    if not other.empty:
        print(f"\n=== {len(other)} other new notice(s) (District 8 / Gulf-wide, etc.) ===")
        _print_rows(other)
    print(f"\n{len(new_rows)} draft row(s) added to notices_<year>.xlsx — open the file, "
          "fill in/fix fields, set confirmed=Y to show on the map.\n")


def main():
    parser = argparse.ArgumentParser(description="Pull dredging/shoaling/draft-restriction notices from the USCG Navigation Center")
    parser.add_argument("--district", default="8", help="NAVCEN district code, e.g. 8 for Lower Mississippi")
    parser.add_argument("--sector", default="0 26", help="NAVCEN sector code(s), space-separated")
    parser.add_argument("--start", help="YYYY-MM-DD, defaults to the day after the last run")
    parser.add_argument("--end", help="YYYY-MM-DD, defaults to today")
    args = parser.parse_args()

    end = args.end or date.today().isoformat()
    if args.start:
        start = args.start
    elif LAST_RUN_FILE.exists():
        start = pd.read_csv(LAST_RUN_FILE)["date"].iloc[0]
    else:
        start = (date.today() - timedelta(days=30)).isoformat()

    print(f"Fetching notices for district {args.district}, sector {args.sector}, {start} to {end}")
    guids = get_guids_for_daterange(args.district, args.sector, start, end)
    print(f"Found {len(guids)} notices in range")

    if guids:
        notices = get_notices(guids)
        notices["categories"] = notices["Message_Body"].apply(categorize)
        notices["mile_markers"] = notices["Message_Body"].apply(extract_mile_markers)
        # every notice in range gets logged - not just ones the dredging/shoaling/draft
        # regexes catch - so nothing gets silently dropped before Molly gets a chance to
        # review it
        matched = notices.copy()
        located = matched.apply(lambda row: resolve_river_mile(row["Message_Body"], row["mile_markers"]), axis=1)
        matched[["river_name", "river_abbr", "mile_marker_min", "mile_marker_max"]] = pd.DataFrame(located.tolist(), index=matched.index)
        matched = matched.rename(columns={
            "Date_Time_Published_GMT": "date_time_published_gmt",
            "Message_ID": "message_id",
            "Originator": "originator",
            "Criticality": "criticality",
            "Geographic_Area": "geographic_area",
            "Message_Body": "notes",
        })
        out_cols = ["date_time_published_gmt", "message_id", "categories", "mile_markers",
                    "river_name", "river_abbr", "mile_marker_min", "mile_marker_max",
                    "geographic_area", "originator", "criticality", "notes"]
        matched = matched[out_cols]

        if OUTPUT_FILE.exists():
            existing = pd.read_csv(OUTPUT_FILE)
            combined = pd.concat([existing, matched], ignore_index=True)
            combined = combined.drop_duplicates(subset=["message_id", "date_time_published_gmt"])
        else:
            combined = matched
        combined.to_csv(OUTPUT_FILE, index=False)
        print(f"{len(matched)} new notices found; {len(combined)} total rows now in {OUTPUT_FILE}")

        new_rows = build_notice_rows(matched)
        new_rows["date_published"] = pd.to_datetime(new_rows["date_published"])
        for year, year_rows in new_rows.groupby(new_rows["date_published"].dt.year):
            target = REPO_ROOT / f"notices_{year}.xlsx"
            if target.exists():
                existing_xl = pd.read_excel(target)
                existing_xl["date_published"] = pd.to_datetime(existing_xl["date_published"])
                combined_xl = pd.concat([existing_xl, year_rows], ignore_index=True)
                # message_id alone isn't a safe dedup key - NAVCEN sometimes reuses a
                # notice's message_id for its own later cancellation - so key on the pair
                combined_xl = combined_xl.drop_duplicates(subset=["message_id", "date_published"], keep="first")
            else:
                combined_xl = year_rows
            combined_xl = combined_xl.sort_values("date_published", ascending=False)
            combined_xl.to_excel(target, index=False)
            print(f"  {len(year_rows)} new row(s) added to {target} ({len(combined_xl)} total).")

        print_notification_summary(matched, new_rows)
    else:
        print("Nothing to process.")

    pd.DataFrame({"date": [end]}).to_csv(LAST_RUN_FILE, index=False)


if __name__ == "__main__":
    main()

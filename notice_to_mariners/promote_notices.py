"""
Promotes reviewed rows out of data/pending_notices.xlsx into the root notices_<year>.xlsx
workbook(s) that app.py reads for the map. Run this after fetch_notices.py and after
opening pending_notices.xlsx to fill in/fix fields and mark each row's "confirmed"
column:
  Y - add to the map, move the row into notices_<year>.xlsx
  N - not relevant, discard the row
  (blank) - not reviewed yet, leave it in the pending file
"""
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
PENDING_FILE = SCRIPT_DIR / "data" / "pending_notices.xlsx"

NOTICE_COLUMNS = [
    "date_published", "cancelled_date", "replaced_date", "active?", "message_id",
    "category", "other_notes", "dredge", "river", "mm_low", "mm_high", "lat", "lon",
    "date_start", "date_end", "northbound", "southbound", "location_details",
    "instructions", "full_memo",
]


def main():
    if not PENDING_FILE.exists():
        print(f"No pending notices file found at {PENDING_FILE}.")
        return

    pending = pd.read_excel(PENDING_FILE)
    if pending.empty:
        print("Pending notices file is empty - nothing to promote.")
        return

    confirmed = pending["confirmed"].astype(str).str.strip().str.upper()
    to_promote = pending[confirmed == "Y"].copy()
    to_discard = pending[confirmed == "N"]
    still_pending = pending[~confirmed.isin(["Y", "N"])]

    if to_discard.shape[0]:
        print(f"Discarding {to_discard.shape[0]} row(s) marked 'N'.")

    if to_promote.empty:
        print("No rows marked 'Y' to promote.")
    else:
        to_promote["date_published"] = pd.to_datetime(to_promote["date_published"])
        to_promote = to_promote[NOTICE_COLUMNS]
        for year, year_rows in to_promote.groupby(to_promote["date_published"].dt.year):
            target = REPO_ROOT / f"notices_{year}.xlsx"
            if target.exists():
                existing = pd.read_excel(target)
                existing["date_published"] = pd.to_datetime(existing["date_published"])
                # a single notice can legitimately produce multiple rows sharing the same
                # message_id/date_published (one per mile-range segment), so only collapse
                # exact full-row duplicates - guards a rerun of promote_notices.py without
                # discarding intentional segment splits
                combined = pd.concat([existing, year_rows], ignore_index=True).drop_duplicates()
            else:
                combined = year_rows
            combined = combined.sort_values("date_published", ascending=False)
            combined.to_excel(target, index=False)
            print(f"Promoted {len(year_rows)} row(s) to {target} ({len(combined)} total rows).")

    still_pending.to_excel(PENDING_FILE, index=False)
    print(f"{len(still_pending)} row(s) still awaiting review in {PENDING_FILE}.")


if __name__ == "__main__":
    main()

"""
One-off backfill: pull every 2021-2025 Broadcast Notice to Mariners that mentions
shoaling on the Mississippi River, for Molly to manually confirm.

Separate from the regular fetch_notices.py -> relevant_notices.csv / notices_<year>.xlsx
pipeline (which only tracks 2026 onward) -- this writes its own simple CSV with just the
fields Molly asked for: date, location description, mile marker(s), the full memo text,
and a blank confirmed column for her to mark Y/N per row herself.

Reuses fetch_notices.py's scraping/parsing helpers (NAVCEN search+export, mile-marker
regex, river resolution) rather than duplicating them -- importing it is safe since all
of its top-level code besides regex compiles/constants is guarded under __main__.
"""
from pathlib import Path

import pandas as pd

import fetch_notices as fn

OUTPUT_FILE = fn.DATA_DIR / "shoaling_notices_2021_2025.csv"

START = "2021-01-01"
END = "2025-12-31"
DISTRICT = "8"
SECTOR = "0 26"  # Lower Mississippi sector, same default as fetch_notices.py

MISSISSIPPI_RIVERS = {"MISSISSIPPI-LO", "MISSISSIPPI-UP"}


def is_mississippi(row):
    if row["river_name"] in MISSISSIPPI_RIVERS:
        return True
    # notices with no extractable mile marker still often name the river in
    # the free-text geographic area (e.g. "LOWER MISSISSIPPI RIVER")
    return "MISSISSIPPI RIVER" in str(row["geographic_area"]).upper()


def main():
    print(f"Fetching notices for district {DISTRICT}, sector {SECTOR}, {START} to {END}")
    guids = fn.get_guids_for_daterange(DISTRICT, SECTOR, START, END)
    print(f"Found {len(guids)} notices in range")
    if not guids:
        print("Nothing to process.")
        return

    notices = fn.get_notices(guids)
    notices["categories"] = notices["Message_Body"].apply(fn.categorize)
    notices["mile_markers"] = notices["Message_Body"].apply(fn.extract_mile_markers)

    shoaling = notices[notices["categories"].str.contains("shoaling")].copy()
    print(f"{len(shoaling)} of {len(notices)} notices mention shoaling")

    located = shoaling.apply(
        lambda row: fn.resolve_river_mile(row["Message_Body"], row["mile_markers"]), axis=1
    )
    shoaling[["river_name", "river_abbr", "mile_marker_min", "mile_marker_max"]] = pd.DataFrame(
        located.tolist(), index=shoaling.index
    )
    shoaling = shoaling.rename(columns={"Geographic_Area": "geographic_area"})

    msr = shoaling[shoaling.apply(is_mississippi, axis=1)].copy()
    print(f"{len(msr)} of {len(shoaling)} shoaling notices are on the Mississippi River")

    out = pd.DataFrame({
        "date": pd.to_datetime(msr["Date_Time_Published_GMT"]),
        "mile_markers": msr["mile_markers"],
        "location_description": msr["geographic_area"],
        "memo": msr["Message_Body"],
        "confirmed": "",
    })
    out = out.drop_duplicates(subset=["date", "memo"]).sort_values("date")
    out.to_csv(OUTPUT_FILE, index=False)
    print(f"Wrote {len(out)} shoaling notice(s) to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()

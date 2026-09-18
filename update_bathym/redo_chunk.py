"""
Redo (delete) one chunk of already-downloaded bathymetry surveys so the
pipeline treats them as new again on the next run.

Chunks are defined in redo_chunks_manifest.csv (built once by whoever set up
this redo -- see that file for how the 2021-2025 survey set was split into
15 roughly count-balanced, chronological chunks: total surveys per year / 3).

For a given chunk this removes the survey from every place a pipeline stage
checks to decide "already done":
  - lm_ids_done.csv / um_ids_done.csv      (1_check_for_surveys.py)
  - data/SurveyPointLayers/{id}_SurveyPoint.gpkg (+ {id}_gdb/)  (2_read_in_surveys.py)
  - data/NAVD88Files, ActualDepthFiles, OtherDatumFiles/{id}*.gpkg (3_process_surveys.py)
  - bathym_fixed.csv rows for that survey                        (4_compute_thresh_depth.py)
  - data/NavigableWidth/{id}_transects.geojson                    (7_compute_navigable_width.py)
  - navigable_width_profile.csv rows for that survey              (7_compute_navigable_width.py)
  - data/WidthByStage/{id}_width_by_stage.csv                     (8_compute_width_by_stage.py)
  - data/WidthByStage/{id}_navigable_path.geojson                 (8_compute_width_by_stage.py)
  - data/DepthPolygons/{id}_depth_polygons.geojson                (9_make_depth_polygons.py)

Defaults to a dry run (prints what it would delete). Pass --confirm to
actually delete. After deleting a chunk, redownload it by running in order:
    python 1_check_for_surveys.py
    python 2_read_in_surveys.py
    python 3_process_surveys.py
    python 4_compute_thresh_depth.py
    python 6_review_surveys.py   (manual sign-flip check in the browser)
    python 7_compute_navigable_width.py
    python 8_compute_width_by_stage.py
    python 9_make_depth_polygons.py
"""

import argparse
import shutil
from pathlib import Path

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DATA_DIR = SCRIPT_DIR / "data"

MANIFEST_FILE = SCRIPT_DIR / "redo_chunks_manifest.csv"
LM_DONE_FILE = SCRIPT_DIR / "lm_ids_done.csv"
UM_DONE_FILE = SCRIPT_DIR / "um_ids_done.csv"
BATHYM_FIXED_FILE = REPO_ROOT / "bathym_fixed.csv"
PROFILE_FILE = REPO_ROOT / "navigable_width_profile.csv"

SURVEYPOINT_DIR = DATA_DIR / "SurveyPointLayers"
NAVD88_DIR = DATA_DIR / "NAVD88Files"
ACTUALDEPTH_DIR = DATA_DIR / "ActualDepthFiles"
OTHER_DIR = DATA_DIR / "OtherDatumFiles"
DEPTHPOLY_DIR = DATA_DIR / "DepthPolygons"
NAVWIDTH_DIR = DATA_DIR / "NavigableWidth"
WIDTHBYSTAGE_DIR = DATA_DIR / "WidthByStage"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk", help="Chunk label, e.g. 2021-1")
    parser.add_argument("--list", action="store_true", help="List all chunk labels with survey counts and exit")
    parser.add_argument("--confirm", action="store_true", help="Actually delete (default is dry run)")
    args = parser.parse_args()

    manifest = pd.read_csv(MANIFEST_FILE)

    if args.list:
        counts = manifest.groupby("chunk").agg(n=("survey_id", "size"), start=("date", "min"), end=("date", "max"))
        print(counts.to_string())
        return

    if not args.chunk:
        parser.error("--chunk is required (use --list to see options)")

    chunk_rows = manifest[manifest["chunk"] == args.chunk]
    if chunk_rows.empty:
        parser.error(f"No such chunk '{args.chunk}'. Use --list to see valid chunk labels.")

    target_ids = set(chunk_rows["survey_id"])
    print(f"Chunk {args.chunk}: {len(target_ids)} surveys, {chunk_rows['date'].min()} to {chunk_rows['date'].max()}")
    mode = "DELETING" if args.confirm else "DRY RUN (pass --confirm to actually delete)"
    print(f"Mode: {mode}\n")

    # ---- 1. done-lists ----
    for label, path in [("lm_ids_done.csv", LM_DONE_FILE), ("um_ids_done.csv", UM_DONE_FILE)]:
        if not path.exists():
            continue
        df = pd.read_csv(path)
        hit = df["ID"].isin(target_ids)
        print(f"{label}: {hit.sum()} rows to remove (of {len(df)})")
        if args.confirm and hit.any():
            df[~hit].to_csv(path, index=False)

    # ---- 2. bathym_fixed.csv ----
    if BATHYM_FIXED_FILE.exists():
        bf = pd.read_csv(BATHYM_FIXED_FILE)
        hit = bf["file"].apply(lambda f: any(str(f).startswith(sid) for sid in target_ids))
        print(f"bathym_fixed.csv: {hit.sum()} rows to remove (of {len(bf)})")
        if args.confirm and hit.any():
            bf[~hit].to_csv(BATHYM_FIXED_FILE, index=False)
    else:
        print("bathym_fixed.csv: not found, skipping")

    # ---- 3. raw/intermediate gpkg copies ----
    def sweep_dir(d: Path, note: str):
        if not d.exists():
            print(f"{note}: dir not found, skipping")
            return
        matched_files = [p for p in d.glob("*.gpkg") if any(p.name.startswith(sid) for sid in target_ids)]
        matched_dirs = [p for p in d.glob("*_gdb") if p.is_dir() and any(p.name.startswith(sid) for sid in target_ids)]
        print(f"{note}: {len(matched_files)} file(s), {len(matched_dirs)} dir(s) to remove")
        if args.confirm:
            for p in matched_files:
                p.unlink(missing_ok=True)
            for p in matched_dirs:
                shutil.rmtree(p)

    sweep_dir(SURVEYPOINT_DIR, "SurveyPointLayers")
    sweep_dir(NAVD88_DIR, "NAVD88Files")
    sweep_dir(ACTUALDEPTH_DIR, "ActualDepthFiles")
    sweep_dir(OTHER_DIR, "OtherDatumFiles")

    # ---- 4. depth polygons ----
    if DEPTHPOLY_DIR.exists():
        matched = [p for p in DEPTHPOLY_DIR.glob("*_depth_polygons.geojson") if any(p.name.startswith(sid) for sid in target_ids)]
        print(f"DepthPolygons: {len(matched)} file(s) to remove")
        if args.confirm:
            for p in matched:
                p.unlink(missing_ok=True)
    else:
        print("DepthPolygons: dir not found, skipping")

    # ---- 5. navigable-width outputs (7_compute_navigable_width.py) ----
    if NAVWIDTH_DIR.exists():
        matched = [p for p in NAVWIDTH_DIR.glob("*_transects.geojson") if any(p.name.startswith(sid) for sid in target_ids)]
        print(f"NavigableWidth: {len(matched)} file(s) to remove")
        if args.confirm:
            for p in matched:
                p.unlink(missing_ok=True)
    else:
        print("NavigableWidth: dir not found, skipping")

    if PROFILE_FILE.exists():
        prof = pd.read_csv(PROFILE_FILE)
        hit = prof["survey_id"].isin(target_ids)
        print(f"navigable_width_profile.csv: {hit.sum()} rows to remove (of {len(prof)})")
        if args.confirm and hit.any():
            prof[~hit].to_csv(PROFILE_FILE, index=False)
    else:
        print("navigable_width_profile.csv: not found, skipping")

    # ---- 6. width-by-stage outputs (8_compute_width_by_stage.py) ----
    if WIDTHBYSTAGE_DIR.exists():
        matched = [p for p in WIDTHBYSTAGE_DIR.glob("*_width_by_stage.csv") if any(p.name.startswith(sid) for sid in target_ids)]
        matched += [p for p in WIDTHBYSTAGE_DIR.glob("*_navigable_path.geojson") if any(p.name.startswith(sid) for sid in target_ids)]
        print(f"WidthByStage: {len(matched)} file(s) to remove")
        if args.confirm:
            for p in matched:
                p.unlink(missing_ok=True)
    else:
        print("WidthByStage: dir not found, skipping")

    if not args.confirm:
        print("\nDry run only -- nothing was deleted. Re-run with --confirm to apply.")
    else:
        print(f"\nChunk {args.chunk} deleted. Next: run 1_check_for_surveys.py -> 2_read_in_surveys.py -> "
              f"3_process_surveys.py -> 4_compute_thresh_depth.py -> 6_review_surveys.py (manual) -> "
              f"7_compute_navigable_width.py -> 8_compute_width_by_stage.py -> 9_make_depth_polygons.py.")


if __name__ == "__main__":
    main()

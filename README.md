# Barge Transportation Dashboard

An interactive dashboard tracking transportation conditions on the Mississippi River system, built to help users understand how low water and other disruptions affect grain barge transportation.

The dashboard combines riverbed surveys, dredging activity, river stage, barge freight rates, grain prices, and grain production/demand indicators into a single map-based interface. Underlying data refreshes daily via an automated pipeline (see `.github/workflows/daily_pipeline.yml`) and is pulled from public sources including the US Army Corps of Engineers, USGS, NOAA, and USDA (see the in-app About page for the full list of data sources).

Built with [Dash](https://dash.plotly.com/) (Python/Plotly) and deployed with Gunicorn.

## Running locally

```bash
pip install -r requirements.txt
python app.py
```

The app serves on `http://localhost:8050` by default.

## Project structure

- `app.py` — the Dash application (layout, callbacks, charts)
- `fetch_*.py`, `backfill_futures_history.py` — data-pipeline scripts run daily by GitHub Actions to refresh river stage, barge rates, grain prices, WASDE estimates, and NASS production history
- `update_bathym/` — bathymetry survey pipeline (eHydro survey ingestion, datum conversion, depth polygon generation)
- `notice_to_mariners/` — USCG Local Notice to Mariners scraper
- `assets/` — CSS, JS, and image assets for the Dash frontend

## Contact

Questions or feedback: [malcor@unc.edu](mailto:malcor@unc.edu)

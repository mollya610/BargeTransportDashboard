"""
Local QA tool: resolve possible duplicate surveys flagged by compute_bathym_stats.py
(same location + date, republished by USACE's eHydro under a second job ID --
see the DUPLICATE-SURVEY DETECTION block there).

Run after review_surveys.py, once both copies in a duplicate pair have been
individually risk-reviewed and confirmed=="yes" -- there's nothing to compare
until both sides have a stored depth/at_risk value. (A pair where one side is
still pending or was already rejected during the main review doesn't need a
decision here; it's skipped automatically.)

For each open duplicate group, shows every member side by side: file, date,
datum, stored depth, at_risk, and a depth-colored point map (reads the raw
gpkg directly, so this must run before make_depth_polygons.py deletes it --
i.e. before the next CI pipeline run, same as review_surveys.py). Molly picks
one to keep (rejects the rest -- same confirmed="rejected" convention as the
main review app, plus deletes the rejected copies' raw gpkg/depth-polygon
files if present) or "Keep both" if they turn out not to be real duplicates
(same survey job republished doesn't always mean same content -- this just
marks the group resolved without touching confirmed on either side).

Local-only tool, never deployed -- own port, separate from app.py and
review_surveys.py.
"""

from pathlib import Path

import geopandas as gpd
import pandas as pd
import plotly.graph_objects as go
from dash import ALL, Dash, dcc, html, Input, Output, State, ctx, no_update

# ---------------- CONFIG ----------------
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
NAVD88_DIR = SCRIPT_DIR / "data" / "NAVD88Files"
DEPTH_POLY_DIR = SCRIPT_DIR / "data" / "DepthPolygons"
BATHYM_FIXED_FILE = REPO_ROOT / "bathym_fixed.csv"

# Same depth bins/colors as review_surveys.py / make_depth_polygons.py / app.py's
# DEPTH_POLY_COLORS -- duplicated rather than imported since review_surveys.py runs
# top-level Dash-app setup on import (its own comment explains why it duplicates
# these too instead of importing from make_depth_polygons.py). Keep in sync by hand.
DEPTH_BINS = [
    (20,   None,  ">20 ft"),
    (17.5, 20,    "17.5-20 ft"),
    (15,   17.5,  "15-17.5 ft"),
    (14,   15,    "14-15 ft"),
    (13,   14,    "13-14 ft"),
    (12,   13,    "12-13 ft"),
    (11,   12,    "11-12 ft"),
    (10,   11,    "10-11 ft"),
    (9,    10,    "9-10 ft"),
    (8,    9,     "8-9 ft"),
    (7,    8,     "7-8 ft"),
    (6,    7,     "6-7 ft"),
    (5,    6,     "5-6 ft"),
    (None, 5,     "<5 ft"),
]
BIN_ORDER = {label: i for i, (_, _, label) in enumerate(DEPTH_BINS)}
DEPTH_POLY_COLORS = {
    ">20 ft":      "#084594",
    "17.5-20 ft":  "#2171b5",
    "15-17.5 ft":  "#4292c6",
    "14-15 ft":    "#74c476",
    "13-14 ft":    "#a1d99b",
    "12-13 ft":    "#fee08b",
    "11-12 ft":    "#fdae61",
    "10-11 ft":    "#f46d43",
    "9-10 ft":     "#d73027",
    "8-9 ft":      "#a50026",
    "7-8 ft":      "#7b0000",
    "6-7 ft":      "#9e0142",
    "5-6 ft":      "#6a0136",
    "<5 ft":       "#3d0026",
}


def assign_depth_bin(depth):
    for lo, hi, label in DEPTH_BINS:
        if lo is not None and depth < lo:
            continue
        if hi is not None and depth >= hi:
            continue
        return label
    return None


def load_bathym_fixed():
    return pd.read_csv(BATHYM_FIXED_FILE)


def _ensure_resolved_column(df):
    """duplicate_resolved is a blank-until-decided tracking column, same idiom as
    at_risk/problem_lon/problem_lat in review_surveys.py -- an all-blank column
    round-trips through CSV as NaN/float64, which then rejects a later string
    assignment, so force object dtype (and create the column if missing)."""
    if "duplicate_resolved" not in df.columns:
        df["duplicate_resolved"] = None
    df["duplicate_resolved"] = df["duplicate_resolved"].astype(object)
    return df


def build_duplicate_groups(df):
    """Union-find over the (asymmetric) duplicate_of links -- compute_bathym_stats.py
    only writes duplicate_of on whichever copy it processed later, pointing back at
    the earlier one(s), so this reconstructs full groups (usually pairs, occasionally
    3+ if eHydro republished the same survey more than twice)."""
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for _, row in df.iterrows():
        dup_of = row.get("duplicate_of")
        if pd.isna(dup_of) or not str(dup_of).strip():
            continue
        a = row["file"]
        parent.setdefault(a, a)
        for b in str(dup_of).split(","):
            b = b.strip()
            if b:
                parent.setdefault(b, b)
                union(a, b)

    groups = {}
    for f in parent:
        groups.setdefault(find(f), []).append(f)
    return list(groups.values())


def get_pending_groups():
    """Duplicate groups still needing a decision: not already marked resolved, and
    with at least 2 members confirmed=="yes" (fewer than that means the group is
    already effectively resolved -- either nothing's been approved yet, or a
    reviewer already rejected all-but-one during the main review pass)."""
    df = load_bathym_fixed()
    if df.empty:
        return []
    groups = build_duplicate_groups(df)
    by_file = df.set_index("file")
    pending = []
    for group in groups:
        sub = by_file.loc[by_file.index.intersection(group)]
        if "duplicate_resolved" in sub.columns and (sub["duplicate_resolved"].fillna("") == "yes").any():
            continue
        if (sub["confirmed"] == "yes").sum() < 2:
            continue
        ordered = sorted(group, key=lambda f: str(sub.loc[f, "date"]) if f in sub.index else "")
        pending.append(ordered)
    return pending


def build_mini_figure(file, row):
    gdf = gpd.read_file(NAVD88_DIR / file).to_crs(4326)
    lons = gdf.geometry.x
    lats = gdf.geometry.y
    depth = float(row["water_elev"]) - gdf["Z_navd88"]
    bins = depth.apply(assign_depth_bin)

    fig = go.Figure()
    for label in sorted(bins.dropna().unique(), key=lambda l: BIN_ORDER.get(l, 999)):
        mask = bins == label
        fig.add_trace(go.Scattermap(
            lon=lons[mask], lat=lats[mask], mode="markers",
            marker=dict(size=6, color=DEPTH_POLY_COLORS.get(label, "#888888")),
            hovertext=[f"{d:.1f} ft" for d in depth[mask]], hoverinfo="text", name=label,
        ))
    fig.update_layout(
        map=dict(style="carto-darkmatter", zoom=12, center=dict(lat=lats.mean(), lon=lons.mean())),
        margin=dict(l=0, r=0, t=0, b=0), showlegend=True,
        legend=dict(x=0.02, y=0.98, xanchor="left", yanchor="top",
                    bgcolor="rgba(0,0,0,0.55)", font=dict(color="white", size=9)),
    )
    return fig


# ---------------- APP ----------------
app = Dash(__name__)

STAT_STYLE = {"font-size": "13px", "margin-bottom": "2px"}
COLUMN_STYLE = {"flex": "1", "padding": "6px", "box-sizing": "border-box", "min-width": "0"}
KEEP_BTN_STYLE = {"padding": "8px 14px", "background": "#2166ac", "color": "white", "border": "none", "margin-top": "8px"}

initial_pending = get_pending_groups()

app.layout = html.Div(
    style={"font-family": "Arial, sans-serif", "padding": "12px"},
    children=[
        dcc.Store(id="pending-store", data=initial_pending),
        dcc.Store(id="index-store", data=0),

        html.H3(id="queue-status"),
        html.Div(id="group-content"),

        html.Div(
            style={"display": "flex", "gap": "14px", "align-items": "center", "margin-top": "14px"},
            children=[
                html.Button("Next group", id="next-btn", n_clicks=0, style={"padding": "8px 16px"}),
                html.Button("Keep both / not a duplicate", id="keep-both-btn", n_clicks=0,
                            style={"padding": "8px 16px", "background": "#fb8c00", "color": "white", "border": "none"}),
            ],
        ),

        html.Div(id="action-message", style={"margin-top": "10px", "color": "#2166ac"}),
    ],
)


@app.callback(
    Output("group-content", "children"),
    Output("queue-status", "children"),
    Input("pending-store", "data"),
    Input("index-store", "data"),
)
def recompute(pending, index):
    if not pending:
        return html.Div("No duplicate groups pending resolution."), "All caught up"

    index = index % len(pending)
    group = pending[index]
    df = load_bathym_fixed().set_index("file")

    columns = []
    for file in group:
        if file not in df.index:
            continue
        row = df.loc[file]
        depth = row.get("depth")
        water_elev = row.get("water_elev")
        stats = [
            html.Div(file, style={"font-weight": "bold", "margin-bottom": "4px"}),
            html.Div(f"date: {row.get('date', 'n/a')}", style=STAT_STYLE),
            html.Div(f"datum: {row.get('datum', 'n/a')}", style=STAT_STYLE),
            html.Div(f"depth: {depth:.2f} ft" if pd.notna(depth) else "depth: n/a", style=STAT_STYLE),
            html.Div(f"water_elev: {water_elev:.2f}" if pd.notna(water_elev) else "water_elev: n/a", style=STAT_STYLE),
            html.Div(f"confirmed: {row.get('confirmed', 'n/a')}, at_risk: {row.get('at_risk', '') or 'n/a'}", style=STAT_STYLE),
        ]

        gpkg_path = NAVD88_DIR / file
        if gpkg_path.exists():
            body = dcc.Graph(figure=build_mini_figure(file, row), style={"height": "380px"})
        else:
            body = html.Div("raw survey points no longer on disk", style={"padding": "20px", "color": "#888"})

        columns.append(html.Div(
            stats + [body, html.Button(
                f"Keep {file}, reject other(s)",
                id={"type": "keep-btn", "file": file}, n_clicks=0, style=KEEP_BTN_STYLE,
            )],
            style=COLUMN_STYLE,
        ))

    content = html.Div(columns, style={"display": "flex", "gap": "10px"})
    status = f"Duplicate group {index + 1} of {len(pending)} pending decisions"
    return content, status


@app.callback(
    Output("index-store", "data", allow_duplicate=True),
    Input("next-btn", "n_clicks"),
    State("pending-store", "data"),
    State("index-store", "data"),
    prevent_initial_call=True,
)
def next_group(n_clicks, pending, index):
    if not pending:
        return 0
    return (index + 1) % len(pending)


@app.callback(
    Output("pending-store", "data", allow_duplicate=True),
    Output("index-store", "data", allow_duplicate=True),
    Output("action-message", "children", allow_duplicate=True),
    Input({"type": "keep-btn", "file": ALL}, "n_clicks"),
    State("pending-store", "data"),
    State("index-store", "data"),
    prevent_initial_call=True,
)
def keep_one(n_clicks_list, pending, index):
    triggered = ctx.triggered_id
    if not isinstance(triggered, dict) or not any(n_clicks_list):
        return no_update, no_update, no_update
    if not pending:
        return pending, index, "Nothing to resolve."

    index = index % len(pending)
    group = pending[index]
    kept_file = triggered["file"]
    if kept_file not in group:
        return no_update, no_update, no_update

    df = _ensure_resolved_column(load_bathym_fixed())
    df.loc[df["file"].isin(group), "duplicate_resolved"] = "yes"

    reject_files = [f for f in group if f != kept_file]
    for f in reject_files:
        df.loc[df["file"] == f, "confirmed"] = "rejected"
        (NAVD88_DIR / f).unlink(missing_ok=True)
        survey_id = f.replace("_SurveyPoint.gpkg", "")
        (DEPTH_POLY_DIR / f"{survey_id}_depth_polygons.geojson").unlink(missing_ok=True)

    df.to_csv(BATHYM_FIXED_FILE, index=False)

    new_pending = get_pending_groups()
    new_index = min(index, max(len(new_pending) - 1, 0))
    message = f"Kept {kept_file}, rejected {', '.join(reject_files)}."
    return new_pending, new_index, message


@app.callback(
    Output("pending-store", "data"),
    Output("index-store", "data", allow_duplicate=True),
    Output("action-message", "children"),
    Input("keep-both-btn", "n_clicks"),
    State("pending-store", "data"),
    State("index-store", "data"),
    prevent_initial_call=True,
)
def keep_both(n_clicks, pending, index):
    if not pending:
        return pending, index, "Nothing to resolve."

    index = index % len(pending)
    group = pending[index]

    df = _ensure_resolved_column(load_bathym_fixed())
    df.loc[df["file"].isin(group), "duplicate_resolved"] = "yes"
    df.to_csv(BATHYM_FIXED_FILE, index=False)

    new_pending = get_pending_groups()
    new_index = min(index, max(len(new_pending) - 1, 0))
    message = f"Marked {', '.join(group)} as not duplicates." if new_pending else "Resolved. No duplicate groups left."
    return new_pending, new_index, message


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8061)

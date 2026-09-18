"""
Voronoi-based centerline extraction for a single river-reach polygon.

Used by 7_compute_navigable_width.py to get a local channel backbone for
"blob" (full-coverage) surveys, without needing an external river-polygon
data source (OSM/NHD): the survey's own point-cloud footprint already covers
its reach bank-to-bank, so the centerline is derived straight from that.

No scipy/networkx dependency -- shapely>=2.0's GEOS-backed voronoi_polygons()
gives the Voronoi edge network directly, and the graph search is a small
stdlib (heapq) double-sweep Dijkstra.
"""

import heapq
import math

import numpy as np
import shapely
from shapely.geometry import LineString, MultiPolygon, Polygon
from shapely.ops import substring
from shapely.prepared import prep


def _densify_ring(coords, densify_m):
    """Insert extra points along a closed ring so consecutive points are
    no more than densify_m apart."""
    out = []
    n = len(coords)
    for i in range(n - 1):  # last coord duplicates the first for a closed ring
        x0, y0 = coords[i]
        x1, y1 = coords[i + 1]
        seg_len = math.hypot(x1 - x0, y1 - y0)
        out.append((x0, y0))
        if seg_len > densify_m:
            steps = int(seg_len // densify_m)
            for s in range(1, steps + 1):
                t = (s * densify_m) / seg_len
                out.append((x0 + t * (x1 - x0), y0 + t * (y1 - y0)))
    return out


def _boundary_points(polygon, densify_m):
    pts = []
    polys = list(polygon.geoms) if isinstance(polygon, MultiPolygon) else [polygon]
    for poly in polys:
        pts.extend(_densify_ring(list(poly.exterior.coords), densify_m))
        for interior in poly.interiors:
            pts.extend(_densify_ring(list(interior.coords), densify_m))
    return pts


def _round_node(x, y, tol=1e-6):
    return (round(x / tol) * tol, round(y / tol) * tol)


def _build_graph(edges_geom):
    """edges_geom: MultiLineString/GeometryCollection of Voronoi edges already
    filtered to lie inside the polygon. Returns adjacency dict node -> [(nbr, w)]."""
    graph = {}
    lines = list(edges_geom.geoms) if hasattr(edges_geom, "geoms") else [edges_geom]
    for line in lines:
        if line.is_empty or line.geom_type != "LineString":
            continue
        coords = list(line.coords)
        for i in range(len(coords) - 1):
            a = _round_node(*coords[i])
            b = _round_node(*coords[i + 1])
            if a == b:
                continue
            w = math.hypot(a[0] - b[0], a[1] - b[1])
            graph.setdefault(a, []).append((b, w))
            graph.setdefault(b, []).append((a, w))
    return graph


def _dijkstra_farthest(graph, start):
    """Returns (farthest_node, dist_dict, prev_dict) from start."""
    dist = {start: 0.0}
    prev = {}
    pq = [(0.0, start)]
    seen = set()
    while pq:
        d, u = heapq.heappop(pq)
        if u in seen:
            continue
        seen.add(u)
        for v, w in graph.get(u, []):
            nd = d + w
            if nd < dist.get(v, math.inf):
                dist[v] = nd
                prev[v] = u
                heapq.heappush(pq, (nd, v))
    farthest = max(dist, key=dist.get)
    return farthest, dist, prev


def _reconstruct_path(prev, start, end):
    path = [end]
    while path[-1] != start:
        path.append(prev[path[-1]])
    path.reverse()
    return path


def _remove_edge(graph, a, b):
    graph[a] = [(n, w) for n, w in graph[a] if n != b]
    graph[b] = [(n, w) for n, w in graph[b] if n != a]


def _spur_length(graph, leaf):
    """Walk the degree-2 chain from `leaf` until a branch point (degree != 2),
    another leaf, or back into itself (a closed loop hanging off the leaf).
    Returns (total_length, spur_nodes, far_end) where spur_nodes is `leaf`
    plus every degree-2 node walked through (these are the ones safe to
    delete entirely) and far_end is the branch/leaf/loop node the spur
    terminates at (kept in the graph -- only its edge to the spur is cut).
    far_end is None if the walk immediately closes a loop back on `leaf`."""
    nodes = [leaf]
    visited = {leaf}
    total = 0.0
    prev, cur = None, leaf
    while True:
        nbrs = [n for n, w in graph[cur] if n != prev]
        if len(graph[cur]) != (1 if prev is None else 2) or not nbrs:
            return total, nodes, None
        nxt = nbrs[0]
        w = next(w for n, w in graph[cur] if n == nxt)
        if nxt in visited:
            # closed loop back on the spur itself -- stop, no far_end to keep
            return total, nodes, None
        total += w
        if len(graph.get(nxt, [])) != 2:
            return total, nodes, nxt  # branch point or another leaf -- keep it
        prev, cur = cur, nxt
        nodes.append(cur)
        visited.add(cur)


def _prune_spurs(graph, prune_len):
    """Iteratively drop short dangling branches (spurs) below a branch point,
    measuring spur length as the full distance from a leaf to the nearest
    junction (degree>=3 node) -- not per-edge -- so a legitimate long,
    non-branching backbone (every node degree 2 except its two true ends) is
    never eaten away. Cleans up the short spurious spurs Voronoi skeletons
    produce near flat (non-rounded) polygon ends."""
    graph = {k: list(v) for k, v in graph.items()}
    if not any(len(v) >= 3 for v in graph.values()):
        return graph  # single simple path, nothing is a "spur" off anything

    changed = True
    while changed:
        changed = False
        leaves = [n for n, v in graph.items() if len(v) == 1]
        for leaf in leaves:
            if leaf not in graph or len(graph[leaf]) != 1:
                continue
            total, nodes, far_end = _spur_length(graph, leaf)
            if far_end is None or len(graph) - len(nodes) < 2:
                continue  # closed loop, or pruning would erase the whole graph
            if total < prune_len:
                chain = nodes + [far_end]
                for a, b in zip(chain, chain[1:]):
                    _remove_edge(graph, a, b)
                for n in nodes:  # far_end stays -- only its edge to the spur is cut
                    graph.pop(n, None)
                changed = True
        if not any(len(v) >= 3 for v in graph.values()):
            break  # collapsed to a simple path -- stop, rest is real backbone
    return graph


def voronoi_centerline(polygon, densify_m=15, simplify_m=5, prune_len=None, trim_ends_m=None):
    """Approximate medial-axis centerline of `polygon` (shapely Polygon or
    MultiPolygon, single CRS units e.g. meters). Returns a LineString, or
    None if the polygon is too small/degenerate to produce a usable skeleton.

    At a flat (non-rounded) open end of the polygon -- e.g. where a survey's
    coverage just stops mid-reach rather than the real bank -- the medial
    axis has no unique bisector and typically veers toward one corner over
    the last stretch instead of landing on the cap's midpoint. This is a
    known characteristic of Voronoi-based centerline extraction generally,
    not something spur-pruning can fix (it's part of the single backbone
    path, not a separate dangling branch). trim_ends_m trims that unreliable
    stretch off both ends of the returned line rather than trying to
    geometrically straighten it -- callers generating transects off this
    line should not sample stations closer than that to either end anyway.
    """
    if polygon is None or polygon.is_empty:
        return None

    boundary_pts = _boundary_points(polygon, densify_m)
    if len(boundary_pts) < 6:
        return None

    voronoi_edges = shapely.voronoi_polygons(
        shapely.multipoints(np.array(boundary_pts)), only_edges=True
    )

    prepared = prep(polygon)
    lines = list(voronoi_edges.geoms) if hasattr(voronoi_edges, "geoms") else [voronoi_edges]
    interior_lines = [ln for ln in lines if ln.geom_type == "LineString" and prepared.contains(ln)]
    if not interior_lines:
        return None

    graph = _build_graph(shapely.GeometryCollection(interior_lines))
    if len(graph) < 2:
        return None

    # Voronoi skeletons fork toward the corners at flat (non-rounded) polygon
    # ends -- prune those short dangling spurs before searching for the
    # backbone, so the double-sweep below doesn't lock onto a corner tip.
    # Corner-spur length scales with local channel width, which we don't know
    # up front, so size the threshold off the graph's own raw diameter
    # instead of an absolute distance: a spur has to be a real fraction of
    # the whole reach to survive.
    if prune_len is None:
        any_node = next(iter(graph))
        seed, _, _ = _dijkstra_farthest(graph, any_node)
        _, raw_dist, _ = _dijkstra_farthest(graph, seed)
        raw_diam = max(raw_dist.values()) if raw_dist else 0.0
        prune_len = max(4 * densify_m, 0.08 * raw_diam)
    graph = _prune_spurs(graph, prune_len)
    if len(graph) < 2:
        return None

    # double-sweep heuristic for the approximate graph diameter path
    any_node = next(iter(graph))
    a, _, _ = _dijkstra_farthest(graph, any_node)
    b, dist_from_a, prev_from_a = _dijkstra_farthest(graph, a)
    if b not in dist_from_a or dist_from_a[b] == 0:
        return None

    path_nodes = _reconstruct_path(prev_from_a, a, b)
    line = LineString(path_nodes)
    if simplify_m:
        line = line.simplify(simplify_m)

    if trim_ends_m is None:
        trim_ends_m = prune_len  # same "how big is the corner artifact" scale
    if trim_ends_m and line.length > 2 * trim_ends_m:
        line = substring(line, trim_ends_m, line.length - trim_ends_m)
    return line

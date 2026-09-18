"""
Identify edge cross-sections in a survey - the ones at the ends of the reach.

Edge sections are identified as the 2 cross-sections with the largest minimum depth.
"""

import numpy as np
import geopandas as gpd
import shapely
from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd 


def get_min_depth(depths):  # min depth in a cross section
    if len(depths) == 0:
        return -np.inf
    return np.min(depths)


def check_linearity(coords, linearity_threshold=0.1):
    if len(coords) < 2:
        return True, 0.0
    centered = coords - coords.mean(axis=0)
    cov = np.cov(centered.T)
    eigvals, _ = np.linalg.eigh(cov)
    eigvals = np.sort(eigvals)[::-1]  # largest to smallest
    # Ratio of minor to major eigenvalue
    if eigvals[0] > 1e-10:
        ratio = eigvals[1] / eigvals[0]
    else:
        ratio = 0.0
    is_linear = ratio < linearity_threshold
    return is_linear, ratio


def axes_are_parallel(axis1, axis2, angle_tolerance_degrees=8.0):
    # Normalize axes
    axis1 = axis1 / np.linalg.norm(axis1)
    axis2 = axis2 / np.linalg.norm(axis2)
    # Dot product (should be close to 1 or -1 if parallel)
    dot = abs(np.dot(axis1, axis2))
    angle_threshold = np.cos(np.radians(angle_tolerance_degrees))
    return dot > angle_threshold


def find_deepest_point(coords, depths):
    """Find the coordinates of the deepest point in a section."""
    deepest_idx = np.argmax(depths)
    return coords[deepest_idx], depths[deepest_idx]


def line_segment_intersects_line(p1, p2, line_coords):
    """
    Check if line segment p1-p2 intersects with a line defined by line_coords.
    Returns True if intersection exists (not counting endpoints touching).
    """
    from shapely.geometry import LineString
    segment = LineString([p1, p2])
    line = LineString(line_coords)
    intersection = segment.intersects(line)
    # Check if it's more than just touching endpoints
    if intersection:
        inter = segment.intersection(line)
        # If intersection is not a point at the endpoints, it's a real crossing
        if hasattr(inter, 'geom_type'):
            if inter.geom_type == 'LineString' or (inter.geom_type == 'Point' and
                not (np.allclose(inter.coords[0], p1) or np.allclose(inter.coords[0], p2))):
                return True
    return False


def generate_path_combinations(path1, path2):
    """
    Generate all possible path combinations from two directional paths.

    Identifies where paths overlap (have matching coordinate points) and where they diverge.
    Returns all possible combinations by mixing diverging segments with the overlap.

    Args:
        path1: dict with 'coords' (Nx2 array), 'sections', 'depths'
        path2: dict with 'coords' (Mx2 array), 'sections', 'depths'

    Returns:
        List of path dicts with all possible combinations
    """
    coords1 = path1["coords"]
    coords2 = path2["coords"]

    # Find exact matching points (with small tolerance for floating point)
    tolerance = 1e-6
    matching_indices_1 = []
    matching_indices_2 = []

    for i, pt1 in enumerate(coords1):
        for j, pt2 in enumerate(coords2):
            if np.allclose(pt1, pt2, atol=tolerance):
                matching_indices_1.append(i)
                matching_indices_2.append(j)
                break

    if not matching_indices_1:
        # No overlap - return both paths as-is
        print("    No overlapping points found between paths")
        return [path1, path2]

    # Find the overlap region (contiguous matching segment)
    # For simplicity, use the first and last matching points as overlap bounds
    overlap_start_1 = matching_indices_1[0]
    overlap_end_1 = matching_indices_1[-1]
    overlap_start_2 = matching_indices_2[0]
    overlap_end_2 = matching_indices_2[-1]

    print(f"    Overlap region: path1[{overlap_start_1}:{overlap_end_1}], path2[{overlap_start_2}:{overlap_end_2}]")

    # Extract segments
    path1_before = {
        "coords": coords1[:overlap_start_1],
        "sections": path1["sections"][:overlap_start_1],
        "depths": path1["depths"][:overlap_start_1],
    }
    path1_overlap = {
        "coords": coords1[overlap_start_1:overlap_end_1+1],
        "sections": path1["sections"][overlap_start_1:overlap_end_1+1],
        "depths": path1["depths"][overlap_start_1:overlap_end_1+1],
    }
    path1_after = {
        "coords": coords1[overlap_end_1+1:],
        "sections": path1["sections"][overlap_end_1+1:],
        "depths": path1["depths"][overlap_end_1+1:],
    }

    path2_before = {
        "coords": coords2[:overlap_start_2],
        "sections": path2["sections"][:overlap_start_2],
        "depths": path2["depths"][:overlap_start_2],
    }
    path2_overlap = {
        "coords": coords2[overlap_start_2:overlap_end_2+1],
        "sections": path2["sections"][overlap_start_2:overlap_end_2+1],
        "depths": path2["depths"][overlap_start_2:overlap_end_2+1],
    }
    path2_after = {
        "coords": coords2[overlap_end_2+1:],
        "sections": path2["sections"][overlap_end_2+1:],
        "depths": path2["depths"][overlap_end_2+1:],
    }

    # Generate all 4 combinations
    combinations = []
    combo_pairs = [
        (path1_before, path1_overlap, path1_after, "Path1-Overlap-Path1"),
        (path2_before, path1_overlap, path1_after, "Path2-Overlap-Path1"),
        (path1_before, path1_overlap, path2_after, "Path1-Overlap-Path2"),
        (path2_before, path1_overlap, path2_after, "Path2-Overlap-Path2"),
    ]

    for before, overlap, after, name in combo_pairs:
        # Only create combinations that have non-empty segments or full overlap
        if len(overlap["coords"]) > 0:
            combo_coords = []
            combo_sections = []
            combo_depths = []

            if len(before["coords"]) > 0:
                combo_coords.extend(before["coords"])
                combo_sections.extend(before["sections"])
                combo_depths.extend(before["depths"])

            combo_coords.extend(overlap["coords"])
            combo_sections.extend(overlap["sections"])
            combo_depths.extend(overlap["depths"])

            if len(after["coords"]) > 0:
                combo_coords.extend(after["coords"])
                combo_sections.extend(after["sections"])
                combo_depths.extend(after["depths"])

            combinations.append({
                "name": name,
                "coords": np.array(combo_coords),
                "sections": combo_sections,
                "depths": np.array(combo_depths),
            })

    return combinations


def find_navigable_path(sections, start_idx, end_idx):
    path_sections = [start_idx]
    path_coords = []
    path_depths = []

    current_idx = start_idx
    visited = {start_idx}

    # Get deepest point of starting section
    current_pt, current_depth = find_deepest_point(sections[current_idx]["coords"],sections[current_idx]["depths"])
    path_coords.append(current_pt)
    path_depths.append(current_depth)

    # Greedy path: always go to deepest reachable point
    while current_idx != end_idx:
        current_section = sections[current_idx]
        current_deepest_pt = current_pt

        # Get perpendicular direction to current section
        current_axis = current_section["axis"]
        current_axis_angle = np.arctan2(current_axis[1], current_axis[0])
        perp_angle = current_axis_angle + np.pi / 2  # 90 degrees perpendicular

        # Find all reachable points across all unvisited sections
        reachable = []  # List of (section_idx, point, depth, angle_diff)

        for i, section in enumerate(sections):
            # Skip current and already visited sections
            if i == current_idx or i in visited:
                continue

            # Get endpoints of current and candidate sections
            current_start = sections[current_idx]["coords"][0]
            current_end = sections[current_idx]["coords"][-1]
            candidate_start = section["coords"][0]
            candidate_end = section["coords"][-1]

            # Test 4 line combinations between endpoints
            endpoint_test_lines = [
                (current_start, candidate_start),
                (current_start, candidate_end),
                (current_end, candidate_start),
                (current_end, candidate_end)
            ]

            # All 4 endpoint paths must be clear (not cross any other section)
            all_paths_clear = True
            for pt1, pt2 in endpoint_test_lines:
                crosses = False
                for j, other_section in enumerate(sections):
                    if j == current_idx or j == i:
                        continue
                    if line_segment_intersects_line(pt1, pt2,
                                                   other_section["coords"]):
                        crosses = True
                        break
                if crosses:
                    all_paths_clear = False
                    break

            # If not all endpoint paths are clear, skip this entire section
            if not all_paths_clear:
                continue

            # Find all valid points on this section (non-crossing + within 45° angle)
            valid_points = []  # List of (point, depth, angle_diff)
            rejected_angle_count = 0

            for pt, depth in zip(section["coords"], section["depths"]):
                # Check if line to this point crosses any other section
                crosses = False
                for j, other_section in enumerate(sections):
                    if j == current_idx or j == i:
                        continue
                    if line_segment_intersects_line(current_deepest_pt, pt,
                                                   other_section["coords"]):
                        crosses = True
                        break

                if crosses:
                    continue

                # Check if jump direction is within 45 degrees of perpendicular
                jump_vec = pt - current_deepest_pt
                jump_angle = np.arctan2(jump_vec[1], jump_vec[0])

                # Calculate angle difference from perpendicular (accounting for 180 degree periodicity)
                angle_diff = np.degrees(jump_angle - perp_angle)
                # Normalize to [-180, 180]
                while angle_diff > 180:
                    angle_diff -= 360
                while angle_diff < -180:
                    angle_diff += 360
                # Account for 180 degree periodicity
                if abs(angle_diff) > 90:
                    angle_diff = 180 - abs(angle_diff)

                if abs(angle_diff) <= 45:
                    valid_points.append((pt, depth, angle_diff))
                else:
                    rejected_angle_count += 1

            # If valid points exist on this section, use the deepest one
            if valid_points:
                deepest_on_section = max(valid_points, key=lambda x: x[1])
                candidate_pt, candidate_depth, angle_diff = deepest_on_section
                reachable.append((i, candidate_pt, candidate_depth, angle_diff))
            elif rejected_angle_count > 0:
                # Section has non-crossing points but all are outside 45° angle
                print(f"    Section {i}: rejected - {rejected_angle_count} points outside 45° angle")

        if not reachable:
            print(f"Dead end at section {current_idx}, stopping.")
            break

        # Pick the deepest reachable point (all are within 45° of perpendicular)
        next_idx, next_pt, next_depth, _ = max(reachable, key=lambda x: x[2])

        # Add to path
        path_coords.append(next_pt)
        path_depths.append(next_depth)
        path_sections.append(next_idx)
        visited.add(next_idx)

        current_idx = next_idx
        current_pt = next_pt
        current_depth = next_depth

    return np.array(path_coords), path_sections, np.array(path_depths)


def are_collinear(coords1, origin1, axis1, coords2, origin2, axis2, angle_tolerance_deg=5.0):
    """
    Check if two sections are collinear by examining the line connecting their midpoints.

    If the connecting line's angle is within ±angle_tolerance_deg of both section axes,
    the sections are collinear and should be merged.
    """
    # Get midpoints
    mid1 = origin1
    mid2 = origin2

    # Vector from mid1 to mid2
    connecting_vec = mid2 - mid1
    if np.linalg.norm(connecting_vec) < 1e-6:
        return False

    # Angle of connecting line
    connecting_angle = np.arctan2(connecting_vec[1], connecting_vec[0])

    # Angles of each section axis
    axis1_angle = np.arctan2(axis1[1], axis1[0])
    axis2_angle = np.arctan2(axis2[1], axis2[0])

    # Helper function to compute signed angle difference (accounting for 180° periodicity)
    def angle_diff(a, b):
        diff = np.degrees(a - b)
        # Normalize to [-180, 180]
        while diff > 180:
            diff -= 360
        while diff < -180:
            diff += 360
        # Account for 180° periodicity (lines have no direction)
        if diff > 90:
            diff = diff - 180
        elif diff < -90:
            diff = diff + 180
        return diff

    # Check if connecting line is aligned with both axes (±tolerance)
    diff1 = angle_diff(connecting_angle, axis1_angle)
    diff2 = angle_diff(connecting_angle, axis2_angle)

    return (np.abs(diff1) <= angle_tolerance_deg) and (np.abs(diff2) <= angle_tolerance_deg)


# ── TEST/DEBUG CODE ────────────────────────────────────────────────────────
UTM_CRS = "EPSG:26915"
WIDTH_TARGET_DEPTH_FT = 9.0
SIMPLIFY_M = 5
GRID_CELL_M = 20
NAVD88_DIR = Path("NAVD88Files")
files = sorted(NAVD88_DIR.glob("*_SurveyPoint.gpkg"))

# loop through all test surveys: 
for fpath in files: 
    print(f"Testing with: {fpath.name}")
    # read in survey point data 
    gdf = gpd.read_file(fpath)
    gdf_utm = gdf.to_crs(UTM_CRS)
    
    # get survey type 
    survey_type = gdf_utm["survey_type"].iloc[0] if "survey_type" in gdf_utm.columns else "unknown"
    print(f"Survey type: {survey_type}")

    # build cross-sections with 8m buffer
    buffer_m = 8
    poly = gdf_utm.geometry.buffer(buffer_m).unary_union # turn all buffered points into a polygon
    fine_dissolved = poly.simplify(SIMPLIFY_M) # clean up cross section 

    # make list of individual cross sections with line ids 
    parts = list(fine_dissolved.geoms) if fine_dissolved.geom_type == "MultiPolygon" else [fine_dissolved]
    parts_gdf = gpd.GeoDataFrame({"line_id": range(len(parts))}, geometry=parts, crs=gdf_utm.crs)
    
    # label which line id each point lies on 
    joined = gpd.sjoin(gdf_utm, parts_gdf, how="inner", predicate="within")
    print(f"Found {len(parts)} cross-sections")
 
    # Extract cross sections
    sections = []
    # loop through each line id (grp = set of points that fall on line_id)
    for line_id, grp in joined.groupby("line_id"):
        if len(grp) < 5:
            continue # make sure section is large enough 
        coords = np.column_stack([grp.geometry.x.values, grp.geometry.y.values])
        depths = grp["depth_ft"].values
        # check if points within a section are linear, skip nonlinear sections
        is_linear, linearity_ratio = check_linearity(coords, linearity_threshold=0.15)
        if not is_linear:
            continue

        # Sort by PCA axis
        centered = coords - coords.mean(axis=0)
        cov = np.cov(centered.T)
        eigvals, eigvecs = np.linalg.eigh(cov)
        axis = eigvecs[:, np.argmax(eigvals)]
        origin = coords.mean(axis=0)
        local_station = (coords - origin) @ axis
        order = np.argsort(local_station)

        sections.append({
            "section_id": line_id,
            "line_id": line_id,
            "coords": coords[order],
            "depths": depths[order],
            "centroid": origin,
            "axis": axis,
            "linearity_ratio": linearity_ratio,
        })

    # Create DataFrame of original sections for debugging
    section_stats = []
    for i, sec in enumerate(sections):
        axis_angle = np.degrees(np.arctan2(sec["axis"][1], sec["axis"][0]))
        section_stats.append({
            "line_id": sec["line_id"],
            "linearity_ratio": sec["linearity_ratio"],
            "axis_angle_deg": axis_angle,
            "centroid_x": sec["centroid"][0],
            "centroid_y": sec["centroid"][1],
            "num_points": len(sec["coords"]),
        })
    stats_df = pd.DataFrame(section_stats)
    print("\nOriginal sections (before merging):")
    print(stats_df.to_string(index=False))
    print()

    # Merge collinear sections
    merged_sections = []
    section_groups = []  # Track which original sections make up each merged section
    used = set()
    merge_log = []  # Log merge decisions for debugging

    for i, sec1 in enumerate(sections):
        if i in used:
            continue

        merged_coords = [sec1["coords"]]
        merged_depths = [sec1["depths"]]
        merged_indices = [i]  # Track which original indices are in this group
        merged_line_ids = [sec1["line_id"]]  # Track original line_ids

        # Find other sections collinear with this one
        for j, sec2 in enumerate(sections[i+1:], start=i+1):
            if j in used:
                continue

            # Calculate merge metrics
            mid1 = sec1["centroid"]
            mid2 = sec2["centroid"]
            connecting_vec = mid2 - mid1

            axis1_angle = np.degrees(np.arctan2(sec1["axis"][1], sec1["axis"][0]))
            axis2_angle = np.degrees(np.arctan2(sec2["axis"][1], sec2["axis"][0]))
            connecting_angle = np.degrees(np.arctan2(connecting_vec[1], connecting_vec[0]))

            # Compute angle differences (accounting for 180° periodicity)
            def angle_diff(a, b):
                diff = np.abs(a - b)
                if diff > 90:
                    diff = 180 - diff
                return diff

            diff_to_axis1 = angle_diff(connecting_angle, axis1_angle)
            diff_to_axis2 = angle_diff(connecting_angle, axis2_angle)

            collinear = are_collinear(sec1["coords"], sec1["centroid"], sec1["axis"],
                                     sec2["coords"], sec2["centroid"], sec2["axis"],
                                     angle_tolerance_deg=5.0)

            merge_log.append({
                "line_id_1": sec1["line_id"],
                "line_id_2": sec2["line_id"],
                "axis1_angle": axis1_angle,
                "axis2_angle": axis2_angle,
                "connecting_angle": connecting_angle,
                "diff_to_axis1": diff_to_axis1,
                "diff_to_axis2": diff_to_axis2,
                "collinear": collinear,
                "merged": collinear,
            })

            if collinear:
                merged_coords.append(sec2["coords"])
                merged_depths.append(sec2["depths"])
                merged_indices.append(j)
                merged_line_ids.append(sec2["line_id"])
                used.add(j)

        # Combine all merged sections
        if len(merged_coords) > 1:
            all_coords = np.vstack(merged_coords)
            all_depths = np.concatenate(merged_depths)
            # Re-sort by the shared axis
            local_station = (all_coords - sec1["centroid"]) @ sec1["axis"]
            order = np.argsort(local_station)
            merged_sections.append({
                "section_id": f"{sec1['section_id']}_merged",
                "line_ids": merged_line_ids,
                "coords": all_coords[order],
                "depths": all_depths[order],
                "centroid": all_coords.mean(axis=0),
                "axis": sec1["axis"],
                "is_merged": True,
                "original_sections": merged_indices,
                "original_coords_list": merged_coords,
            })
            section_groups.append({
                "indices": merged_indices,
                "is_merged": True,
            })
        else:
            sec1["line_ids"] = [sec1["line_id"]]
            merged_sections.append(sec1)
            section_groups.append({
                "indices": [i],
                "is_merged": False,
            })

    sections = merged_sections

    # Print merge decisions
    merge_df = pd.DataFrame(merge_log)
    if not merge_df.empty:
        print("\nMerge decisions (all pairs checked):")
        print(merge_df.to_string(index=False))
        print()

    # Filter out straggler sections (too small compared to main navigable path)
    if sections:
        # Find sections with high depth values (top 50%)
        all_depths = []
        for sec in sections:
            all_depths.extend(sec["depths"])
        depth_threshold = np.percentile(all_depths, 50)

        high_depth_sections = []
        for sec in sections:
            if np.max(sec["depths"]) >= depth_threshold:
                high_depth_sections.append(sec)

        if high_depth_sections:
            # Calculate average length of high-depth sections
            avg_length = np.mean([np.linalg.norm(sec["coords"][-1] - sec["coords"][0])
                                  for sec in high_depth_sections])
            min_length = 0.1 * avg_length

            # Keep only sections longer than 10% of average
            original_count = len(sections)
            sections = [sec for sec in sections
                       if np.linalg.norm(sec["coords"][-1] - sec["coords"][0]) >= min_length]

            print(f"Removed {original_count - len(sections)} straggler sections")
            print(f"Kept {len(sections)} main navigable sections (avg length: {avg_length:.1f}m, min: {min_length:.1f}m)\n")

    # Identify edge sections using perpendicular crossing logic
    if len(sections) >= 2:
        # Get reach axis and origin
        all_centroids = np.array([s["centroid"] for s in sections])
        centered = all_centroids - all_centroids.mean(axis=0)
        cov = np.cov(centered.T)
        eigvals, eigvecs = np.linalg.eigh(cov)
        reach_axis = eigvecs[:, np.argmax(eigvals)]
        reach_origin = all_centroids.mean(axis=0)

        # For each section, count how many sections are on each side along the reach
        edge_indices = []
        positions = []

        for i, sec in enumerate(sections):
            # Project this section onto reach axis
            my_pos = (sec["centroid"] - reach_origin) @ reach_axis
            positions.append(my_pos)

            # Count sections upstream (lower position) and downstream (higher position)
            upstream_count = 0
            downstream_count = 0

            for j, other_sec in enumerate(sections):
                if i == j:
                    continue
                other_pos = (other_sec["centroid"] - reach_origin) @ reach_axis
                if other_pos < my_pos:
                    upstream_count += 1
                else:
                    downstream_count += 1

            # Edge if no sections on one side
            if upstream_count == 0 or downstream_count == 0:
                edge_indices.append(i)

        result = {
            "edge_indices": edge_indices,
            "positions": positions,
            "reach_axis": reach_axis,
            "reach_origin": reach_origin,
        }
    else:
        result = {"edge_indices": list(range(len(sections))), "positions": [0.0] * len(sections)}

    # Store edge indices for later use
    edge_indices = result["edge_indices"]

    print(f"Processing {len(sections)} valid cross-sections\n")

    print("All sections by position along reach:")
    positions_with_idx = [(i, result["positions"][i]) for i in range(len(sections))]
    positions_with_idx.sort(key=lambda x: x[1])
    for idx, (sec_idx, pos) in enumerate(positions_with_idx):
        is_edge = " <- EDGE" if sec_idx in result["edge_indices"] else ""
        print(f"  {sec_idx:2d}: position = {pos:10.1f} m{is_edge}")

    print(f"\nEdge sections (upstream, downstream): {result['edge_indices']}")
    upstream_idx, downstream_idx = result['edge_indices']
    print(f"  Upstream (section {upstream_idx}): {result['positions'][upstream_idx]:.1f}m")
    print(f"  Downstream (section {downstream_idx}): {result['positions'][downstream_idx]:.1f}m")

    # Find navigable paths from each edge
    print("\nFinding navigable paths...")
    paths = []
    path_results = []  # Track if paths reached their destination

    for start_idx, end_idx in [(upstream_idx, downstream_idx), (downstream_idx, upstream_idx)]:
        print(f"  Path from section {start_idx} to {end_idx}:")
        path_coords, path_sections, path_depths = find_navigable_path(sections, start_idx, end_idx)

        # Check if path reached its destination or got blocked
        reached_destination = (len(path_sections) > 0 and path_sections[-1] == end_idx)
        if not reached_destination and len(path_sections) > 1:
            blocked_at = path_sections[-1]
            print(f"    ⚠ Path blocked at section {blocked_at}, couldn't reach {end_idx}")
        elif not reached_destination:
            print(f"    ⚠ Path failed to progress")

        path_info = {
            "start": start_idx,
            "end": end_idx,
            "coords": path_coords,
            "sections": path_sections,
            "depths": path_depths,
            "reached_destination": reached_destination,
            "length": np.sum([np.linalg.norm(path_coords[i+1] - path_coords[i]) for i in range(len(path_coords)-1)]) if len(path_coords) > 1 else 0,
        }
        path_results.append(path_info)
        print(f"    Sections in path: {path_sections}")
        print(f"    Path length: {path_info['length']:.1f}m")

    # Decide which paths to use based on blocked status
    if path_results[0]["reached_destination"] and path_results[1]["reached_destination"]:
        # Both paths complete - use both
        print("\n  Both paths complete - using both paths")
        paths = path_results
    elif path_results[0]["reached_destination"] and not path_results[1]["reached_destination"]:
        # Path 1 complete, Path 2 blocked - use Path 2 (blocked one)
        print("\n  Path 2 blocked, Path 1 complete - using blocked path (Path 2)")
        paths = [path_results[1]]
    elif not path_results[0]["reached_destination"] and path_results[1]["reached_destination"]:
        # Path 1 blocked, Path 2 complete - use Path 1 (blocked one)
        print("\n  Path 1 blocked, Path 2 complete - using blocked path (Path 1)")
        paths = [path_results[0]]
    else:
        # Both paths blocked - use both (they span from different directions)
        print("\n  Both paths blocked - using both (coverage from both directions)")
        paths = path_results

    # Print path summary
    print(f"\n  Generated {len(paths)} path option(s):")
    for i, path in enumerate(paths):
        name = path.get("name", f"Path {i}")
        print(f"    {i+1}. {name}: {len(path['sections'])} sections, length {path.get('length', 0):.1f}m")

    # Find overlapping points between paths
    overlap_points = []
    if len(paths) > 1:
        coords1 = paths[0]["coords"]
        coords2 = paths[1]["coords"]
        tolerance = 1e-6

        for pt1 in coords1:
            for pt2 in coords2:
                if np.allclose(pt1, pt2, atol=tolerance):
                    overlap_points.append(pt1)
                    break

    # Ensure both paths go in same direction
    if len(paths) > 1:
        # Check direction by comparing section indices
        sections1_start = paths[0]["sections"][0]
        sections1_end = paths[0]["sections"][-1]
        sections2_start = paths[1]["sections"][0]
        sections2_end = paths[1]["sections"][-1]

        path1_forward = sections1_start < sections1_end
        path2_forward = sections2_start < sections2_end

        # If going in opposite directions, reverse path 2
        if path1_forward != path2_forward:
            print(f"\n  Path directions differ (Path 1: {'forward' if path1_forward else 'backward'}, Path 2: {'forward' if path2_forward else 'backward'}) - reversing Path 2")
            paths[1]["coords"] = paths[1]["coords"][::-1]
            paths[1]["sections"] = paths[1]["sections"][::-1]
            paths[1]["depths"] = paths[1]["depths"][::-1]

    # Generate path options based on portions
    def split_path_into_portions(path, overlap_points, tolerance=1e-6):
        """Split path into portions where overlap status changes."""
        coords = path["coords"]
        sections = path.get("sections", [])
        depths = path.get("depths", [])
        portions = []

        if len(coords) == 0:
            return []

        # Mark each point as overlapping or not
        is_overlapping = []
        for pt in coords:
            overlaps = any(np.allclose(pt, op, atol=tolerance) for op in overlap_points)
            is_overlapping.append(overlaps)

        # Split into portions
        current_portion_coords = [coords[0]]
        current_portion_sections = [sections[0]] if len(sections) > 0 else []
        current_portion_depths = [depths[0]] if len(depths) > 0 else []
        current_overlap = is_overlapping[0]

        for i in range(1, len(coords)):
            if is_overlapping[i] == current_overlap:
                # Same status, continue portion
                current_portion_coords.append(coords[i])
                if len(sections) > i:
                    current_portion_sections.append(sections[i])
                if len(depths) > i:
                    current_portion_depths.append(depths[i])
            else:
                # Status changed, save portion and start new one
                portions.append({
                    "coords": np.array(current_portion_coords),
                    "sections": current_portion_sections,
                    "depths": np.array(current_portion_depths) if len(current_portion_depths) > 0 else None,
                    "is_overlapping": current_overlap
                })
                current_portion_coords = [coords[i]]
                current_portion_sections = [sections[i]] if len(sections) > i else []
                current_portion_depths = [depths[i]] if len(depths) > i else []
                current_overlap = is_overlapping[i]

        # Save last portion
        if len(current_portion_coords) > 0:
            portions.append({
                "coords": np.array(current_portion_coords),
                "sections": current_portion_sections,
                "depths": np.array(current_portion_depths) if len(current_portion_depths) > 0 else None,
                "is_overlapping": current_overlap
            })

        return portions

    path_options = []
    if len(paths) > 1:
        # Split both paths into portions
        portions1 = split_path_into_portions(paths[0], overlap_points)
        portions2 = split_path_into_portions(paths[1], overlap_points)

        print(f"\n  Path 1: {len(portions1)} portions")
        for i, p in enumerate(portions1):
            print(f"    Portion {i+1}: {'overlapping' if p['is_overlapping'] else 'not overlapping'} ({len(p['coords'])} points)")

        print(f"\n  Path 2: {len(portions2)} portions")
        for i, p in enumerate(portions2):
            print(f"    Portion {i+1}: {'overlapping' if p['is_overlapping'] else 'not overlapping'} ({len(p['coords'])} points)")

        # Check that both paths have same number of portions and overlap status
        if len(portions1) != len(portions2):
            print(f"\n  ERROR: Path 1 has {len(portions1)} portions, Path 2 has {len(portions2)} portions - mismatch!")
        else:
            overlap_mismatch = False
            for i, (p1, p2) in enumerate(zip(portions1, portions2)):
                if p1["is_overlapping"] != p2["is_overlapping"]:
                    print(f"  ERROR: Portion {i+1} overlap status mismatch (Path 1: {p1['is_overlapping']}, Path 2: {p2['is_overlapping']})")
                    overlap_mismatch = True

            if not overlap_mismatch:
                # Generate all path combinations
                num_non_overlapping = sum(1 for p in portions1 if not p["is_overlapping"])
                num_options = 2 ** num_non_overlapping

                print(f"\n  {num_non_overlapping} non-overlapping portions -> {num_options} path option(s)")

                # Generate combinations
                for combo_idx in range(num_options):
                    full_coords = []
                    full_sections = []
                    full_depths = []
                    binary_choices = format(combo_idx, f'0{num_non_overlapping}b')
                    choice_idx = 0

                    for portion_idx, (p1, p2) in enumerate(zip(portions1, portions2)):
                        if p1["is_overlapping"]:
                            # Only one option
                            full_coords.extend(p1["coords"])
                            full_sections.extend(p1.get("sections", []))
                            if p1.get("depths") is not None:
                                full_depths.extend(p1["depths"])
                        else:
                            # Two options, use binary choice
                            if binary_choices[choice_idx] == '0':
                                full_coords.extend(p1["coords"])
                                full_sections.extend(p1.get("sections", []))
                                if p1.get("depths") is not None:
                                    full_depths.extend(p1["depths"])
                            else:
                                full_coords.extend(p2["coords"])
                                full_sections.extend(p2.get("sections", []))
                                if p2.get("depths") is not None:
                                    full_depths.extend(p2["depths"])
                            choice_idx += 1

                    path_options.append({
                        "option_id": combo_idx + 1,
                        "coords": np.array(full_coords),
                        "sections": np.array(full_sections) if len(full_sections) > 0 else np.array([]),
                        "depths": np.array(full_depths) if len(full_depths) > 0 else np.array([])
                    })

                print(f"\n  Generated {len(path_options)} path option(s):")
                for opt in path_options:
                    print(f"    Option {opt['option_id']}: {len(opt['coords'])} points")

    # Calculate navigable width for each path option
    def calculate_navigable_width(path_coords, path_sections, sections, min_depth_threshold=9.0):
        """
        Calculate navigable width at each point on the path.
        Returns: widths array, min_width, avg_width, min_width_coord
        """
        widths = []
        min_width_coord = None

        for point_idx, (point, section_idx) in enumerate(zip(path_coords, path_sections)):
            section = sections[int(section_idx)]
            section_coords = section["coords"]
            section_depths = section["depths"]

            # Find closest point in section to the path point
            distances = np.linalg.norm(section_coords - point, axis=1)
            closest_idx = np.argmin(distances)

            # Check if the closest point has sufficient depth
            if section_depths[closest_idx] < min_depth_threshold:
                width = 0.0
            else:
                # Expand left and right from closest_idx while depth >= min_depth_threshold
                left_idx = closest_idx
                while left_idx > 0 and section_depths[left_idx - 1] >= min_depth_threshold:
                    left_idx -= 1

                right_idx = closest_idx
                while right_idx < len(section_depths) - 1 and section_depths[right_idx + 1] >= min_depth_threshold:
                    right_idx += 1

                # Calculate distance from left_idx to right_idx along the cross section
                if left_idx == right_idx:
                    width = 0.0
                else:
                    # Sum distances between consecutive points
                    width = 0.0
                    for i in range(left_idx, right_idx):
                        width += np.linalg.norm(section_coords[i + 1] - section_coords[i])

            widths.append(width)

        widths = np.array(widths)
        min_width = np.min(widths) if len(widths) > 0 else 0
        avg_width = np.mean(widths) if len(widths) > 0 else 0
        min_width_idx = np.argmin(widths) if len(widths) > 0 else 0
        min_width_coord = path_coords[min_width_idx]

        return widths, min_width, avg_width, min_width_coord

    # Determine which paths to show and evaluate
    plots_to_show = path_options if len(path_options) > 0 else paths

    # Evaluate all path options
    path_results = []
    for opt in plots_to_show:
        # Get section indices for this path
        if "sections" in opt and len(opt["sections"]) > 0:
            path_sections = opt["sections"]
        else:
            # Fallback for paths without section info
            path_sections = np.arange(len(sections))[::max(1, len(sections)//len(opt["coords"]))][:len(opt["coords"])]

        widths, min_width, avg_width, min_width_coord = calculate_navigable_width(
            opt["coords"],
            path_sections,
            sections
        )

        path_length = sum(np.linalg.norm(opt["coords"][i+1] - opt["coords"][i])
                         for i in range(len(opt["coords"])-1))

        path_results.append({
            "path": opt,
            "widths": widths,
            "min_width": min_width,
            "avg_width": avg_width,
            "min_width_coord": min_width_coord,
            "path_length": path_length
        })

    # Find optimal path
    if len(path_results) > 0:
        # Sort by: max min_width, then max avg_width, then min path_length
        optimal_idx = max(range(len(path_results)),
                         key=lambda i: (path_results[i]["min_width"],
                                       path_results[i]["avg_width"],
                                       -path_results[i]["path_length"]))
        optimal_path = path_results[optimal_idx]

        print(f"\n  Path Width Analysis:")
        for i, result in enumerate(path_results):
            path_name = result["path"].get("option_id", f"Path {i+1}")
            print(f"    {path_name}: min_width={result['min_width']:.1f}m, avg_width={result['avg_width']:.1f}m, length={result['path_length']:.1f}m")

        print(f"\n  Optimal Path: {optimal_path['path'].get('option_id', 'Unknown')}")
        print(f"    Min width: {optimal_path['min_width']:.1f}m")
        print(f"    Avg width: {optimal_path['avg_width']:.1f}m")
        print(f"    Min width location: {optimal_path['min_width_coord']}")

    # Plot path options
    # Create subplot grid
    num_plots = len(plots_to_show)
    num_cols = min(3, num_plots)
    num_rows = (num_plots + num_cols - 1) // num_cols
    fig, axes = plt.subplots(num_rows, num_cols, figsize=(6*num_cols, 6*num_rows))

    # Ensure axes is always 2D array
    if num_rows == 1 and num_cols == 1:
        axes = np.array([[axes]])
    elif num_rows == 1 or num_cols == 1:
        axes = np.array([axes]).reshape(num_rows, num_cols)
    else:
        axes = np.array(axes)

    # Plot each path option
    for plot_idx, plot_data in enumerate(plots_to_show):
        row = plot_idx // num_cols
        col = plot_idx % num_cols
        ax = axes[row, col]

        # Plot all points colored by depth
        scatter = ax.scatter(
            gdf_utm.geometry.x, gdf_utm.geometry.y,
            c=gdf_utm["depth_ft"], cmap="RdYlBu_r", vmin=5, vmax=30, s=10, alpha=0.6
        )

        # Plot all section lines in light gray
        for i, section in enumerate(sections):
            coords = section["coords"]
            ax.plot(coords[:, 0], coords[:, 1], color="black", linewidth=0.8, alpha=0.2)

        # Highlight edge sections
        for edge_idx in edge_indices:
            if edge_idx < len(sections):
                coords = sections[edge_idx]["coords"]
                ax.plot(coords[:, 0], coords[:, 1], color="darkgreen", linewidth=2, alpha=0.6, zorder=3)
                mid_idx = len(coords) // 2
                ax.text(coords[mid_idx, 0], coords[mid_idx, 1], str(edge_idx),
                       fontsize=12, fontweight="bold", ha="center",
                       bbox=dict(boxstyle="round,pad=0.5", facecolor="yellow",
                                edgecolor="darkgreen", linewidth=2, alpha=0.8))

        # Plot the path
        path_coords = plot_data["coords"]
        ax.plot(path_coords[:, 0], path_coords[:, 1],
               color="blue", linewidth=4, alpha=0.9, zorder=5)

        # Title
        if "option_id" in plot_data:
            title = f"Path Option {plot_data['option_id']}"
        else:
            title = f"Path {plot_idx + 1}"
        ax.set_title(title, fontsize=12, fontweight="bold")
        ax.set_aspect("equal")

    # Hide unused subplots
    for idx in range(num_plots, num_rows * num_cols):
        row = idx // num_cols
        col = idx % num_cols
        axes[row, col].set_visible(False)

    # Add single colorbar
    cbar_ax = fig.add_axes([0.92, 0.15, 0.02, 0.7])
    plt.colorbar(scatter, cax=cbar_ax, label="Depth (ft)")

    fig.suptitle(f"{fpath.name}\nPath Options", fontsize=14, fontweight="bold")
    plt.tight_layout(rect=[0, 0, 0.9, 0.96])
    plt.show()

    # Plot optimal path
    if len(path_results) > 0:
        fig, ax = plt.subplots(figsize=(12, 10))

        # Plot all points colored by depth
        scatter = ax.scatter(
            gdf_utm.geometry.x, gdf_utm.geometry.y,
            c=gdf_utm["depth_ft"], cmap="RdYlBu_r", vmin=5, vmax=30, s=10, alpha=0.6
        )

        # Plot all section lines in light gray
        for i, section in enumerate(sections):
            coords = section["coords"]
            ax.plot(coords[:, 0], coords[:, 1], color="black", linewidth=0.8, alpha=0.2)

        # Highlight edge sections
        for edge_idx in edge_indices:
            if edge_idx < len(sections):
                coords = sections[edge_idx]["coords"]
                ax.plot(coords[:, 0], coords[:, 1], color="darkgreen", linewidth=2, alpha=0.6, zorder=3)
                mid_idx = len(coords) // 2
                ax.text(coords[mid_idx, 0], coords[mid_idx, 1], str(edge_idx),
                       fontsize=12, fontweight="bold", ha="center",
                       bbox=dict(boxstyle="round,pad=0.5", facecolor="yellow",
                                edgecolor="darkgreen", linewidth=2, alpha=0.8))

        # Plot optimal path
        optimal_path = path_results[optimal_idx]
        path_coords = optimal_path["path"]["coords"]
        ax.plot(path_coords[:, 0], path_coords[:, 1],
               color="blue", linewidth=4, alpha=0.9, zorder=5, label="Optimal Path")

        # Mark minimum width location
        min_width_coord = optimal_path["min_width_coord"]
        ax.scatter([min_width_coord[0]], [min_width_coord[1]],
                  color="red", s=200, zorder=6, marker="*",
                  edgecolors="darkred", linewidths=2,
                  label=f"Min Width: {optimal_path['min_width']:.1f}m")

        plt.colorbar(scatter, ax=ax, label="Depth (ft)")
        ax.set_title(f"{fpath.name}\nOptimal Path (Min Width: {optimal_path['min_width']:.1f}m, Avg Width: {optimal_path['avg_width']:.1f}m)",
                    fontsize=14, fontweight="bold")
        ax.set_aspect("equal")
        ax.legend(loc="best")
        plt.tight_layout()
        plt.show()

        # Save results to dataframe
        survey_id = fpath.stem
        results_df = pd.DataFrame([{
            "survey_id": survey_id,
            "min_width": optimal_path["min_width"],
            "avg_width": optimal_path["avg_width"],
            "path_length": optimal_path["path_length"],
            "min_width_coords": str(optimal_path["min_width_coord"]),
            "optimal_path_polyline": str(optimal_path["path"]["coords"].tolist())
        }])

        print(f"\n  Results saved to dataframe:")
        print(results_df)

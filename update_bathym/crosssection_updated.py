"""
Identify edge cross-sections in a survey - the ones at the ends of the reach.

Edge sections are identified as the 2 cross-sections with the largest minimum depth.
"""

import numpy as np
import geopandas as gpd
import shapely
from pathlib import Path
import pandas as pd

# coords passed into the barge-width functions below are UTM (meters, e.g.
# EPSG:26915) -- barge_width_ft/min_width_ft are feet, so distances need
# converting before comparing against them.
FT_PER_M = 3.280839895


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


def calculate_barge_width_depths(coords, depths, barge_width_ft=300, min_width_ft=150):
    """
    Calculate average depths centered at each point, spanning barge_width_ft total width.

    For each point, finds all points within min_width_ft distance on each side,
    and returns the average depth. Points with less than min_width_ft available
    on either side are marked with NaN (not valid for consideration).

    Args:
        coords: Nx2 array of (x, y) coordinates along the cross section
        depths: N array of depth values
        barge_width_ft: Total width to average over (default 300ft for barge)
        min_width_ft: Required distance on each side (default 150ft)

    Returns:
        averaged_depths: N array of averaged depths (NaN where not enough width)
    """
    averaged_depths = np.full(len(depths), np.nan)

    for i, point in enumerate(coords):
        # Calculate distances (in feet) from this point to all others along the cross section
        distances_ft = np.linalg.norm(coords - point, axis=1) * FT_PER_M

        # Check if we have enough width on both sides
        max_dist_left = np.max(distances_ft[:i]) if i > 0 else 0
        max_dist_right = np.max(distances_ft[i+1:]) if i < len(coords) - 1 else 0

        # Only consider if we have at least min_width_ft available in both directions
        if max_dist_left >= min_width_ft and max_dist_right >= min_width_ft:
            # Get all points within min_width_ft on each side
            valid_mask = distances_ft <= min_width_ft
            if np.sum(valid_mask) > 1:  # at least 2 points (center + others)
                averaged_depths[i] = np.mean(depths[valid_mask])

    return averaged_depths


def find_deepest_point_barge_width(coords, depths, barge_width_ft=300, min_width_ft=150):
    """
    Find the deepest point considering 300ft barge width averaged depths.

    Returns the point with the highest average depth (averaging over ±150ft),
    only considering points that have sufficient width on both sides.
    """
    averaged_depths = calculate_barge_width_depths(coords, depths, barge_width_ft, min_width_ft)

    # Find the index with the maximum valid averaged depth
    valid_mask = ~np.isnan(averaged_depths)
    if not np.any(valid_mask):
        # Fallback to regular deepest point if no valid barge-width points
        deepest_idx = np.argmax(depths)
        return coords[deepest_idx], depths[deepest_idx]

    deepest_idx = np.nanargmax(averaged_depths)
    return coords[deepest_idx], averaged_depths[deepest_idx]


def perpendicular_cone_crossing_test(current_pt, perp_angle, sections, current_idx, candidate_idx):
    """
    Test if perpendicular cone from current point can reach candidate section.

    Creates 2 test rays at ±20° from the perpendicular direction. Acceptance criteria:
    1. Both rays reach candidate without crossing other sections, OR
    2. One ray reaches candidate cleanly AND the other ray doesn't cross any sections

    Args:
        current_pt: Starting point (x, y)
        perp_angle: Perpendicular angle in radians
        sections: List of all cross-section dictionaries
        current_idx: Index of current section (excluded from crossing test)
        candidate_idx: Index of candidate section

    Returns:
        True if cone is acceptable per the above criteria, False otherwise
    """
    from shapely.geometry import LineString, Point

    candidate_coords = sections[candidate_idx]["coords"]
    candidate_line = LineString(candidate_coords)

    # Create 2 test rays at ±20° from perpendicular
    test_angles = [perp_angle + np.radians(20), perp_angle - np.radians(20)]

    ray_results = []  # Track (reaches_candidate, crosses_other_sections) for each ray

    for test_angle in test_angles:
        # Create a long ray extending from current_pt in this direction
        ray_direction = np.array([np.cos(test_angle), np.sin(test_angle)])
        ray_endpoint = current_pt + 1000 * ray_direction  # Extend far enough to hit candidate
        ray = LineString([current_pt, ray_endpoint])

        # Find intersection with candidate section
        intersection = ray.intersection(candidate_line)
        reaches_candidate = False
        intersection_pt = None

        if not intersection.is_empty and not (intersection.geom_type == 'Point' and np.allclose(intersection.coords[0], current_pt)):
            reaches_candidate = True
            # Get the first intersection point (closest to current_pt)
            if intersection.geom_type == 'Point':
                intersection_pt = np.array(intersection.coords[0])
            elif intersection.geom_type == 'LineString':
                intersection_pt = np.array(intersection.coords[0])

        # Check if the ray path crosses any other sections
        crosses_other = False

        if reaches_candidate and intersection_pt is not None:
            # Check from current_pt to candidate intersection
            test_segment = LineString([current_pt, intersection_pt])
        else:
            # Check the entire ray direction
            test_segment = ray

        for j, other_section in enumerate(sections):
            if j == current_idx or j == candidate_idx:
                continue
            other_line = LineString(other_section["coords"])

            # Check if this segment crosses the other section (not just touches endpoints)
            crossing = test_segment.intersection(other_line)
            if not crossing.is_empty:
                # Check if it's more than just touching at an endpoint
                if crossing.geom_type == 'Point':
                    # A single point - check if it's at the endpoints of test_segment
                    crossing_coords = crossing.coords[0]
                    if not (np.allclose(crossing_coords, current_pt) or
                           (intersection_pt is not None and np.allclose(crossing_coords, intersection_pt))):
                        # Crosses in the middle
                        crosses_other = True
                        break
                elif crossing.geom_type in ['LineString', 'MultiPoint', 'MultiLineString']:
                    # Crosses with a line or multiple points
                    crosses_other = True
                    break

        ray_results.append((reaches_candidate, crosses_other))

    # Apply acceptance criteria
    ray1_reaches, ray1_crosses = ray_results[0]
    ray2_reaches, ray2_crosses = ray_results[1]

    # Criterion 1: Both rays reach candidate without crossing
    if ray1_reaches and not ray1_crosses and ray2_reaches and not ray2_crosses:
        return True

    # Criterion 2: One reaches candidate cleanly AND the other doesn't cross anything
    if (ray1_reaches and not ray1_crosses and not ray2_crosses) or \
       (ray2_reaches and not ray2_crosses and not ray1_crosses):
        return True

    return False


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



def find_longest_section(sections):
    """Find the section with the greatest point coverage (cumulative distance through all points)."""
    longest_idx = 0
    max_length = 0
    lengths = []

    for i, sec in enumerate(sections):
        coords = sec["coords"]
        if len(coords) < 2:
            lengths.append(0)
            continue
        # Calculate cumulative distance through all points (total coverage)
        total_distance = 0
        for j in range(len(coords) - 1):
            segment_distance = np.linalg.norm(coords[j+1] - coords[j])
            total_distance += segment_distance
        lengths.append(total_distance)
        if total_distance > max_length:
            max_length = total_distance
            longest_idx = i

    return longest_idx, max_length


def navigable_path_from_longest(sections, center_idx, starting_point, starting_depth):
    """
    Build navigable paths in both directions from the longest section.

    Phase 1: Starting from the deepest point on the center section, find first reachable
    sections in both +90° and -90° perpendicular directions (direction-constrained).

    Phase 2: Expand both paths independently without direction constraints. Both paths
    share a visited set to avoid crossing. Paths end when no reachable unvisited sections remain.

    Args:
        sections: List of all cross-section dictionaries
        center_idx: Index of the longest/center section to start from
        starting_point: The (x, y) coordinates of the deepest point on the center section
        starting_depth: The 300ft-averaged depth at the starting point

    Returns:
        Two paths (upstream and downstream)
    """
    center_section = sections[center_idx]
    center_axis = center_section["axis"]
    center_axis_angle = np.arctan2(center_axis[1], center_axis[0])

    # Shared visited set for both paths
    shared_visited = {center_idx}

    # Initialize both paths
    paths = []

    # PHASE 1: Find first reachable sections in both directions (direction-constrained)
    first_jumps = []

    for direction_idx, direction_offset in enumerate([np.pi / 2, -np.pi / 2]):
        perp_angle = center_axis_angle + direction_offset
        direction_name = "upstream" if direction_idx == 0 else "downstream"
        reachable = []

        for i, section in enumerate(sections):
            if i in shared_visited:
                continue

            # Test perpendicular cone from starting point
            cone_clear = perpendicular_cone_crossing_test(starting_point, perp_angle, sections, center_idx, i)

            if not cone_clear:
                continue

            # Find valid points (direction-constrained for first step)
            barge_averaged_depths = calculate_barge_width_depths(section["coords"], section["depths"])
            valid_points = []

            for pt_idx, (pt, depth, avg_depth) in enumerate(zip(section["coords"], section["depths"], barge_averaged_depths)):
                if np.isnan(avg_depth):
                    continue

                # Check line crossing
                crosses = False
                for j, other_section in enumerate(sections):
                    if j == center_idx or j == i:
                        continue
                    if line_segment_intersects_line(starting_point, pt, other_section["coords"]):
                        crosses = True
                        break

                if crosses:
                    continue

                # Check angle (direction-constrained for first step only)
                jump_vec = pt - starting_point
                jump_angle = np.arctan2(jump_vec[1], jump_vec[0])
                angle_diff = np.degrees(jump_angle - perp_angle)

                while angle_diff > 180:
                    angle_diff -= 360
                while angle_diff < -180:
                    angle_diff += 360

                if abs(angle_diff) <= 45:
                    valid_points.append((pt, avg_depth))

            if valid_points:
                deepest = max(valid_points, key=lambda x: x[1])
                reachable.append((i, deepest[0], deepest[1]))

        if not reachable:
            first_jumps.append(None)
        else:
            next_idx, next_pt, next_depth = max(reachable, key=lambda x: x[2])
            shared_visited.add(next_idx)
            first_jumps.append({
                "idx": next_idx,
                "pt": next_pt,
                "depth": next_depth,
                "direction": direction_name
            })

    # PHASE 2: Expand paths independently, without direction constraints
    active_paths = [
        {"coords": [starting_point], "sections": [center_idx], "depths": [starting_depth], "current_idx": center_idx, "current_pt": starting_point, "direction": "upstream"},
        {"coords": [starting_point], "sections": [center_idx], "depths": [starting_depth], "current_idx": center_idx, "current_pt": starting_point, "direction": "downstream"}
    ]

    # Add first jumps if they exist
    for path_idx, jump in enumerate(first_jumps):
        if jump:
            active_paths[path_idx]["coords"].append(jump["pt"])
            active_paths[path_idx]["sections"].append(jump["idx"])
            active_paths[path_idx]["depths"].append(jump["depth"])
            active_paths[path_idx]["current_idx"] = jump["idx"]
            active_paths[path_idx]["current_pt"] = jump["pt"]

    # PHASE 2: Expand paths (no direction constraint). Continue expanding each path until it ends
    path_status = [True, True]  # Track if each path is still active (True = active, False = ended)

    while path_status[0] or path_status[1]:
        # Try to expand path 0
        if path_status[0]:
            current_path = active_paths[0]
            current_idx = current_path["current_idx"]
            current_pt = current_path["current_pt"]

            reachable = []

            # Get perpendicular angles to test (try both directions if needed)
            current_section = sections[current_idx]
            current_axis = current_section["axis"]
            current_axis_angle = np.arctan2(current_axis[1], current_axis[0])

            for i, section in enumerate(sections):
                if i in shared_visited:
                    continue

                # Try perpendicular cone test in both directions (first try +90°, then -90° if blocked)
                cone_clear = False
                for direction_offset in [np.pi / 2, -np.pi / 2]:
                    perp_angle = current_axis_angle + direction_offset
                    if perpendicular_cone_crossing_test(current_pt, perp_angle, sections, current_idx, i):
                        cone_clear = True
                        break

                if not cone_clear:
                    continue

                # Find valid points (NO direction constraint, but still need 45° angle allowance)
                perp_angle = current_axis_angle + np.pi / 2  # Use +90° as reference for angle checking

                barge_averaged_depths = calculate_barge_width_depths(section["coords"], section["depths"])
                valid_points = []

                for pt, depth, avg_depth in zip(section["coords"], section["depths"], barge_averaged_depths):
                    if np.isnan(avg_depth):
                        continue

                    # Check line crossing
                    crosses = False
                    for j, other_section in enumerate(sections):
                        if j == current_idx or j == i:
                            continue
                        if line_segment_intersects_line(current_pt, pt, other_section["coords"]):
                            crosses = True
                            break

                    if crosses:
                        continue

                    # Check angle (allow both perpendicular directions, but must be roughly perpendicular)
                    jump_vec = pt - current_pt
                    jump_angle = np.arctan2(jump_vec[1], jump_vec[0])
                    angle_diff = np.degrees(jump_angle - perp_angle)

                    # Normalize to [-180, 180]
                    while angle_diff > 180:
                        angle_diff -= 360
                    while angle_diff < -180:
                        angle_diff += 360

                    # Account for 180 degree periodicity - accept both perpendicular directions
                    if abs(angle_diff) > 90:
                        angle_diff = 180 - abs(angle_diff)

                    if abs(angle_diff) <= 45:
                        valid_points.append((pt, avg_depth))

                if valid_points:
                    deepest = max(valid_points, key=lambda x: x[1])
                    reachable.append((i, deepest[0], deepest[1]))

            if not reachable:
                path_status[0] = False
            else:
                next_idx, next_pt, next_depth = max(reachable, key=lambda x: x[2])
                shared_visited.add(next_idx)
                current_path["coords"].append(next_pt)
                current_path["sections"].append(next_idx)
                current_path["depths"].append(next_depth)
                current_path["current_idx"] = next_idx
                current_path["current_pt"] = next_pt

        # Try to expand path 1
        if path_status[1]:
            current_path = active_paths[1]
            current_idx = current_path["current_idx"]
            current_pt = current_path["current_pt"]

            reachable = []

            # Get perpendicular angles to test (try both directions if needed)
            current_section = sections[current_idx]
            current_axis = current_section["axis"]
            current_axis_angle = np.arctan2(current_axis[1], current_axis[0])

            for i, section in enumerate(sections):
                if i in shared_visited:
                    continue

                # Try perpendicular cone test in both directions (first try +90°, then -90° if blocked)
                cone_clear = False
                for direction_offset in [np.pi / 2, -np.pi / 2]:
                    perp_angle = current_axis_angle + direction_offset
                    if perpendicular_cone_crossing_test(current_pt, perp_angle, sections, current_idx, i):
                        cone_clear = True
                        break

                if not cone_clear:
                    continue

                # Find valid points (NO direction constraint, but still need 45° angle allowance)
                perp_angle = current_axis_angle + np.pi / 2  # Use +90° as reference for angle checking

                barge_averaged_depths = calculate_barge_width_depths(section["coords"], section["depths"])
                valid_points = []

                for pt, depth, avg_depth in zip(section["coords"], section["depths"], barge_averaged_depths):
                    if np.isnan(avg_depth):
                        continue

                    # Check line crossing
                    crosses = False
                    for j, other_section in enumerate(sections):
                        if j == current_idx or j == i:
                            continue
                        if line_segment_intersects_line(current_pt, pt, other_section["coords"]):
                            crosses = True
                            break

                    if crosses:
                        continue

                    # Check angle (allow both perpendicular directions, but must be roughly perpendicular)
                    jump_vec = pt - current_pt
                    jump_angle = np.arctan2(jump_vec[1], jump_vec[0])
                    angle_diff = np.degrees(jump_angle - perp_angle)

                    # Normalize to [-180, 180]
                    while angle_diff > 180:
                        angle_diff -= 360
                    while angle_diff < -180:
                        angle_diff += 360

                    # Account for 180 degree periodicity - accept both perpendicular directions
                    if abs(angle_diff) > 90:
                        angle_diff = 180 - abs(angle_diff)

                    if abs(angle_diff) <= 45:
                        valid_points.append((pt, avg_depth))

                if valid_points:
                    deepest = max(valid_points, key=lambda x: x[1])
                    reachable.append((i, deepest[0], deepest[1]))

            if not reachable:
                path_status[1] = False
            else:
                next_idx, next_pt, next_depth = max(reachable, key=lambda x: x[2])
                shared_visited.add(next_idx)
                current_path["coords"].append(next_pt)
                current_path["sections"].append(next_idx)
                current_path["depths"].append(next_depth)
                current_path["current_idx"] = next_idx
                current_path["current_pt"] = next_pt

    # Return both paths
    return [
        {
            "coords": np.array(p["coords"]),
            "sections": p["sections"],
            "depths": np.array(p["depths"]),
            "direction": p["direction"]
        }
        for p in active_paths
    ]



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


def extract_cross_sections(gdf, utm_crs, buffer_m=8, simplify_m=5):
    """Extract cross-sections from survey point data."""
    gdf_utm = gdf.to_crs(utm_crs)

    # Build cross-sections with buffer
    poly = gdf_utm.geometry.buffer(buffer_m).unary_union
    fine_dissolved = poly.simplify(simplify_m)

    # Make list of individual cross sections with line ids
    parts = list(fine_dissolved.geoms) if fine_dissolved.geom_type == "MultiPolygon" else [fine_dissolved]
    parts_gdf = gpd.GeoDataFrame({"line_id": range(len(parts))}, geometry=parts, crs=gdf_utm.crs)

    # Label which line id each point lies on
    joined = gpd.sjoin(gdf_utm, parts_gdf, how="inner", predicate="within")

    # Extract cross sections
    sections = []
    for line_id, grp in joined.groupby("line_id"):
        if len(grp) < 5:
            continue
        coords = np.column_stack([grp.geometry.x.values, grp.geometry.y.values])
        depths = grp["depth_ft"].values

        # Check if points within a section are linear
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

    return sections


def merge_collinear_sections(sections):
    """Merge collinear sections together."""
    merged_sections = []
    used = set()
    merge_log = []

    for i, sec1 in enumerate(sections):
        if i in used:
            continue

        merged_coords = [sec1["coords"]]
        merged_depths = [sec1["depths"]]
        merged_indices = [i]
        merged_line_ids = [sec1["line_id"]]

        # Find other sections collinear with this one
        for j, sec2 in enumerate(sections[i+1:], start=i+1):
            if j in used:
                continue

            collinear = are_collinear(sec1["coords"], sec1["centroid"], sec1["axis"],
                                     sec2["coords"], sec2["centroid"], sec2["axis"],
                                     angle_tolerance_deg=5.0)

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
            })
        else:
            sec1["line_ids"] = [sec1["line_id"]]
            merged_sections.append(sec1)

    return merged_sections


def filter_straggler_sections(sections):
    """Remove very small sections compared to main navigable path."""
    if not sections:
        return sections

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
        sections = [sec for sec in sections
                   if np.linalg.norm(sec["coords"][-1] - sec["coords"][0]) >= min_length]

    return sections


def identify_edge_sections(sections):
    """Identify upstream and downstream edge sections."""
    if len(sections) < 2:
        return list(range(len(sections))), [0.0] * len(sections)

    # Get reach axis and origin
    all_centroids = np.array([s["centroid"] for s in sections])
    centered = all_centroids - all_centroids.mean(axis=0)
    cov = np.cov(centered.T)
    eigvals, eigvecs = np.linalg.eigh(cov)
    reach_axis = eigvecs[:, np.argmax(eigvals)]
    reach_origin = all_centroids.mean(axis=0)

    # For each section, count sections on each side
    edge_indices = []
    positions = []

    for i, sec in enumerate(sections):
        my_pos = (sec["centroid"] - reach_origin) @ reach_axis
        positions.append(my_pos)

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

        if upstream_count == 0 or downstream_count == 0:
            edge_indices.append(i)

    return edge_indices, positions


def combine_bidirectional_paths(bidirectional_paths):
    """Combine upstream and downstream paths into a single continuous path."""
    upstream = [p for p in bidirectional_paths if p["direction"] == "upstream"][0]
    downstream = [p for p in bidirectional_paths if p["direction"] == "downstream"][0]

    # Reverse upstream path and concatenate with downstream (avoiding duplicate center point)
    combined_coords = np.vstack([upstream["coords"][::-1], downstream["coords"][1:]])
    combined_sections = upstream["sections"][::-1] + downstream["sections"][1:]
    combined_depths = np.hstack([upstream["depths"][::-1], downstream["depths"][1:]])

    return {
        "coords": combined_coords,
        "sections": combined_sections,
        "depths": combined_depths,
        "upstream": upstream,
        "downstream": downstream,
    }


def find_navigable_path_from_survey(gdf, utm_crs="EPSG:26915", buffer_m=8, simplify_m=5):
    """
    Find the navigable path through a river survey starting from the longest section.

    Args:
        gdf: GeoDataFrame with survey points
        utm_crs: UTM coordinate system (default EPSG:26915)
        buffer_m: Buffer distance for cross-section extraction (default 8m)
        simplify_m: Simplification distance for cross-section polygons (default 5m)

    Returns:
        Dictionary with combined path coordinates, sections, depths, and individual paths
    """
    # Extract cross-sections
    sections = extract_cross_sections(gdf, utm_crs, buffer_m, simplify_m)

    if len(sections) == 0:
        return None

    # Merge collinear sections
    sections = merge_collinear_sections(sections)

    # Filter straggler sections
    sections = filter_straggler_sections(sections)

    if len(sections) == 0:
        return None

    # Identify edge sections
    edge_indices, positions = identify_edge_sections(sections)

    # Find longest section and its deepest point
    longest_idx, longest_length = find_longest_section(sections)
    longest_section = sections[longest_idx]
    starting_point, starting_depth = find_deepest_point_barge_width(
        longest_section["coords"], longest_section["depths"]
    )

    # Find bidirectional paths
    bidirectional_paths = navigable_path_from_longest(sections, longest_idx, starting_point, starting_depth)

    # Combine into single path
    combined_path = combine_bidirectional_paths(bidirectional_paths)

    return combined_path


# ── TEST/DEBUG CODE ────────────────────────────────────────────────────────
if __name__ == "__main__":
    UTM_CRS = "EPSG:26915"
    NAVD88_DIR = Path("NAVD88Files")
    files = sorted(NAVD88_DIR.glob("*_SurveyPoint.gpkg"))

    # loop through all test surveys:
    for fpath in files:
        # Read survey data
        gdf = gpd.read_file(fpath)

        # Get navigable path
        path = find_navigable_path_from_survey(gdf, utm_crs=UTM_CRS)

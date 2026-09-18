"""
Blob (full-coverage multibeam survey) navigable-path and width algorithm, reused by
7_compute_navigable_width.py (compute_blob_vessel_path) and 8_compute_width_by_stage.py
(calculate_width_at_chunk at other depth thresholds along the same path).
"""

import numpy as np
from shapely.geometry import Point, LineString

# ── CONFIG ────────────────────────────────────────────────────────────────────
UTM_CRS = "EPSG:26915"

# Survey parameters
SECTION_WIDTH_FT = 300  # Width of section to average
SECTION_WIDTH_M = SECTION_WIDTH_FT / 3.280839895  # Convert to meters
SAMPLE_INTERVAL_FT = 25  # Test every 25 ft along centerline
SAMPLE_INTERVAL_M = SAMPLE_INTERVAL_FT / 3.280839895
CROSS_SECTION_SPACING_FT = 300  # Spacing between cross-sections
CROSS_SECTION_SPACING_M = CROSS_SECTION_SPACING_FT / 3.280839895  # Convert to meters


def find_cross_section_angle(gdf_utm, point_x, point_y, prev_angle=None):
    """
    Find the angle of the cross section at a given point.

    If prev_angle is provided, uses hill-climbing from that angle.
    Otherwise tests all angles 0-180° to find minimum line length.

    Returns: angle in degrees (0-180)
    """
    BUFFER_DIST_M = 50 / 3.280839895  # 50ft in meters
    points_x = gdf_utm.geometry.x.values
    points_y = gdf_utm.geometry.y.values

    def get_line_length(angle_deg):
        """Calculate line length through point at given angle."""
        angle_rad = np.radians(angle_deg)
        dx = np.cos(angle_rad)
        dy = np.sin(angle_rad)

        # Find extent in positive direction (test every 50m for speed)
        max_dist_pos = 0
        for dist_m in np.arange(0, 1000, 50):
            test_x = point_x + dist_m * dx
            test_y = point_y + dist_m * dy
            dists = np.sqrt((points_x - test_x)**2 + (points_y - test_y)**2)
            if np.any(dists <= BUFFER_DIST_M):
                max_dist_pos = dist_m

        # Find extent in negative direction
        max_dist_neg = 0
        for dist_m in np.arange(0, 1000, 50):
            test_x = point_x - dist_m * dx
            test_y = point_y - dist_m * dy
            dists = np.sqrt((points_x - test_x)**2 + (points_y - test_y)**2)
            if np.any(dists <= BUFFER_DIST_M):
                max_dist_neg = dist_m

        return max_dist_pos + max_dist_neg

    # If previous angle provided, use hill-climbing from that angle
    if prev_angle is not None:
        base_length = get_line_length(prev_angle)
        best_angle = prev_angle
        best_length = base_length

        # Test 10 degrees in positive direction
        test_angle_pos = (prev_angle + 10) % 180
        length_pos = get_line_length(test_angle_pos)

        if length_pos < best_length:
            # Keep going in positive direction
            best_angle = test_angle_pos
            best_length = length_pos
            while True:
                test_angle = (best_angle + 10) % 180
                test_length = get_line_length(test_angle)
                if test_length < best_length:
                    best_angle = test_angle
                    best_length = test_length
                else:
                    break
        else:
            # Test 10 degrees in negative direction
            test_angle_neg = (prev_angle - 10) % 180
            length_neg = get_line_length(test_angle_neg)

            if length_neg < best_length:
                # Keep going in negative direction
                best_angle = test_angle_neg
                best_length = length_neg
                while True:
                    test_angle = (best_angle - 10) % 180
                    test_length = get_line_length(test_angle)
                    if test_length < best_length:
                        best_angle = test_angle
                        best_length = test_length
                    else:
                        break

        return best_angle

    # First step: test all angles
    best_angle = 0
    best_length = float('inf')

    for angle in np.arange(0, 180, 10):
        length = get_line_length(angle)
        if length < best_length:
            best_length = length
            best_angle = angle

    return best_angle


def find_deepest_section_vertical(gdf_utm, centerline_x, survey_hull=None):
    """
    Find the deepest 300ft section along a vertical centerline (same as horizontal but with X axis).

    Tests at 25ft intervals along the centerline, buffering 150ft around each point.
    Only tests points where the entire buffer stays within the survey area.

    Returns: (best_x, best_y, best_avg_depth, all_results)
    """
    bounds = gdf_utm.total_bounds
    min_y, max_y = bounds[1], bounds[3]

    buffer_radius_m = SECTION_WIDTH_M / 2

    # Create convex hull of survey if not provided
    if survey_hull is None:
        survey_hull = gdf_utm.geometry.unary_union.convex_hull

    # Create centerline as a vertical line
    centerline = LineString([(centerline_x, min_y), (centerline_x, max_y)])

    # Clip centerline to hull
    centerline_clipped = centerline.intersection(survey_hull)
    if centerline_clipped.is_empty:
        return None, None, 0, []

    results = []

    # Sample every 25ft along the centerline
    for distance in np.arange(0, centerline.length, SAMPLE_INTERVAL_M):
        sample_pt = centerline.interpolate(distance)
        sample_x, sample_y = sample_pt.x, sample_pt.y

        # Create circular buffer around the point
        buffer = sample_pt.buffer(buffer_radius_m)

        # Check if buffer is fully within the survey hull
        if not buffer.within(survey_hull):
            continue

        # Find survey points within buffer
        points_in_buffer = gdf_utm[gdf_utm.geometry.within(buffer)]

        if len(points_in_buffer) > 0:
            avg_depth = points_in_buffer["depth_ft"].mean()
            results.append({
                'x': sample_x,
                'y': sample_y,
                'distance': distance,
                'avg_depth': avg_depth,
                'num_points': len(points_in_buffer)
            })

    if not results:
        return None, None, 0, results

    # Find deepest section
    best_result = max(results, key=lambda r: r['avg_depth'])

    return best_result['x'], best_result['y'], best_result['avg_depth'], results


def find_deepest_section(gdf_utm, centerline_y, survey_hull=None):
    """
    Find the deepest 300ft section along the horizontal centerline.

    Tests at 25ft intervals along the centerline, buffering 150ft around
    each point to capture nearby survey points. Only tests points where
    the entire buffer stays within the survey area (defined by convex hull).

    Returns: (best_x, best_y, best_avg_depth, all_results)
    """
    bounds = gdf_utm.total_bounds
    min_x, max_x = bounds[0], bounds[2]

    buffer_radius_m = SECTION_WIDTH_M / 2

    # Create convex hull of survey if not provided
    if survey_hull is None:
        survey_hull = gdf_utm.geometry.unary_union.convex_hull

    # Create centerline as a horizontal line
    centerline = LineString([(min_x, centerline_y), (max_x, centerline_y)])

    # Clip centerline to hull to get valid testing region
    centerline_clipped = centerline.intersection(survey_hull)
    if centerline_clipped.is_empty:
        return None, None, 0, []

    results = []

    # Get the extent of the clipped centerline
    if hasattr(centerline_clipped, 'coords'):
        # Single line segment
        clipped_coords = list(centerline_clipped.coords)
    else:
        # Multiple segments, use bounds
        clipped_coords = None

    # Sample every 25ft along the centerline
    for distance in np.arange(0, centerline.length, SAMPLE_INTERVAL_M):
        # Get point on centerline
        sample_pt = centerline.interpolate(distance)
        sample_x, sample_y = sample_pt.x, sample_pt.y

        # Create circular buffer around the point (150ft = half of 300ft section)
        buffer = sample_pt.buffer(buffer_radius_m)

        # Check if buffer is fully within the survey hull
        if not buffer.within(survey_hull):
            continue

        # Find survey points within buffer
        points_in_buffer = gdf_utm[gdf_utm.geometry.within(buffer)]

        if len(points_in_buffer) > 0:
            avg_depth = points_in_buffer["depth_ft"].mean()
            results.append({
                'x': sample_x,
                'y': sample_y,
                'distance': distance,
                'avg_depth': avg_depth,
                'num_points': len(points_in_buffer)
            })

    if not results:
        return None, None, 0, results

    # Find deepest section
    best_result = max(results, key=lambda r: r['avg_depth'])

    return best_result['x'], best_result['y'], best_result['avg_depth'], results


def build_path_direction(gdf_utm, start_y, start_point, direction, survey_hull, fixed_angle=None):
    """
    Build a continuous path in one direction by iteratively jumping to deepest sections.

    At each step, finds cross-section angle using hill-climbing from previous angle.
    Filters candidates to be within ±45° of perpendicular to that cross-section.

    fixed_angle: if given (the "horizontal"/"vertical" review modes -- see
    get_navigable_path), skips the per-step hill-climbing entirely and uses this
    constant as the cross-section angle for every step, so "perpendicular to the
    cross-section" never drifts from the survey's initial horizontal/vertical line.

    Returns: (path_list, angle_dict) where angle_dict maps point coords to cross-section angles
    """
    path = []
    angles = {}  # Cache cross-section angles
    current_y = start_y + direction * CROSS_SECTION_SPACING_M
    prev_point = start_point
    prev_angle = None  # For hill-climbing
    direction_name = "upstream" if direction == 1 else "downstream"

    while True:
        x, y, depth, all_results = find_deepest_section(gdf_utm, current_y, survey_hull)

        if x is None:
            print(f"    {direction_name.capitalize()} path ended at step {len(path) + 1} (no valid section)")
            break

        if fixed_angle is not None:
            cross_section_angle = fixed_angle
        else:
            # Find cross-section angle at current point (use hill-climbing from previous angle)
            if prev_point not in angles:
                angles[prev_point] = find_cross_section_angle(gdf_utm, prev_point[0], prev_point[1], prev_angle)
            cross_section_angle = angles[prev_point]
            prev_angle = cross_section_angle  # Store for next iteration

        # Calculate perpendicular direction(s) to cross section (in 0-360 space)
        perp_angle1 = (cross_section_angle + 90) % 360
        perp_angle2 = (cross_section_angle - 90) % 360

        # Choose perpendicular closest to our direction of travel
        target_angle = 90 if direction == 1 else 270

        # Pick the perpendicular closer to our target direction
        diff1 = abs(perp_angle1 - target_angle)
        if diff1 > 180:
            diff1 = 360 - diff1

        diff2 = abs(perp_angle2 - target_angle)
        if diff2 > 180:
            diff2 = 360 - diff2

        target_perp = perp_angle1 if diff1 < diff2 else perp_angle2

        # Filter candidates: must be within ±45° of perpendicular direction
        valid_candidates = []

        for result in all_results:
            # Calculate angle from prev_point to candidate
            dx = result['x'] - prev_point[0]
            dy = result['y'] - prev_point[1]
            candidate_angle = np.degrees(np.arctan2(dy, dx))

            # Normalize to 0-180 for comparison with target_perp
            if candidate_angle < 0:
                candidate_angle += 360

            # Check if within ±45° of target perpendicular
            angle_diff = abs(candidate_angle - target_perp)
            if angle_diff > 180:
                angle_diff = 360 - angle_diff

            if angle_diff <= 45:
                valid_candidates.append(result)

        if not valid_candidates:
            break

        # Pick deepest among valid candidates
        best_result = max(valid_candidates, key=lambda r: r['avg_depth'])
        x, y, depth = best_result['x'], best_result['y'], best_result['avg_depth']
        current_point = (x, y)

        path.append((x, y, depth))
        prev_point = current_point

        if len(path) % 5 == 0:
            print(f"    {direction_name.capitalize()} step {len(path)}: {depth:.1f} ft")

        current_y += direction * CROSS_SECTION_SPACING_M

    return path, angles


def get_navigable_path(gdf_utm, mode="original"):
    """
    Find the complete navigable path for a survey using bidirectional pathfinding.

    mode (set in 6_review_surveys.py's per-blob-survey orientation choice, read off
    bathym_fixed.csv's blob_path_mode column by 7_compute_navigable_width.py):
      - "original" (default): detect orientation from the survey's own bounding box
        (wider than tall -> vertical lines; taller than wide -> horizontal lines) and
        hill-climb the cross-section angle at every step, exactly as originally built.
      - "horizontal": force horizontal test lines, always travelling up/down (Y), with
        the cross-section angle FIXED at 0 deg (perfectly horizontal) for every step --
        no per-step hill-climbing -- so "perpendicular to the cross-section" is always
        exactly vertical (90/270 deg), never drifting off that.
      - "vertical": force vertical test lines, always travelling left/right (X), with
        the cross-section angle FIXED at 90 deg (perfectly vertical) for every step, so
        travel is always exactly horizontal (0/180 deg).
      - "original_horizontal" / "original_vertical": same hill-climbing behavior as
        "original" (per-step angle recompute stays on, unlike "horizontal"/"vertical"'s
        fixed angle), but always starts from the center HORIZONTAL or VERTICAL line
        respectively, instead of "original"'s width-vs-height auto-detect choosing for
        you. Added 2026-08-31 (the first use, "original_vertical", was called
        "vertical_start" before this pair existed) for cases where the auto-detected
        orientation wasn't the one wanted but the fully-fixed "horizontal"/"vertical"
        mode was too rigid (no hill-climb) -- e.g. converted cross-section surveys run
        through the blob pathway, where a reviewer picks the starting line by eye in
        6_review_surveys.py.
    Both fixed-angle modes still hop to the deepest 300ft section every
    CROSS_SECTION_SPACING_FT and still restrict the next hop to within +/-45 deg of
    perpendicular (see build_path_direction/build_path_direction_vertical) -- only the
    per-step angle recompute is skipped, in favor of the survey's chosen starting line.

    Returns: {
        'complete_path': list of (x, y, depth) tuples from downstream to upstream,
        'center_point': (x, y, depth) of center,
        'upstream_path': list of upstream points,
        'downstream_path': list of downstream points,
        'cached_angles': dict mapping point coordinates to cross-section angles
    }
    """
    bounds = gdf_utm.total_bounds  # [minx, miny, maxx, maxy]
    min_x, min_y, max_x, max_y = bounds

    if mode == "horizontal":
        print("  Blob path mode: horizontal (forced) - using horizontal lines")
        mid_y = (min_y + max_y) / 2
        center_pt, upstream_path, downstream_path, cached_angles = find_bidirectional_path(
            gdf_utm, mid_y, fixed_angle=0.0)
    elif mode == "vertical":
        print("  Blob path mode: vertical (forced) - using vertical lines")
        mid_x = (min_x + max_x) / 2
        center_pt, upstream_path, downstream_path, cached_angles = find_bidirectional_path_vertical(
            gdf_utm, mid_x, fixed_angle=90.0)
    elif mode == "original_horizontal":
        print("  Blob path mode: original_horizontal (hill-climbing, forced horizontal center line)")
        mid_y = (min_y + max_y) / 2
        center_pt, upstream_path, downstream_path, cached_angles = find_bidirectional_path(gdf_utm, mid_y)
    elif mode == "original_vertical":
        print("  Blob path mode: original_vertical (hill-climbing, forced vertical center line)")
        mid_x = (min_x + max_x) / 2
        center_pt, upstream_path, downstream_path, cached_angles = find_bidirectional_path_vertical(gdf_utm, mid_x)
    else:
        width = max_x - min_x
        height = max_y - min_y
        # Detect orientation: if wider than tall, use vertical lines; otherwise horizontal
        if width > height:
            print(f"  Survey is horizontal (width {width:.0f}m > height {height:.0f}m) - using vertical lines")
            mid_x = (min_x + max_x) / 2
            center_pt, upstream_path, downstream_path, cached_angles = find_bidirectional_path_vertical(gdf_utm, mid_x)
        else:
            print(f"  Survey is vertical (height {height:.0f}m >= width {width:.0f}m) - using horizontal lines")
            mid_y = (min_y + max_y) / 2
            center_pt, upstream_path, downstream_path, cached_angles = find_bidirectional_path(gdf_utm, mid_y)
    print(f"  Found center point + {len(upstream_path)} upstream + {len(downstream_path)} downstream")

    if center_pt[0] is None:
        return None

    complete_path = build_complete_path(center_pt, upstream_path, downstream_path)

    return {
        'complete_path': complete_path,
        'center_point': center_pt,
        'upstream_path': upstream_path,
        'downstream_path': downstream_path,
        'cached_angles': cached_angles
    }


def get_minimum_width(gdf_utm, path, depth_threshold=9):
    """
    Calculate the minimum navigable width along a path.

    For each chunk of the path, calculates the width of water with sufficient depth.
    Returns: {
        'min_width': minimum total width (ft),
        'min_half_width': minimum half-width (ft),
        'location': (x, y) of the bottleneck midpoint,
        'all_widths': list of width data for each chunk
    }
    """
    if not path or len(path) < 2:
        return None

    print(f"  Calculating widths at {len(path) - 1} chunks...")
    widths = []

    for i in range(len(path) - 1):
        width_data = calculate_width_at_chunk(gdf_utm, path[i], path[i + 1], depth_threshold)
        widths.append(width_data)
        if (i + 1) % 5 == 0:
            print(f"    Completed {i + 1}/{len(path) - 1} chunks")

    if not widths:
        return None

    # Find bottleneck
    min_width_overall = min(w['min_width'] for w in widths)
    min_total_width = min(w['total_width'] for w in widths)
    bottleneck_idx = min(range(len(widths)), key=lambda i: widths[i]['total_width'])
    bottleneck_location = widths[bottleneck_idx]['midpoint']

    result = {
        'min_width': min_width_overall,
        'min_total_width': min_total_width,
        'location': bottleneck_location,
        'all_widths': widths
    }

    return result


def build_complete_path(center_pt, upstream_path, downstream_path):
    """
    Combine upstream, center, and downstream paths into one continuous path.

    Returns: list of (x, y, depth) tuples from downstream to upstream
    """
    # Reverse downstream so it goes from center outward
    reversed_downstream = list(reversed(downstream_path))
    complete_path = reversed_downstream + [center_pt] + upstream_path
    return complete_path


def calculate_width_at_chunk(gdf_utm, point_a, point_b, depth_threshold=9):
    """
    Calculate navigable width at the midpoint of a path chunk.

    For the line from point_a to point_b:
    - Find midpoint
    - Calculate perpendicular direction
    - Extend perpendicular in both directions while maintaining 9ft+ depth
    - Return (width_left, width_right, min_width, midpoint)
    """
    DEPTH_THRESHOLD_FT = depth_threshold

    # Midpoint of chunk
    mid_x = (point_a[0] + point_b[0]) / 2
    mid_y = (point_a[1] + point_b[1]) / 2

    # Direction of chunk
    chunk_angle = np.arctan2(point_b[1] - point_a[1], point_b[0] - point_a[0])

    # Perpendicular direction (90 degrees from chunk)
    perp_angle = chunk_angle + np.pi / 2

    dx = np.cos(perp_angle)
    dy = np.sin(perp_angle)

    # Find extent in positive perpendicular direction (9ft+ depth)
    width_pos = 0
    for dist_m in np.arange(0, 500, 10):  # Test up to 500m, every 10m
        test_x = mid_x + dist_m * dx
        test_y = mid_y + dist_m * dy

        # Find points within small buffer around test point
        test_point = Point(test_x, test_y)
        buffer = test_point.buffer(15.24)  # 50ft buffer for sampling
        nearby_points = gdf_utm[gdf_utm.geometry.within(buffer)]

        if len(nearby_points) > 0:
            avg_depth = nearby_points["depth_ft"].mean()
            if avg_depth >= DEPTH_THRESHOLD_FT:
                width_pos = dist_m * 3.280839895  # Convert to feet
            else:
                break
        else:
            break

    # Find extent in negative perpendicular direction
    width_neg = 0
    for dist_m in np.arange(0, 500, 10):
        test_x = mid_x - dist_m * dx
        test_y = mid_y - dist_m * dy

        test_point = Point(test_x, test_y)
        buffer = test_point.buffer(15.24)
        nearby_points = gdf_utm[gdf_utm.geometry.within(buffer)]

        if len(nearby_points) > 0:
            avg_depth = nearby_points["depth_ft"].mean()
            if avg_depth >= DEPTH_THRESHOLD_FT:
                width_neg = dist_m * 3.280839895
            else:
                break
        else:
            break

    total_width = width_pos + width_neg
    min_width = min(width_pos, width_neg)

    return {
        'width_left': width_neg,
        'width_right': width_pos,
        'total_width': total_width,
        'min_width': min_width,
        'midpoint': (mid_x, mid_y),
        'perpendicular_angle': perp_angle
    }


def build_path_direction_vertical(gdf_utm, start_x, start_point, direction, survey_hull, fixed_angle=None):
    """
    Build a continuous path in one direction using vertical lines (X-axis version).

    At each step, finds cross-section angle using hill-climbing from previous angle.
    Filters candidates to be within ±45° of perpendicular to that cross-section.

    fixed_angle: see build_path_direction's docstring -- same constant-angle override.

    Returns: (path_list, angle_dict)
    """
    path = []
    angles = {}
    current_x = start_x + direction * CROSS_SECTION_SPACING_M
    prev_point = start_point
    prev_angle = None
    direction_name = "right" if direction == 1 else "left"

    while True:
        x, y, depth, all_results = find_deepest_section_vertical(gdf_utm, current_x, survey_hull)

        if x is None:
            print(f"    {direction_name.capitalize()} path ended at step {len(path) + 1} (no valid section)")
            break

        if fixed_angle is not None:
            cross_section_angle = fixed_angle
        else:
            if prev_point not in angles:
                angles[prev_point] = find_cross_section_angle(gdf_utm, prev_point[0], prev_point[1], prev_angle)
            cross_section_angle = angles[prev_point]
            prev_angle = cross_section_angle

        # Calculate perpendicular direction(s) to cross section (in 0-360 space)
        perp_angle1 = (cross_section_angle + 90) % 360
        perp_angle2 = (cross_section_angle - 90) % 360

        # Choose perpendicular closest to our direction of travel
        target_angle = 0 if direction == 1 else 180  # Right (0°) vs Left (180°)

        diff1 = abs(perp_angle1 - target_angle)
        if diff1 > 180:
            diff1 = 360 - diff1

        diff2 = abs(perp_angle2 - target_angle)
        if diff2 > 180:
            diff2 = 360 - diff2

        target_perp = perp_angle1 if diff1 < diff2 else perp_angle2

        # Filter candidates by angle constraint
        valid_candidates = []

        for result in all_results:
            dx = result['x'] - prev_point[0]
            dy = result['y'] - prev_point[1]
            candidate_angle = np.degrees(np.arctan2(dy, dx))

            if candidate_angle < 0:
                candidate_angle += 360

            angle_diff = abs(candidate_angle - target_perp)
            if angle_diff > 180:
                angle_diff = 360 - angle_diff

            if angle_diff <= 45:
                valid_candidates.append(result)

        if not valid_candidates:
            break

        best_result = max(valid_candidates, key=lambda r: r['avg_depth'])
        x, y, depth = best_result['x'], best_result['y'], best_result['avg_depth']
        current_point = (x, y)

        path.append((x, y, depth))
        prev_point = current_point

        if len(path) % 5 == 0:
            print(f"    {direction_name.capitalize()} step {len(path)}: {depth:.1f} ft")

        current_x += direction * CROSS_SECTION_SPACING_M

    return path, angles


def find_bidirectional_path_vertical(gdf_utm, center_x, fixed_angle=None):
    """
    Find deepest sections in both directions from center using vertical lines.

    fixed_angle: see build_path_direction_vertical's docstring.

    Returns: (center_pt, right_path, left_path, all_angles_dict)
    """
    survey_hull = gdf_utm.geometry.unary_union.convex_hull

    # Find deepest at center
    cx, cy, c_depth, _ = find_deepest_section_vertical(gdf_utm, center_x, survey_hull)
    center_pt = (cx, cy, c_depth)

    if cx is None:
        return center_pt, [], [], {}

    # Build right path (direction = 1 for increasing X)
    right_path, right_angles = build_path_direction_vertical(gdf_utm, center_x, (cx, cy), 1, survey_hull, fixed_angle)

    # Build left path (direction = -1 for decreasing X)
    left_path, left_angles = build_path_direction_vertical(gdf_utm, center_x, (cx, cy), -1, survey_hull, fixed_angle)

    # Combine all cached angles
    center_angle = fixed_angle if fixed_angle is not None else find_cross_section_angle(gdf_utm, cx, cy)
    all_angles = {(cx, cy): center_angle}
    all_angles.update(right_angles)
    all_angles.update(left_angles)

    return center_pt, right_path, left_path, all_angles


def find_bidirectional_path(gdf_utm, center_y, fixed_angle=None):
    """
    Find deepest sections in both directions from center, building continuous paths.

    fixed_angle: see build_path_direction's docstring.

    Returns: (center_pt, upstream_path, downstream_path, all_angles_dict)
    """
    # Create convex hull of survey area once (actual survey shape, not bounding box)
    survey_hull = gdf_utm.geometry.unary_union.convex_hull

    # Find deepest at center
    cx, cy, c_depth, _ = find_deepest_section(gdf_utm, center_y, survey_hull)
    center_pt = (cx, cy, c_depth)

    if cx is None:
        return center_pt, [], [], {}

    # Build upstream path (direction = 1 for increasing Y)
    upstream_path, upstream_angles = build_path_direction(gdf_utm, center_y, (cx, cy), 1, survey_hull, fixed_angle)

    # Build downstream path (direction = -1 for decreasing Y)
    downstream_path, downstream_angles = build_path_direction(gdf_utm, center_y, (cx, cy), -1, survey_hull, fixed_angle)

    # Combine all cached angles
    center_angle = fixed_angle if fixed_angle is not None else find_cross_section_angle(gdf_utm, cx, cy)
    all_angles = {(cx, cy): center_angle}
    all_angles.update(upstream_angles)
    all_angles.update(downstream_angles)

    return center_pt, upstream_path, downstream_path, all_angles



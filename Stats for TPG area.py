#!/usr/bin/env python3
"""Print various stats on an area used for a TPG spinoff."""

from argparse import ArgumentParser, BooleanOptionalAction
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyproj
import shapely
from shapely import MultiPolygon, Point, Polygon, ops
from travelpygame.util import (
	detect_cat_cols,
	format_area,
	format_distance,
	format_point,
	get_area,
	get_extreme_corner_points,
	get_extreme_points,
	get_polygons,
	get_projected_crs,
	get_transform_methods,
	read_geodataframe,
)

from lib.stats import get_longest_distance

if TYPE_CHECKING:
	import geopandas
	from pandas import Series


def _join_unique(s: 'Series', joiner: str = ', '):
	return joiner.join(str(item) for item in s.dropna().unique())


def print_point_info(
	name: str,
	gdf: 'geopandas.GeoDataFrame',
	metric_poly: MultiPolygon | Polygon,
	metric_point: shapely.Point,
	metric_to_wgs84: Any | None,
):
	point = (
		metric_point if metric_to_wgs84 is None else ops.transform(metric_to_wgs84, metric_point)
	)
	print(f'{name.capitalize()}:', format_point(point))
	# Ah, whoops, you don't necessarily want contains() here because that doesn't include points right on the boundary of a polygon
	subset = gdf[gdf.intersects(point)].drop(
		columns=['area', gdf.active_geometry_name or 'geometry'], errors='ignore'
	)
	if subset.empty:
		metric_nearest = ops.nearest_points(metric_point, metric_poly)[1]
		if metric_nearest == metric_point:
			# Give up
			return
		nearest = (
			metric_nearest
			if metric_to_wgs84 is None
			else ops.transform(metric_to_wgs84, metric_nearest)
		)
		print(f'Closest in area to {name}:', format_point(nearest))
		subset = gdf[gdf.intersects(nearest)].drop(
			columns=['area', gdf.active_geometry_name or 'geometry'], errors='ignore'
		)
		if subset.empty:
			print('huh?? nearest_points returned something not actually in the thing')
			return
	info = [f'{col_name}: {_join_unique(col)}' for col_name, col in subset.items() if not col.empty]
	if info:
		print('\t' + '\t'.join(info))


def print_projected_things(
	gdf: 'geopandas.GeoDataFrame',
	poly: MultiPolygon | Polygon,
	metric_to_wgs84: Any,
	metric_crs: Any,
):
	# poly is assumed to already be in a metric CRS at this point
	centroid = poly.centroid
	print_point_info('centroid', gdf, poly, centroid, metric_to_wgs84)

	rep_point = poly.representative_point()
	print_point_info('representative point', gdf, poly, rep_point, metric_to_wgs84)

	max_inscribed_circle = shapely.maximum_inscribed_circle(poly)
	pole_of_inaccessibility = shapely.get_point(max_inscribed_circle, 0)
	print('Max inscribed circle radius:', format_distance(shapely.length(max_inscribed_circle)))
	print_point_info('pole of inaccessibility', gdf, poly, pole_of_inaccessibility, metric_to_wgs84)

	min_bounding_circle = shapely.minimum_bounding_circle(poly)
	min_circle_centroid = min_bounding_circle.centroid
	# Minimized maximum distance to points? Apparently
	print_point_info(
		'minimum bounding circle centroid', gdf, poly, min_circle_centroid, metric_to_wgs84
	)

	min_x, min_y, max_x, max_y = poly.bounds
	bounding_box = shapely.box(min_x, min_y, max_x, max_y)
	print('Bounding box size:', format_area(bounding_box.area))
	diagonal_dist = shapely.distance(shapely.Point(min_x, min_y), shapely.Point(max_x, max_y))
	print('Bounding box diagonal distance:', format_distance(diagonal_dist))

	# We don't really need to be doing this in projected space, oh well
	convex_hull = poly.convex_hull
	assert isinstance(convex_hull, (Polygon, MultiPolygon)), type(convex_hull)
	print('Convex hull size:', format_area(convex_hull.area))
	max_convex_dist = get_longest_distance(convex_hull)
	print('Longest distance inside convex hull:', format_distance(max_convex_dist))
	convex_centroid = convex_hull.centroid
	print_point_info('convex hull centroid', gdf, poly, convex_centroid, metric_to_wgs84)

	multipoly = MultiPolygon(get_polygons(gdf))
	extremes = get_extreme_points(
		multipoly, gdf.crs, find_centre_points=True, force_non_contained_centre_points=True
	)
	for name, point in extremes.items():
		assert isinstance(name, str), f'name is {type(name)}'
		assert isinstance(point, Point), f'point is {type(point)}'
		print_point_info(name, gdf, multipoly, point, None)
	extremes = get_extreme_corner_points(multipoly, gdf.crs, metric_crs=metric_crs)
	for name, point in extremes.items():
		assert isinstance(name, str), f'name is {type(name)}'
		assert isinstance(point, Point), f'point is {type(point)}'
		print_point_info(name, gdf, multipoly, point, None)


def print_cat_stats(
	gdf: 'geopandas.GeoDataFrame', cat_cols: list[str], total_area: float | None = None
):
	"""🐈"""
	if total_area is None:
		total_area = gdf['area'].sum()

	for cat_col in cat_cols:
		grouper = gdf.groupby(cat_col, sort=False)['area']
		areas = grouper.sum().sort_values(ascending=False).to_frame()
		areas['percent'] = (areas / total_area).map('{:%}'.format)  # pyright: ignore[reportOperatorIssue] #but it's never None though?
		areas['area'] = areas['area'].map(format_area)
		n = grouper.size()
		areas['count'] = n
		areas['count_percent'] = n / gdf.index.size
		print(areas)


def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument('path', type=Path, help='Path to geojson/gpkg/etc file')
	argparser.add_argument(
		'--category',
		nargs='*',
		help='Column name (specified multiple times) containing stats to print a breakdown per area or info for extreme/interesting points, otherwise attempts to autodetect',
	)
	argparser.add_argument(
		'--print-category-stats',
		action=BooleanOptionalAction,
		default=False,
		help='Print breakdown of categories by area/percentage of total area/etc',
	)
	argparser.add_argument(
		'--metres-crs',
		'--metric-crs',
		help='Override CRS used for area computations, should be something with metres as the unit',
	)
	argparser.add_argument(
		'--area-from-crs',
		action=BooleanOptionalAction,
		default=False,
		help='Use the metric CRS to calculate the area of polygons instead of the WGS84 geod, defaults to False',
	)
	argparser.add_argument(
		'--print-invalidity',
		action=BooleanOptionalAction,
		default=False,
		help='Print some info about geometries being invalid after reprojecting, or coverage being invalid, but this seems to be too strict and never works so it is false by default',
	)
	args = argparser.parse_args()

	path = args.path
	print_invalidity: bool = args.print_invalidity
	gdf = read_geodataframe(path)
	west, south, east, north = gdf.total_bounds
	print(f'Latitude: {north} to {south}')
	print(f'Longitude: {west} to {east}')
	if args.metres_crs:
		metric_crs = pyproj.CRS.from_user_input(args.metres_crs)
	else:
		metric_crs = get_projected_crs((west, south, east, north)) or gdf.estimate_utm_crs()
		print(
			f'Autodetected metric CRS: {metric_crs.name} {metric_crs.list_authority()} {metric_crs.scope} {metric_crs.remarks}'
		)

	cat_cols: list[str] = args.category
	if not cat_cols:
		cat_cols = detect_cat_cols(gdf)

	if print_invalidity:
		invalid_reasons = gdf.is_valid_reason()
		invalid_reasons = invalid_reasons[invalid_reasons != 'Valid Geometry']
		if not invalid_reasons.empty:
			print('Invalid geometries:')
			print(invalid_reasons)

	metres = gdf.to_crs(metric_crs)
	gdf['area'] = metres['area'] = (
		metres.area if args.area_from_crs else gdf.geometry.map(get_area, na_action='ignore')
	)
	total_area = metres['area'].sum()
	print('Total area:', format_area(total_area))

	if args.print_category_stats:
		print_cat_stats(gdf, cat_cols)

	if print_invalidity:
		# I don't know why this happens for things which seem perfectly valid
		invalid_reasons = metres.is_valid_reason().rename('invalid_reason')
		is_invalid = invalid_reasons != 'Valid Geometry'
		invalid_reasons = invalid_reasons[is_invalid]
		if not invalid_reasons.empty:
			print('Invalid geometries after converting to metres CRS:')
			print(is_invalid.to_string())
			metres = metres.drop(index=invalid_reasons.index)

	if print_invalidity:
		coverage_valid = metres.is_valid_coverage()
		if not coverage_valid:
			print(
				'Does not form a valid coverage, which might be fine but might have unexpected results'
			)
			# To be honest, is_valid_coverage is very strict, not a lot actually does have a complete lack of gaps apparently
	else:
		coverage_valid = False

	metric_poly = MultiPolygon(get_polygons(metres))

	metric_to_wgs84 = get_transform_methods(metric_crs, 'wgs84')[0]
	print_projected_things(gdf, metric_poly, metric_to_wgs84, metric_crs)


if __name__ == '__main__':
	main()

#!/usr/bin/env python3
"""Generate some stats for all the locations from a file, or a user's known submissions."""

import asyncio
import logging
from argparse import ZERO_OR_MORE, ArgumentParser, BooleanOptionalAction
from collections.abc import Hashable
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import geopandas
import numpy
import pandas
import shapely
from aiohttp import ClientSession
from travelpygame.point_set_stats import (
	find_furthest_point,
	find_geometric_median,
	get_total_uniqueness,
	get_uniqueness,
)
from travelpygame.util import (
	circular_mean_xy,
	fix_x_coord,
	fix_y_coord,
	format_area,
	get_area,
	get_geometry_antipode,
	get_point_antipodes,
)
from travelpygame.util.distance import cartesian_product_distances, geod_distance
from travelpygame.util.formatting import (
	format_dataframe,
	format_distance,
	format_number,
	format_point,
)
from travelpygame.util.io_utils import geometry_to_file_async, output_dataframe, output_geodataframe
from travelpygame.util.pandas_utils import detect_cat_cols

from lib.format_utils import describe_point
from lib.io_utils import load_point_set_from_arg, load_polygons

if TYPE_CHECKING:
	from shapely.geometry.base import BaseGeometry
	from travelpygame.point_set import PointSet

logger = logging.getLogger(__name__)


async def _maybe_describe_point(point: shapely.Point, session: ClientSession | None):
	if session is None:
		return format_point(point)
	return await describe_point(point, session, include_coords=True)


async def print_point(
	point_set: 'PointSet',
	point: shapely.Point,
	name: str,
	session: ClientSession | None,
	*,
	get_median: bool = False,
):
	desc = await _maybe_describe_point(point, session)
	print(f'{name}:', desc)
	distances = point_set.get_all_distances(point)
	if not point_set.contains(point):
		dist = distances.iloc[0]
		closest = distances.index[0]
		print(f'Closest to: {closest}, {format_distance(dist)} away')

	furthest_dist = distances.iloc[-1]
	furthest = distances.index[-1]
	print(f'Furthest from: {furthest}, {format_distance(furthest_dist)} away')

	if get_median:
		median_index = distances.size // 2
		median = distances.iloc[median_index]
		print(
			f'Point at median distance: {distances.index[median_index]}, {format_distance(median)} away'
		)

	print()


@dataclass
class PointSetStats:
	# centres
	circular_mean: shapely.Point
	arithmetic_mean: shapely.Point
	"""Just the mean of all the lat/lng coordinates"""
	arithmetic_median: shapely.Point
	closest_to_bbox: shapely.Point
	raw_centroid: shapely.Point
	"""Computed using normal geometric CRS, so technically wrong and assumes flat earth"""
	centroid: shapely.Point
	"""Computed using projected CRS"""
	centre_of_extremes: shapely.Point
	geometric_median: shapely.Point | None
	"""Optional since it can take some time to compute"""
	antipoint: shapely.Point | None
	"""Furthest possible point from anywhere on earth, optional since it can take some time to compute"""

	# extreme points
	# Maybe some of these shouldn't be tuples and should instead be separated into two attributes
	westmost: tuple[float, list[Hashable]]
	"""(longitude, list of indexes at this longitude). Assumes the earth is flat and that -180 longitude is the edge of the planet, because WGS84"""
	eastmost: tuple[float, list[Hashable]]
	"""(longitude, list of indexes at this longitude). Assumes the earth is flat and that 180 longitude is the edge of the planet, because WGS84"""
	northmost: tuple[float, list[Hashable]]
	"""(latitude, list of indexes at this latitude)"""
	southmost: tuple[float, list[Hashable]]
	"""(latitude, list of indexes at this latitude)"""
	nw_most: tuple[float, Hashable]
	"""(distance, index closest to corner)"""
	ne_most: tuple[float, Hashable]
	"""(distance, index closest to corner)"""
	sw_most: tuple[float, Hashable]
	"""(distance, index closest to corner)"""
	se_most: tuple[float, Hashable]
	"""(distance, index closest to corner)"""
	antipoint_closest: Hashable | None

	# Distances in metres that might be a good measure of the extent of one's travels
	diagonal_dist: float
	total_dist_from_centroid: float
	max_dist_from_centroid: float
	antipoint_dist: float | None
	"""Distance from any point in the point set to the antipoint, so smaller numbers indicate more well-travelled (can cover own deadzones better). Optional since the antipoint can take some time to compute"""
	# Other measures of extent
	convex_hull_area: float
	concave_hull_area: float
	bbox_area: float

	# Other stuff
	closest_to_bbox_label: Hashable
	closest_to_bbox_dist: float

	@property
	def centres(self) -> dict[str, shapely.Point]:
		d = {
			'Circular mean point': self.circular_mean,
			'Mean point': self.arithmetic_mean,
			'Median point': self.arithmetic_median,
			'Closest point to bounding box corners': self.closest_to_bbox,
			'Centroid': self.raw_centroid,
			'Centroid (projected)': self.centroid,
			'Centre of extremes': self.centre_of_extremes,
		}
		if self.geometric_median is not None:
			d['Geometric median'] = self.geometric_median
		return d

	@property
	def distance_extents(self) -> dict[str, float]:
		return {
			'Diagonal distance of bounding box': self.diagonal_dist,
			'Total distance from centroid': self.total_dist_from_centroid,
			'Maximum distance from centroid': self.max_dist_from_centroid,
		}

	@property
	def area_extents(self) -> dict[str, float]:
		return {
			'Bounding box area': self.bbox_area,
			'Convex hull area': self.convex_hull_area,
			'Concave hull area': self.concave_hull_area,
		}


def get_point_set_stats(point_set: 'PointSet', *, find_geomedian: bool, find_antipoint: bool):
	geo = point_set.points
	coords = shapely.get_coordinates(geo)
	west, south, east, north = geo.total_bounds
	sw = shapely.Point(west, south)
	se = shapely.Point(east, south)
	nw = shapely.Point(west, north)
	ne = shapely.Point(east, north)
	bbox = shapely.box(west, south, east, north)
	x, y = coords.T
	d = dict(point_set.items())

	westmost = geo[geo.x == west].index.tolist()
	eastmost = geo[geo.x == east].index.tolist()
	northmost = geo[geo.y == north].index.tolist()
	southmost = geo[geo.y == south].index.tolist()

	centre_x = fix_x_coord((west + east) / 2)
	centre_y = fix_y_coord((south + north) / 2)
	centre_of_extremes = shapely.Point(centre_x, centre_y)
	circ_mean_x, circ_mean_y = circular_mean_xy(x, y)
	circ_mean = shapely.Point(circ_mean_x, circ_mean_y)
	mean = shapely.Point(fix_x_coord(x.mean()), fix_y_coord(y.mean()))
	median_coords = numpy.median(coords, axis=0)
	median = shapely.Point(fix_x_coord(median_coords[0]), fix_y_coord(median_coords[1]))

	bbox_dists = cartesian_product_distances(
		geo, geopandas.GeoSeries([sw, se, nw, ne], index=['sw', 'se', 'nw', 'ne'])
	)
	total_bbox_dists = bbox_dists.sum(axis='columns')
	closest_index_to_corners = total_bbox_dists.idxmax()
	closest_to_bbox_dist = total_bbox_dists.loc[closest_index_to_corners]
	closest_to_corners = d[closest_index_to_corners]
	nwmost, nw_dist = point_set.get_closest_index(nw)
	nemost, ne_dist = point_set.get_closest_index(ne)
	swmost, sw_dist = point_set.get_closest_index(sw)
	semost, se_dist = point_set.get_closest_index(se)
	diagonal_dist = geod_distance(sw, ne)

	raw_centroid = shapely.centroid(point_set.multipoint)
	centroid = point_set.centroid
	centroid_distances = point_set.get_all_distances(centroid)
	total_centroid_dist = centroid_distances.sum()
	max_centroid_dist = centroid_distances.max()

	if find_antipoint:
		initial = get_geometry_antipode(circ_mean)
		antipoint, antipoint_dist = find_furthest_point(geo, initial)
		antipoint_closest, _ = point_set.get_closest_index(antipoint)
	else:
		antipoint = antipoint_dist = antipoint_closest = None
	geo_median = find_geometric_median(geo, centroid) if find_geomedian else None

	return PointSetStats(
		circ_mean,
		mean,
		median,
		closest_to_corners,
		raw_centroid,
		centroid,
		centre_of_extremes,
		geo_median,
		antipoint,
		(west, westmost),
		(east, eastmost),
		(north, northmost),
		(south, southmost),
		(nw_dist, nwmost),
		(ne_dist, nemost),
		(sw_dist, swmost),
		(se_dist, semost),
		antipoint_closest,
		diagonal_dist,
		total_centroid_dist,
		max_centroid_dist,
		antipoint_dist,
		get_area(point_set.convex_hull),
		get_area(point_set.concave_hull),
		get_area(bbox),
		closest_index_to_corners,
		closest_to_bbox_dist,
	)


async def print_centre_points(
	point_set: 'PointSet', stats: PointSetStats, session: ClientSession | None
):
	for name, point in stats.centres.items():
		if name == 'Closest point to bounding box corners':
			continue
		await print_point(point_set, point, name, session)

	print(
		f'Closest point to all corners of bounding box: {stats.closest_to_bbox_label}, {format_distance(stats.closest_to_bbox_dist)}'
	)


def _join_labels(labels: list[Hashable]):
	if len(labels) == 1:
		return f': {labels[0]}'
	joined = ', '.join(str(label) for label in labels)
	return f's: {joined}'


def print_extreme_points(stats: PointSetStats):
	west, westmost = stats.westmost
	east, eastmost = stats.eastmost
	north, northmost = stats.northmost
	south, southmost = stats.southmost

	print(f'Westmost point{_join_labels(westmost)} ({format_number(west, 12)}°)')
	print(f'Eastmost point{_join_labels(eastmost)} ({format_number(east, 12)}°)')
	print(f'Northmost point{_join_labels(northmost)} ({format_number(north, 12)}°)')
	print(f'Southmost point{_join_labels(southmost)} ({format_number(south, 12)}°)')

	nw_dist, nwmost = stats.nw_most
	ne_dist, nemost = stats.ne_most
	sw_dist, swmost = stats.sw_most
	se_dist, semost = stats.se_most
	print(f'Northwestmost point: {nwmost}, {format_distance(nw_dist)} away from corner')
	print(f'Northeastmost point: {nemost}, {format_distance(ne_dist)} away from corner')
	print(f'Southwestmost point: {swmost}, {format_distance(sw_dist)} away from corner')
	print(f'Southeastmost point: {semost}, {format_distance(se_dist)} away from corner')


def print_extents(stats: PointSetStats):
	for name, distance in stats.distance_extents.items():
		print(f'{name}: {format_distance(distance)}')
	for name, area in stats.area_extents.items():
		print(f'{name}: {format_area(area)}')


def print_unique_points(point_set: 'PointSet', uniqueness_path: Path | None):
	closest, uniqueness_ = get_uniqueness(point_set.points)
	uniqueness = pandas.DataFrame({'closest': closest, 'uniqueness': uniqueness_})
	uniqueness = uniqueness.sort_values('uniqueness', ascending=False)
	if uniqueness_path:
		output_dataframe(uniqueness, uniqueness_path)
	print(format_dataframe(uniqueness, 'uniqueness'))

	total_uniqueness = get_total_uniqueness(point_set.points)
	avg_uniqueness = total_uniqueness / (point_set.points.size - 1)
	print(
		format_dataframe(
			pandas.DataFrame(
				{'total_uniqueness': total_uniqueness, 'avg_uniqueness': avg_uniqueness}
			),
			('total_uniqueness', 'avg_uniqueness'),
		)
	)


async def print_furthest_point_from_poly(
	points: 'PointSet', poly: 'BaseGeometry', session: ClientSession | None, name: str = 'polygon'
):
	furthest_point, dist = find_furthest_point(points.point_array, polygon=poly)
	desc = await _maybe_describe_point(furthest_point, session)
	closest_index, _ = points.get_closest_index(furthest_point)
	print(
		f'Furthest point within {name}: {desc}, {format_distance(dist)} away, closest to {closest_index}'
	)


async def print_furthest_points(
	point_set: 'PointSet', stats: PointSetStats, session: ClientSession | None
):
	if stats.antipoint:
		assert stats.antipoint_dist is not None
		desc = await _maybe_describe_point(stats.antipoint, session)
		print(
			f'Furthest point: {desc}, {format_distance(stats.antipoint_dist)} away, closest to {stats.antipoint_closest}'
		)

	await print_furthest_point_from_poly(point_set, point_set.envelope, session, 'own bounding box')
	await print_furthest_point_from_poly(
		point_set, point_set.convex_hull, session, 'own convex hull'
	)
	await print_furthest_point_from_poly(
		point_set, point_set.concave_hull, session, 'own concave hull'
	)


def print_column_stats(
	point_set: 'PointSet', category_cols: list[str] | None, sep_char: str | None
):
	if not category_cols:
		category_cols = detect_cat_cols(point_set.gdf)
	if not category_cols:
		print('Could not find any category columns in this point set')
		return
	for col_name in category_cols:
		col = point_set.gdf[col_name]
		if sep_char:
			col = col.str.split(sep_char).explode()
		counts = col.value_counts()
		print(f'{col_name}: {counts.size} unique values')
		percent = counts / counts.sum()
		df = pandas.DataFrame({'count': counts, '%': percent})
		print(format_dataframe(df, number_cols='count', percent_cols='%'))


async def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument(
		'point_set',
		help='Path to file (.csv, .ods, .xls, .xlsx, pickled DataFrame, GeoJSON, etc), or player:<player display name> or username:<player username>, which will load all the submissions for a particular player.',
	)

	load_args = argparser.add_argument_group(
		'Loading arguments', 'Arguments to control how point_set is loaded'
	)
	output_args = argparser.add_argument_group(
		'Output arguments', 'Arguments to control what additional information is saved and where'
	)

	load_args.add_argument(
		'--lat-column',
		'--latitude-column',
		dest='lat_col',
		help='Force a specific column label for latitude, defaults to autodetected',
	)
	load_args.add_argument(
		'--lng-column',
		'--longitude-column',
		dest='lng_col',
		help='Force a specific column label for longitude, defaults to autodetected',
	)
	load_args.add_argument(
		'--unheadered',
		action='store_true',
		help='Explicitly treat csv/Excel as not having a header, otherwise autodetect (and default to yes header if unknown)',
	)
	load_args.add_argument(
		'--crs',
		default='wgs84',
		help='Coordinate reference system to use if point_set is .csv/.ods/etc, defaults to WGS84',
	)
	load_args.add_argument(
		'--name-col',
		help='Force a specific column label for the name of each point, otherwise autodetect',
	)

	output_args.add_argument(
		'--uniqueness-path',
		type=Path,
		help='Optionally output uniqueness of each pic to here (as a table)',
	)
	output_args.add_argument(
		'--antipodes-path',
		type=Path,
		help='Optionally output antipodes of each pic to here (as a table)',
	)
	output_args.add_argument(
		'--convex-hull-path',
		type=Path,
		help='Optionally output convex hull of all pics to here (can be geojson, etc)',
	)
	output_args.add_argument(
		'--concave-hull-path',
		type=Path,
		help='Optionally output concave hull of all pics to here (can be geojson, etc)',
	)

	argparser.add_argument(
		'--projected-crs',
		'--metric-crs',
		'--metres-crs',
		help='Projected coordinate reference system to use for some operations, autodetect if not specified',
	)
	argparser.add_argument(
		'--polygon-path',
		type=Path,
		help='Optional path to a file containing polygons to get stats like furthest point in a region or whatever else',
	)
	argparser.add_argument(
		'--reverse-geocode',
		action=BooleanOptionalAction,
		default=True,
		help='Reverse geocode when printing points, defaults to true.',
	)
	argparser.add_argument(
		'--column-stats',
		action=BooleanOptionalAction,
		default=False,
		help='Include additional stats about counts of columns, defaults to false. If --category-columns is not specified alongside this, they will be autodetected',
	)
	argparser.add_argument(
		'--category-columns',
		'--category-cols',
		'--cat-cols',
		nargs=ZERO_OR_MORE,
		help='Columns to get counts of, implies --column-stats.',
	)
	argparser.add_argument(
		'--split-categories',
		default='/',
		help='With --column-stats, split columns by this character (single slash / by default) to have multiple values in one column. Use empty string as an argument to disable',
	)
	argparser.add_argument(
		'--geomedian',
		'--geometric-median',
		action=BooleanOptionalAction,
		default=True,
		help='Calculate the geometric median of all points, defaults to True.',
	)
	argparser.add_argument(
		'--antipoint',
		'--furthest-away-point',
		action=BooleanOptionalAction,
		default=True,
		help='Calculate the furthest away point anywhere in the world from any points, defaults to True.',
	)

	args = argparser.parse_args()

	point_set = await load_point_set_from_arg(
		args.point_set,
		args.lat_col,
		args.lng_col,
		args.crs,
		args.name_col,
		args.projected_crs,
		force_unheadered=args.unheadered,
	)

	geo = point_set.points
	print(f'{geo.size} points')

	if args.convex_hull_path:
		await geometry_to_file_async(args.convex_hull_path, point_set.convex_hull)
	if args.concave_hull_path:
		await geometry_to_file_async(args.concave_hull_path, point_set.concave_hull)

	antipoints = get_point_antipodes(geo)
	if args.antipodes_path:
		antipodes_gdf = geopandas.GeoDataFrame(
			{'name': geo.index.to_list()}, geometry=antipoints, crs=geo.crs
		)
		print(antipodes_gdf)
		await asyncio.to_thread(
			output_geodataframe, antipodes_gdf, args.antipodes_path, index=False
		)
	antipoints_mp = shapely.MultiPoint(antipoints)
	antihull = shapely.concave_hull(antipoints_mp)
	assert isinstance(antihull, shapely.Polygon), f'antihull is {type(antihull)}, expected Polygon'

	if args.column_stats or args.category_columns:
		print_column_stats(point_set, args.category_columns, args.split_categories)

	print_unique_points(point_set, args.uniqueness_path)
	stats = get_point_set_stats(
		point_set, find_geomedian=args.geomedian, find_antipoint=args.antipoint
	)

	use_reverse_geocode: bool = args.reverse_geocode
	async with ClientSession() if use_reverse_geocode else nullcontext() as sesh:
		print_extreme_points(stats)
		print('-' * 10)
		await print_centre_points(point_set, stats, sesh)
		print('-' * 10)
		print_extents(stats)
		print('-' * 10)
		await print_furthest_points(point_set, stats, sesh)

		polygon_path: Path | None = args.polygon_path
		if polygon_path:
			polygon = await asyncio.to_thread(load_polygons, polygon_path)
			if polygon:
				await print_furthest_point_from_poly(point_set, polygon, sesh, polygon_path.stem)
				await print_furthest_point_from_poly(
					point_set, polygon.envelope, sesh, f'{polygon_path.stem} bounding box'
				)
			else:
				print(f'Could not find any polygons in {polygon_path.stem}')


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	asyncio.run(main())

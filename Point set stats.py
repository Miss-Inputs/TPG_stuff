#!/usr/bin/env python3
"""Generate some stats for all the locations from a file, or a user's known submissions."""

import asyncio
import logging
from argparse import ArgumentParser
from pathlib import Path
from typing import TYPE_CHECKING

import geopandas
import numpy
import pandas
import shapely
from aiohttp import ClientSession
from travelpygame import (
	PointSetInfo,
	find_furthest_point,
	get_uniqueness,
	load_or_fetch_per_player_submissions,
	load_points_async,
	validate_points,
)
from travelpygame.point_set_stats import find_geometric_median, get_total_uniqueness
from travelpygame.util import (
	circular_mean_xy,
	fix_x_coord,
	fix_y_coord,
	format_dataframe,
	format_distance,
	format_point,
	geometry_to_file_async,
	get_centroid,
	get_closest_index,
	get_extreme_corners_of_point_set,
	get_point_antipodes,
	get_polygons,
	get_projected_crs,
	output_geodataframe,
	read_geodataframe_async,
	try_set_index_name_col,
)

from lib.format_utils import describe_point
from lib.settings import Settings

if TYPE_CHECKING:
	from shapely.geometry.base import BaseGeometry

logger = logging.getLogger(__name__)


async def print_furthest_point_from_poly(
	points: PointSetInfo, poly: 'BaseGeometry', session: ClientSession, name: str = 'polygon'
):
	furthest_point, dist = find_furthest_point(points.point_array, polygon=poly)
	desc = await describe_point(furthest_point, session, include_coords=True)
	closest_index, _ = get_closest_index(furthest_point, points.point_array)
	print(
		f'Furthest point within {name}: {desc}, {format_distance(dist)} away, closest to {points.points.index[closest_index]}'
	)


async def print_furthest_point(
	point_set: PointSetInfo, initial: shapely.Point, session: ClientSession
):
	furthest_point, dist = find_furthest_point(point_set.point_array, initial)
	desc = await describe_point(furthest_point, session, include_coords=True)
	closest_index, _ = get_closest_index(furthest_point, point_set.point_array)
	print(
		f'Furthest point: {desc}, {format_distance(dist)} away, closest to {point_set.points.index[closest_index]}'
	)

	await print_furthest_point_from_poly(point_set, point_set.envelope, session, 'own bounding box')
	await print_furthest_point_from_poly(
		point_set, point_set.convex_hull, session, 'own convex hull'
	)
	await print_furthest_point_from_poly(
		point_set, point_set.concave_hull, session, 'own concave hull'
	)


async def print_point(
	point_set: PointSetInfo,
	point: shapely.Point,
	name: str,
	session: ClientSession,
	*,
	get_median: bool = False,
):
	print(f'{name}:', await describe_point(point, session, include_coords=True))
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


async def print_average_points(point_set: PointSetInfo, session: ClientSession):
	coords = point_set.coord_array

	x, y = coords.T
	circ_mean_x, circ_mean_y = circular_mean_xy(x, y)
	circ = shapely.Point(circ_mean_x, circ_mean_y)
	await print_point(point_set, circ, 'Circular mean point', session)

	mean = shapely.Point(fix_x_coord(x.mean()), fix_y_coord(y.mean()))
	await print_point(point_set, mean, 'Mean point', session)

	median_coords = numpy.median(coords, axis=0)
	median = shapely.Point(fix_x_coord(median_coords[0]), fix_y_coord(median_coords[1]))
	await print_point(point_set, median, 'Median point', session)

	centroid = point_set.centroid
	await print_point(point_set, centroid, 'Centroid of all points', session)
	centroid_distances = point_set.get_all_distances(centroid)
	print(f'Total distance from centroid: {format_distance(centroid_distances.sum())}')
	print(f'Mean distance from centroid: {format_distance(centroid_distances.mean())}')

	geo_median = find_geometric_median(point_set.points, centroid)
	await print_point(point_set, geo_median, 'Geometric median', session)


async def print_extreme_points(point_set: PointSetInfo, session: ClientSession):
	geo = point_set.points
	west, south, east, north = geo.total_bounds

	westmost = geo[geo.x == west]
	eastmost = geo[geo.x == east]
	southmost = geo[geo.y == south]
	northmost = geo[geo.y == north]
	print('Westmost point(s):', ', '.join(westmost.index), f'({west})')
	print('Eastmost point(s):', ', '.join(eastmost.index), f'({east})')
	print('Southmost point(s):', ', '.join(southmost.index), f'({south})')
	print('Northmost point(s):', ', '.join(northmost.index), f'({north})')
	print()

	nw, ne, se, sw = get_extreme_corners_of_point_set(geo)
	print(f'Northwestmost point: {nw}')
	print(f'Northeastmost point: {ne}')
	print(f'Southeastmost point: {se}')
	print(f'Southwestmost point: {sw}')
	print()

	centre_x = fix_x_coord((west + east) / 2)
	centre_y = fix_y_coord((south + north) / 2)
	centre = shapely.Point(centre_x, centre_y)
	await print_point(point_set, centre, 'Centre of extremes', session)


def print_unique_points(point_set: PointSetInfo, uniqueness_path: Path | None):
	closest, uniqueness_ = get_uniqueness(point_set.points)
	uniqueness = pandas.DataFrame({'closest': closest, 'uniqueness': uniqueness_})
	uniqueness = uniqueness.sort_values('uniqueness', ascending=False)
	if uniqueness_path:
		uniqueness.to_csv(uniqueness_path)
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


async def load_polygons(path: Path):
	gdf = await read_geodataframe_async(path)
	polygons = get_polygons(gdf)
	return shapely.MultiPolygon(polygons) if polygons else None


async def load_point_set(
	path_or_name: str,
	lat_col: str | None,
	lng_col: str | None,
	crs_arg: str | None,
	name_col: str | None,
	*,
	force_unheadered: bool,
):
	if path_or_name.startswith('username:'):
		settings = Settings()
		all_subs = await load_or_fetch_per_player_submissions(
			settings.subs_per_player_path, settings.main_tpg_data_path
		)
		gdf = all_subs[path_or_name.removeprefix('username:')]
	else:
		gdf = await load_points_async(
			path_or_name,
			lat_col,
			lng_col,
			crs=crs_arg,
			has_header=False if force_unheadered else None,
		)
	assert gdf.crs, 'gdf had no crs, which should never happen'
	if not gdf.crs.is_geographic:
		logger.warning('gdf had non-geographic CRS %s, converting to WGS84')
		gdf = gdf.to_crs('wgs84')

	gdf = gdf.set_index(name_col) if name_col else try_set_index_name_col(gdf)
	if isinstance(gdf.index, pandas.RangeIndex):
		gdf.index = pandas.Index(gdf.geometry.map(format_point))
	_, to_drop = validate_points(gdf, name_for_log=path_or_name)
	return gdf.drop(index=list(to_drop)) if to_drop else gdf


async def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument(
		'point_set',
		help='Path to file (.csv, .ods, .xls, .xlsx, pickled DataFrame, GeoJSON, etc), or username:<player username>, which will load all the submissions for a particular player.',
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

	argparser.add_argument(
		'--projected-crs',
		'--metric-crs',
		'--metres-crs',
		help='Projected coordinate reference system to use for some operations, autodetect if not specified',
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
		'--polygon-path',
		type=Path,
		help='Optional path to a file containing polygons to get stats like furthest point in a region or whatever else',
	)

	args = argparser.parse_args()
	gdf = await load_point_set(
		args.point_set,
		args.lat_col,
		args.lng_col,
		args.crs,
		args.name_col,
		force_unheadered=args.unheadered,
	)

	projected_crs = args.projected_crs
	if not projected_crs:
		projected_crs = get_projected_crs(gdf)
		if projected_crs:
			print(f'Autodetected CRS: {projected_crs.name} {projected_crs.srs}')
		else:
			print('Unable to autodetect CRS, this will result in using a generic one')

	point_set = PointSetInfo(gdf, projected_crs)

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

	print_unique_points(point_set, args.uniqueness_path)

	async with ClientSession() as sesh:
		await print_extreme_points(point_set, sesh)
		await print_average_points(point_set, sesh)
		await print_furthest_point(point_set, get_centroid(antipoints_mp), sesh)
		polygon_path: Path | None = args.polygon_path
		if polygon_path:
			polygon = await load_polygons(polygon_path)
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

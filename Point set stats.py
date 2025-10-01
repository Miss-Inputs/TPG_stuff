#!/usr/bin/env python3
"""Generate some stats for all the locations from a file. May need a better name.

Note that this is extremely under construction."""

import asyncio
import logging
from argparse import ArgumentParser
from collections.abc import Collection
from pathlib import Path
from typing import TYPE_CHECKING

import pandas
import shapely
from aiohttp import ClientSession
from travelpygame import find_furthest_point, get_uniqueness, load_points_async
from travelpygame.util import (
	circular_mean_points,
	format_distance,
	get_centroid,
	get_closest_point_index,
	get_point_antipodes,
	try_set_index_name_col,
)

from lib.format_utils import describe_point

if TYPE_CHECKING:
	from geopandas import GeoSeries

logger = logging.getLogger(__name__)


async def print_furthest_point(
	points: Collection[shapely.Point], initial: shapely.Point, session: ClientSession
):
	furthest_point, dist = find_furthest_point(points, initial)
	desc = await describe_point(furthest_point, session, include_coords=True)
	print(f'Furthest point: {desc}, {format_distance(dist)} away')


async def print_average_points(geo: 'GeoSeries', session: ClientSession):
	points = geo.to_numpy()
	circ = circular_mean_points(points)
	print('Circular mean point:', await describe_point(circ, session, include_coords=True))
	closest_index, dist = get_closest_point_index(circ, points)
	print(f'Closest to: {geo.index[closest_index]}, {format_distance(dist)} away')

	mp = shapely.MultiPoint(points)
	centroid = get_centroid(mp)
	print('Centroid of all points:', await describe_point(centroid, session, include_coords=True))
	closest_index, dist = get_closest_point_index(centroid, points)
	print(f'Closest to: {geo.index[closest_index]}, {format_distance(dist)} away')


async def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument(
		'path',
		type=Path,
		help='Path to file (.csv, .ods, .xls, .xlsx, pickled DataFrame, GeoJSON, etc)',
	)

	argparser.add_argument(
		'--lat-column',
		'--latitude-column',
		dest='lat_col',
		help='Force a specific column label for latitude, defaults to autodetected',
	)
	argparser.add_argument(
		'--lng-column',
		'--longitude-column',
		dest='lng_col',
		help='Force a specific column label for latitude, defaults to autodetected',
	)
	argparser.add_argument(
		'--unheadered',
		action='store_true',
		help='Explicitly treat csv/Excel as not having a header, otherwise autodetect (and default to yes header if unknown)',
	)
	argparser.add_argument(
		'--crs', default='wgs84', help='Coordinate reference system to use, defaults to WGS84'
	)
	
	argparser.add_argument('--uniqueness-path', type=Path, help='Optionally output uniqueness of each pic to here')

	args = argparser.parse_args()
	gdf = await load_points_async(
		args.path,
		args.lat_col,
		args.lng_col,
		crs=args.crs,
		has_header=False if args.unheadered else None,
	)
	assert gdf.crs, 'gdf had no crs, which should never happen'
	if not gdf.crs.is_geographic:
		logger.warning('gdf had non-geographic CRS %s, converting to WGS84')
		gdf = gdf.to_crs('wgs84')

	gdf = try_set_index_name_col(gdf)

	geo = gdf.geometry
	points = [g for g in geo if isinstance(g, shapely.Point)]

	antipoints = get_point_antipodes(geo)
	antipoints_mp = shapely.MultiPoint(antipoints)
	antihull = shapely.concave_hull(antipoints_mp)
	assert isinstance(antihull, shapely.Polygon)

	async with ClientSession() as sesh:
		await print_average_points(geo, sesh)
		await print_furthest_point(points, get_centroid(antipoints_mp), sesh)

	closest, uniqueness_ = get_uniqueness(geo)
	uniqueness = pandas.DataFrame({'closest': closest, 'uniqueness': uniqueness_})
	uniqueness = uniqueness.sort_values('uniqueness', ascending=False)
	if args.uniqueness_path:
		await asyncio.to_thread(uniqueness.to_csv, args.uniqueness_path)
	uniqueness['uniqueness'] = uniqueness['uniqueness'].map(format_distance)
	print(uniqueness)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	asyncio.run(main())

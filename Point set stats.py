#!/usr/bin/env python3
"""Generate some stats for all the locations from a file. May need a better name.

Note that this is extremely under construction."""

import asyncio
import logging
from argparse import ArgumentParser
from pathlib import Path
from typing import Any

import geopandas
import pandas
import shapely
from aiohttp import ClientSession
from travelpygame import find_furthest_point, get_uniqueness, load_points_async
from travelpygame.util import (
	circular_mean_points,
	format_dataframe,
	format_distance,
	get_centroid,
	get_closest_index,
	get_point_antipodes,
	get_projected_crs,
	output_geodataframe,
	try_set_index_name_col,
)

from lib.format_utils import describe_point

logger = logging.getLogger(__name__)


async def print_furthest_point(
	geo: geopandas.GeoSeries, initial: shapely.Point, session: ClientSession
):
	points = geo.to_numpy()
	furthest_point, dist = find_furthest_point(points, initial)
	desc = await describe_point(furthest_point, session, include_coords=True)
	closest_index, _ = get_closest_index(furthest_point, points)
	print(
		f'Furthest point: {desc}, {format_distance(dist)} away, closest to {geo.index[closest_index]}'
	)


async def print_average_points(
	geo: geopandas.GeoSeries, mp: shapely.MultiPoint, projected_crs: Any, session: ClientSession
):
	points = geo.to_numpy()
	circ = circular_mean_points(points)
	print('Circular mean point:', await describe_point(circ, session, include_coords=True))
	closest_index, dist = get_closest_index(circ, points)
	print(f'Closest to: {geo.index[closest_index]}, {format_distance(dist)} away')

	centroid = get_centroid(mp, projected_crs, geo.crs)
	print('Centroid of all points:', await describe_point(centroid, session, include_coords=True))
	closest_index, dist = get_closest_index(centroid, points)
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
	argparser.add_argument(
		'--projected-crs',
		'--metric-crs',
		'--metres-crs',
		help='Projected coordinate reference system to use for some operations, autodetect if not specified',
	)
	argparser.add_argument(
		'--name-col',
		help='Force a specific column label for the name of each point, otherwise autodetect',
	)

	argparser.add_argument(
		'--uniqueness-path', type=Path, help='Optionally output uniqueness of each pic to here'
	)
	argparser.add_argument(
		'--antipodes-path', type=Path, help='Optionally output antipodes of each pic to here'
	)
	argparser.add_argument(
		'--convex-hull-path', type=Path, help='Optionally output convex hull of all pics to here'
	)
	argparser.add_argument(
		'--concave-hull-path', type=Path, help='Optionally output convex hull of all pics to here'
	)

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

	gdf = gdf.set_index(args.name_col) if args.name_col else try_set_index_name_col(gdf)
	geo = gdf.geometry
	projected_crs = args.projected_crs
	if not projected_crs:
		projected_crs = get_projected_crs(gdf)

	mp = shapely.MultiPoint(geo.to_numpy())
	if args.convex_hull_path:
		# We will only compute it if we're outputting it, since there's not really a nice way to display it, and we don't do anything else with it
		convex_hull = shapely.convex_hull(mp)
		geopandas.GeoSeries([convex_hull], crs=gdf.crs).to_file(args.convex_hull_path)
	if args.concave_hull_path:
		concave_hull = shapely.concave_hull(mp)
		geopandas.GeoSeries([concave_hull], crs=gdf.crs).to_file(args.concave_hull_path)

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
	assert isinstance(antihull, shapely.Polygon)

	async with ClientSession() as sesh:
		await print_average_points(geo, mp, projected_crs, sesh)
		await print_furthest_point(geo, get_centroid(antipoints_mp), sesh)

	closest, uniqueness_ = get_uniqueness(geo)
	uniqueness = pandas.DataFrame({'closest': closest, 'uniqueness': uniqueness_})
	uniqueness = uniqueness.sort_values('uniqueness', ascending=False)
	if args.uniqueness_path:
		await asyncio.to_thread(uniqueness.to_csv, args.uniqueness_path)
	print(format_dataframe(uniqueness, 'uniqueness'))


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	asyncio.run(main())

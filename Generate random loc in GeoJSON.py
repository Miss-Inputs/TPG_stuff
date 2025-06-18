#!/usr/bin/env python3

import asyncio
from argparse import ArgumentParser, BooleanOptionalAction
from functools import partial
from pathlib import Path

import shapely.ops
from aiohttp import ClientSession
from pyproj import Transformer
from shapely import MultiPolygon, Polygon

from lib.format_utils import describe_point, format_point
from lib.geo_utils import random_point_in_poly
from lib.io_utils import read_geodataframe_async
from lib.stats import get_longest_distance_from_point


async def main() -> None:
	argparser = ArgumentParser()
	argparser.add_argument('path', type=Path)
	argparser.add_argument('--n', type=int, default=1)
	argparser.add_argument('--print-columns', nargs='*')
	# so you can use --print-columns UCL_NAME_2021 STATE_NAME_2021 for example
	argparser.add_argument('--stats', action=BooleanOptionalAction, default=False)
	args = argparser.parse_args()

	path = args.path
	gdf = await read_geodataframe_async(path)
	utm = gdf.estimate_utm_crs()
	to_wgs84 = Transformer.from_crs(gdf.crs, 'wgs84', always_xy=True)
	to_utm = Transformer.from_crs(gdf.crs, utm, always_xy=True)

	poly = gdf.union_all()
	if not isinstance(poly, (Polygon, MultiPolygon)):
		# TODO: Support points as well
		raise TypeError(f'{path} must contain polygon(s), got {type(poly)}')

	# if args.stats and False:
	# 	utm_poly = shapely.ops.transform(to_utm.transform, poly)
	# 	longest_distance = get_longest_distance(utm_poly)
	# 	print(f'Longest possible distance: {longest_distance:g} metres')
	n = args.n

	async with ClientSession() as sesh:
		for i in range(1, n + 1):
			point = random_point_in_poly(poly)
			point = shapely.ops.transform(to_wgs84.transform, point)
			if args.print_columns:
				rows = gdf[gdf.contains(point)].head(1).squeeze()
				desc = ', '.join(rows[args.print_columns].to_list())
			else:
				desc = await describe_point(point, sesh)
			print(f'{i}: {format_point(point)} {desc}')
			if args.stats:
				utm_poly = shapely.ops.transform(to_utm.transform, poly)
				utm_point = shapely.ops.transform(to_utm.transform, point)
				utm_antipode, distance = get_longest_distance_from_point(utm_poly, utm_point)
				print(
					'Furthest possible point:',
					shapely.ops.transform(
						partial(to_utm.transform, direction='INVERSE'), utm_antipode
					),
					distance,
				)


if __name__ == '__main__':
	asyncio.run(main(), debug=False)

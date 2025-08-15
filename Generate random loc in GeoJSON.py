#!/usr/bin/env python3

import asyncio
from argparse import ArgumentParser, BooleanOptionalAction
from collections import Counter, defaultdict
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any

import shapely.ops
from aiohttp import ClientSession
from numpy.random import default_rng
from pandas import Series
from pyproj import Transformer
from shapely import MultiPolygon, Point, Polygon

from lib.format_utils import describe_point, format_distance, format_point
from lib.geo_utils import random_point_in_poly
from lib.io_utils import read_geodataframe_async
from lib.stats import get_longest_distance_from_point, summarize_counter

if TYPE_CHECKING:
	import geopandas


def _get_point_data(point: Point, gdf: 'geopandas.GeoDataFrame', value_cols: list[str]):
	"""point and gdf are assumed to be in the same CRS"""
	rows = gdf[gdf.contains(point)].head(1).squeeze()
	assert isinstance(rows, Series), f'Uh oh squeeze failed, we ended up with {type(rows)}'
	return rows[value_cols]


async def _random_single_point_in_poly(
	poly: Polygon | MultiPolygon,
	gdf: 'geopandas.GeoDataFrame',
	to_wgs84: Transformer,
	to_utm: Transformer,
	value_cols: list[str],
	seed: int | None,
	*,
	stats: bool,
):
	async with ClientSession() as sesh:
		raw_point = random_point_in_poly(
			poly, seed, use_tqdm=True, desc='Finding point inside poly', unit='attempt'
		)
		point = shapely.ops.transform(to_wgs84.transform, raw_point)
		print(format_point(point))
		if value_cols:
			data = _get_point_data(raw_point, gdf, value_cols)
			for k, v in data.items():
				print(f'{k}: {v}')
		else:
			desc = await describe_point(point, sesh)
			print(desc)
		if stats:
			utm_poly = shapely.ops.transform(to_utm.transform, poly)
			utm_point = shapely.ops.transform(to_utm.transform, raw_point)
			utm_furthest_point, distance = get_longest_distance_from_point(utm_poly, utm_point)
			furthest_point = shapely.ops.transform(
				partial(to_utm.transform, direction='INVERSE'), utm_furthest_point
			)
			furthest_point_desc = await describe_point(furthest_point, sesh)
			print(
				f'Furthest possible point: {format_point(furthest_point)} {furthest_point_desc}: {format_distance(distance)} away'
			)


async def _random_points_in_poly(
	poly: Polygon | MultiPolygon,
	num_points: int,
	gdf: 'geopandas.GeoDataFrame',
	to_wgs84: Transformer,
	value_cols: list[str],
	seed: int | None,
	*,
	stats: bool,
):
	random = default_rng(seed)
	total_data: defaultdict[str, list[Any]] = defaultdict(list)
	async with ClientSession() as sesh:
		for i in range(1, num_points + 1):
			point = random_point_in_poly(
				poly,
				random,
				use_tqdm=True,
				desc='Finding point inside poly',
				unit='attempt',
				leave=False,
			)
			point = shapely.ops.transform(to_wgs84.transform, point)
			if value_cols:
				data = _get_point_data(point, gdf, value_cols)
				desc = ', '.join(data.to_list())
				for k, v in data.items():
					assert isinstance(k, str), type(k)
					total_data[k].append(v)
			else:
				desc = await describe_point(point, sesh)
			print(f'{i}: {format_point(point)} {desc}')
	if stats:
		for col, values in total_data.items():
			counter = Counter(values)
			print(f'{col}:')
			print(summarize_counter(counter))
			print('-' * 10)


async def main() -> None:
	argparser = ArgumentParser()
	argparser.add_argument(
		'path',
		type=Path,
		help='GeoJSON (or GeoPackage, etc) file to draw random point(s) from. Must contain polygons or multipolygons, for now',
	)
	argparser.add_argument(
		'-n',
		'--num-points',
		type=int,
		default=1,
		help='Number of points to generate, defaults to 1',
		dest='n',
	)
	argparser.add_argument(
		'--print-columns',
		'--value-columns',
		'--value-cols',
		nargs='*',
		help='Prints values of the random point(s) where they line up with rows in the input file, so you can use --value-columns state country for example',
		dest='value_cols',
	)
	argparser.add_argument(
		'--stats',
		action=BooleanOptionalAction,
		default=False,
		help='Display some info about the generated point if n == 1, or counts of things in the generated columns if n > 1',
	)
	argparser.add_argument(
		'--seed',
		'--random-seed',
		type=int,
		default=None,
		help='Optional seed for random number generator, default none (use default entropy)',
		dest='seed',
	)
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

	n: int = args.n
	seed: int | None = args.seed
	value_cols: list[str] = args.value_cols
	if n == 1:
		await _random_single_point_in_poly(
			poly, gdf, to_wgs84, to_utm, value_cols, seed, stats=args.stats
		)
	else:
		await _random_points_in_poly(poly, n, gdf, to_wgs84, value_cols, seed, stats=args.stats)


if __name__ == '__main__':
	asyncio.run(main(), debug=False)

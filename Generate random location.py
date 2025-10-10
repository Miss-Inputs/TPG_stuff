#!/usr/bin/env python3

import asyncio
from argparse import ArgumentParser, BooleanOptionalAction
from collections import Counter, defaultdict
from functools import partial
from pathlib import Path
from typing import Any

import geopandas
import shapely.ops
from aiohttp import ClientSession
from numpy.random import default_rng
from pandas import Series
from pyproj import Transformer
from shapely import MultiPolygon, Point, Polygon
from tqdm.auto import tqdm
from travelpygame import output_geodataframe, random_point_in_poly, random_points_in_poly
from travelpygame.util import format_distance, format_point, read_geodataframe_async

from lib.format_utils import describe_point
from lib.stats import get_longest_distance_from_point, summarize_counter


def _get_point_data(point: Point, gdf: 'geopandas.GeoDataFrame', value_cols: list[str]):
	"""point and gdf are assumed to be in the same CRS"""
	rows = gdf[gdf.contains(point)].head(1).squeeze()
	assert isinstance(rows, Series), f'Uh oh squeeze failed, we ended up with {type(rows)}'
	return rows[value_cols]


async def _random_single_point_in_poly(
	poly: Polygon | MultiPolygon | None,
	gdf: 'geopandas.GeoDataFrame',
	to_wgs84: Transformer | None,
	to_utm: Transformer,
	value_cols: list[str],
	seed: int | None,
	*,
	stats: bool,
):
	async with ClientSession() as sesh:
		raw_point = random_point_in_poly(
			gdf, seed, use_tqdm=True, desc='Finding point inside poly', unit='attempt'
		)
		point = shapely.ops.transform(to_wgs84.transform, raw_point) if to_wgs84 else raw_point
		print(format_point(point, None))
		if value_cols:
			data = _get_point_data(raw_point, gdf, value_cols)
			for k, v in data.items():
				print(f'{k}: {v}')
		else:
			desc = await describe_point(point, sesh)
			print(desc)
		if stats and poly:
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
	num_points: int,
	gdf: 'geopandas.GeoDataFrame',
	to_wgs84: Transformer | None,
	value_cols: list[str],
	seed: int | None,
	*,
	print_each_point: bool,
	stats: bool,
	reverse_geocode: bool,
) -> geopandas.GeoDataFrame:
	random = default_rng(seed)
	total_data: defaultdict[str, list[Any]] = defaultdict(list)
	rows = []
	points = random_points_in_poly(
		gdf, num_points, random, use_tqdm=True, desc='Generating points', unit='point'
	)
	if to_wgs84:
		points = [shapely.ops.transform(to_wgs84.transform, point) for point in points]
	async with ClientSession() as sesh:
		for i, point in enumerate(points):
			if value_cols:
				data = _get_point_data(point, gdf, value_cols)
				desc = ', '.join(str(datum) for datum in data)
				if stats:
					for k, v in data.items():
						assert isinstance(k, str), type(k)
						total_data[k].append(v)
				rows.append({'point': point, **data.to_dict()})
			elif reverse_geocode:
				desc = await describe_point(point, sesh)
				rows.append({'point': point, 'name': desc})
			else:
				desc = ''
				rows.append({'point': point})

			if print_each_point:
				tqdm.write(f'{i}: {format_point(point, None)} {desc}')
	if stats:
		for col, values in total_data.items():
			counter = Counter(values)
			tqdm.write(f'{col}:')
			tqdm.write(str(summarize_counter(counter)))
			tqdm.write('-' * 10)
	return geopandas.GeoDataFrame(rows, geometry='point', crs=gdf.crs)


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
		help='Prints values of the random point(s) where they line up with rows in the input file, so you can use --value-columns state country to print the state and the country that each point is in, for example, if the file contains those columns. This will also be saved into --output-path if that is used.',
		dest='value_cols',
	)
	argparser.add_argument(
		'--stats',
		action=BooleanOptionalAction,
		default=False,
		help='Display some info about the generated point if n == 1, or counts of things in the generated columns if n > 1',
	)
	argparser.add_argument(
		'--reverse-geocode',
		action=BooleanOptionalAction,
		default=False,
		help='Reverse geocode the address of each point if --value-cols is not specified, defaults to false',
	)
	argparser.add_argument(
		'--seed',
		'--random-seed',
		type=int,
		default=None,
		help='Optional seed for random number generator, default none (use default entropy)',
		dest='seed',
	)
	argparser.add_argument(
		'--output-path', type=Path, help='Output generated points here, if n is more than 1'
	)
	args = argparser.parse_args()

	path = args.path
	n: int = args.n
	seed: int | None = args.seed
	output_path: Path | None = args.output_path
	value_cols: list[str] = args.value_cols

	gdf = await read_geodataframe_async(path)
	if gdf.crs and gdf.crs.equals('wgs84'):
		to_wgs84 = None
	else:
		to_wgs84 = Transformer.from_crs(gdf.crs, 'wgs84', always_xy=True)
	utm = gdf.estimate_utm_crs()
	to_utm = Transformer.from_crs(gdf.crs, utm, always_xy=True)

	# TODO: Support points as well by selecting a random point (otherwise random_point_in_poly might end up doing that, but slowly)
	if args.stats and n == 1:
		# Now we only need to generate the union if we want stats for a single point
		if gdf.index.size == 1 and isinstance(gdf.geometry.iloc[0], (Polygon, MultiPolygon)):
			poly = gdf.geometry.iloc[0]
			assert isinstance(poly, (Polygon, MultiPolygon)), 'what'
		else:
			poly = gdf.union_all()
			if not isinstance(poly, (Polygon, MultiPolygon)):
				raise TypeError(f'{path} must contain polygon(s), got {type(poly)}')
	else:
		poly = None

	if n == 1:
		await _random_single_point_in_poly(
			poly, gdf, to_wgs84, to_utm, value_cols, seed, stats=args.stats
		)
	else:
		points = await _random_points_in_poly(
			n,
			gdf,
			to_wgs84,
			value_cols,
			seed,
			print_each_point=output_path is None,
			stats=args.stats,
			reverse_geocode=args.reverse_geocode,
		)
		if output_path:
			output_geodataframe(points, output_path, index=False)


if __name__ == '__main__':
	asyncio.run(main(), debug=False)

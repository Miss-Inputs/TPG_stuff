#!/usr/bin/env python3
"""For the H round (Hashing TPG) of Alphabet TPG. Converts points into hashes or solves it backwards"""

import asyncio
from argparse import ArgumentParser
from pathlib import Path
from typing import TYPE_CHECKING

from geopandas import GeoDataFrame, GeoSeries
from tqdm.auto import tqdm
from travelpygame.hashing import grid_search_point, josh_hash, josh_hash_point, random_search_point
from travelpygame.util import DistanceMethod, format_dataframe, output_geodataframe

from lib.arg_utils import parse_box, parse_coords
from lib.io_utils import load_point_set_from_arg

if TYPE_CHECKING:
	from shapely import Point


def check_num(n: float, expected: float, name: str = 'n', max_decimal_places: int = 15):
	if n == expected:
		return
	delta = abs(n - expected)
	for i in range(max_decimal_places):
		tol = 10.0**-i
		if delta >= tol:
			raise AssertionError(
				f'{name} is inaccurate at {i} decimal places: got {n}, expected {expected}'
			)
			# or should this return a bool, I dunno
	# If it reaches here, it's close enough


def check_coords(
	lat: float, expected_lat: float, lng: float, expected_lng: float, max_decimal_places: int = 15
):
	check_num(lat, expected_lat, 'latitude', max_decimal_places)
	check_num(lng, expected_lng, 'longitude', max_decimal_places)


def check_hash(
	lat: float, lng: float, expected_lat: float, expected_lng: float, max_decimal_places: int = 5
):
	"""Ruling is that it only needs to be accurate to 5 decimal places…"""
	out_lat, out_lng = josh_hash(lat, lng)
	check_coords(out_lat, expected_lat, out_lng, expected_lng, max_decimal_places)


def check_stuff():
	check_hash(41.021975, -83.922158, 18.53474509, 80.932374)
	check_hash(1, 1, 79.9469526957796, 146.922054290772)
	check_hash(1, -1, 79.9469526957796, 146.922054290772)
	check_hash(-1, 1, 82.9589496460282, 148.32839012146)
	check_hash(-35.2657996911622, 149.122853651663, -7.66468798671241, 59.2206859588623)


def convert_point_set(path_arg: str, output_path: Path | None):
	point_set = asyncio.run(load_point_set_from_arg(path_arg, force_wgs84=True))
	hashed = {}
	for name, point in tqdm(point_set.items(), total=point_set.count, desc='Hashing', unit='point'):
		if not isinstance(name, str):
			# TODO: Hmm I should probably enforce this in PointSet (or have it be only int/str) to avoid other weird problems elsewhere
			name = repr(name)
		hashed[name] = josh_hash_point(point)

	gs = GeoSeries(hashed, crs='wgs84')
	print(gs)
	if output_path:
		if gs.index.equals(point_set.gdf.index):
			gdf = point_set.gdf.set_geometry(gs)
		else:
			gdf = gs.to_frame()
			assert isinstance(gdf, GeoDataFrame), type(gdf)
		gdf = gdf.reset_index()
		output_geodataframe(gdf, output_path, index=False)


def solve(
	target: 'Point',
	threshold: float | None,
	random_limit: int | None,
	bbox: tuple[float, float, float, float] | None,
	output_path: Path | None,
	*,
	use_random: bool,
):
	# TODO: These should be parameters
	distance_method = DistanceMethod.Haversine
	grid_size = 1e-4
	early_return = False

	if use_random:
		points = random_search_point(
			target, threshold, distance_method, random_limit, 5, bbox, early_return=early_return
		)
	else:
		points = grid_search_point(
			target, threshold, distance_method, grid_size, 5, bbox, early_return=early_return
		)
	if not points:
		print('No points were found with the given search parameters. Bummer')
		return
	rows = [
		{'geometry': point, 'hashed_lat': hashed.y, 'hashed_lng': hashed.x, 'distance': dist}
		for point, hashed, dist in points
	]
	gdf = GeoDataFrame(rows, crs='wgs84')
	print(format_dataframe(gdf, 'distance', 'geometry'))
	if output_path:
		output_geodataframe(gdf, output_path, index=False)


def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	mode_args = argparser.add_argument_group(
		'Mode',
		'Only one of these arguments may be used. If none are used, checks that the formula is correct with some known values (if there is no output, nothing threw an assertion, so it is fine)',
	).add_mutually_exclusive_group()
	solve_args = argparser.add_argument_group(
		'Solve args', 'Additional arguments for --solve mode, all ignored if --solve is not used.'
	)

	mode_args.add_argument(
		'--path',
		'--points',
		nargs='?',
		help='Point set to hash all the points of, if not provided, tests some known values to ensure the formula is correct (if there is no output then no assertions failed so it is fine)',
	)
	mode_args.add_argument(
		'--solve',
		type=parse_coords,
		help='Finds possible input coordinates to find a certain point (enter as comma-separated lat and lng), or points within a certain range.',
	)
	solve_args.add_argument(
		'--threshold',
		type=float,
		help='Distance in metres to check for points. If not provided or <=0, only find points that hash to the exact target within 5 decimal places.',
	)
	solve_args.add_argument(
		'--limit',
		type=int,
		help='Amount of random points to generate before giving up. If not set, keeps generating coordinates forever until something is found.',
	)
	solve_args.add_argument(
		'--search-mode',
		choices=['random', 'grid'],
		default='random',
		help='Method of finding points, can be random (default) to generate random coords or grid to search over a fixed grid of coords',
	)
	solve_args.add_argument(
		'--bbox',
		type=parse_box,
		help='Optionally only look for coordinates inside this bbox, specified as comma-separated minx/miny/maxx/maxy',
	)
	argparser.add_argument(
		'--output-path',
		'--out-path',
		type=Path,
		help='Optional path to write the hashed points with --path/--points, or a list of possible solutions with --solve (csv, geojson, etc)',
	)
	args = argparser.parse_args()

	if args.path:
		convert_point_set(args.path, args.output_path)
	elif args.solve:
		solve(
			args.solve,
			args.threshold,
			args.limit,
			args.bbox,
			args.output_path,
			use_random=args.search_mode == 'random',
		)
	else:
		check_stuff()


if __name__ == '__main__':
	main()

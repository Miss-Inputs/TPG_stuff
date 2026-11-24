#!/usr/bin/env python3
"""For the N round (Number changing) of Alphabet TPG. Adds all possible points with the transformation rules to a point set and outputs the result."""
# Some of this probably wants to be in travelpygame

import asyncio
from argparse import ArgumentParser
from collections.abc import Callable
from pathlib import Path
from typing import Literal

from geopandas import GeoDataFrame, GeoSeries
from shapely import Point
from travelpygame.point_set import PointSet
from travelpygame.util import format_distance, format_point, output_geodataframe

from lib.arg_utils import parse_coords
from lib.io_utils import load_point_set_from_arg


def change_digit(coord: float, index: int, new_digit: int) -> float:
	"""index here counts _away_ from the decimal point: 1 is the digit right before it, -1 is the digit right after it, 2 is the tens, etc
	For simplicity, 0 is just the decimal point itself, so don't change that"""
	# This way is probably slow, but if anyone asks, I'm avoiding floating point inaccuracy
	s = list(f'{coord:011.6f}')
	dec_pos = s.index('.')
	s[dec_pos - index] = str(new_digit)
	return float(''.join(s))


def change_leading(coord: float, new: int) -> float:
	# Yeah nah using modulos and shit here _would_ introduce floating point inaccuracies and nobody likes that
	s = list(f'{coord:011.6f}')
	# We need 4 characters before the decimal (to hold a possible sign, or 0 if we aren't adding one which is fine), and then keep 6 digits of precision
	# well, assigning it like this will just delete the first digit (0) if new is positive, which is also fine
	s[0:3] = str(new)
	return float(''.join(s))


def change_coord(point: Point, axis: Literal['lat', 'lng'], func: Callable[..., float], *args):
	if axis == 'lat':
		return Point(point.x, func(point.y, *args))
	return Point(func(point.x, *args), point.y)


def is_point_valid(point: Point, existing: Point) -> bool:
	return (
		not point.equals_exact(existing, 1e-6)
		and point.x >= -180
		and point.x <= 180
		and point.y >= -90
		and point.y <= 90
	)


def add_transformations(point_set: PointSet) -> PointSet:
	"""
	Transformations:
	Add a single digit (that is, a number from 0-9) after the decimal point to either the latitude or longitude of your original coordinates.
	Remove a single digit after the decimal point from either the latitude or longitude of your original coordinates.
	Change a single digit after the decimal point, or the units, from either the latitude or longitude of your original coordinates.
	Change everything before the units in either the latitude or longitude of your coordinates to any integer between -17 and 18.
	"""
	transformed = {}
	for name, point in point_set.items():
		if not isinstance(name, str):
			name = repr(name)
		# Include original point, since that is still valid
		transformed[name] = point
		# TODO: Include adding/removing/changing the digits after the decimal point, which is not that useful, so I didn't bother
		for axis in ('lat', 'lng'):
			for n in range(10):
				new_point = change_coord(point, axis, change_digit, 1, n)
				if is_point_valid(new_point, point):
					transformed[f'{name} {axis} pos 1 to {n}'] = new_point
			for n in range(-17, 18):
				new_point = change_coord(point, axis, change_leading, n)
				if is_point_valid(new_point, point):
					transformed[f'{name} {axis} leading to {n}'] = new_point

	gs = GeoSeries(transformed, crs='wgs84')
	print(gs)
	gdf = gs.to_frame()
	assert isinstance(gdf, GeoDataFrame), type(gdf)
	return PointSet(gdf, point_set.name)


def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument('path', help='Path to point set or player:name etc')
	argparser.add_argument(
		'--output-path',
		'--out-path',
		type=Path,
		help='Optional path to write the hashed points with --path/--points, or a list of possible solutions with --solve (csv, geojson, etc)',
	)
	argparser.add_argument('--target', type=parse_coords, help='Show best distances to the target')
	args = argparser.parse_args()

	point_set = asyncio.run(load_point_set_from_arg(args.path, force_wgs84=True))
	new_ps = add_transformations(point_set)

	if args.output_path:
		gdf = new_ps.gdf.reset_index(names='name')
		output_geodataframe(gdf, args.output_path, index=False)

	target: Point | None = args.target
	if target:
		dists = new_ps.get_all_distances(target)
		top = 10  # that'll do for now
		for index, dist in dists.head(top).items():
			print(f'{index}: {format_point(new_ps[index])} {format_distance(dist)}')


if __name__ == '__main__':
	main()

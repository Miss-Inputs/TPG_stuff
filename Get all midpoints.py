#!/usr/bin/env python3
"""Find all the combinations of midpoints for you and your teammate in TPG, or you and yourself."""

import asyncio
import itertools
import logging
from argparse import ArgumentParser
from collections.abc import Hashable
from pathlib import Path
from typing import TYPE_CHECKING

import geopandas
from shapely import Point
from tqdm.auto import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from travelpygame.util import geod_distance, get_midpoint, output_geodataframe

from lib.io_utils import load_point_set_from_arg

if TYPE_CHECKING:
	from travelpygame.point_set import PointSet

logger = logging.getLogger(Path(__file__).stem)

ItemType = tuple[Hashable, Point]


def get_row_midpoint(item_1: ItemType, item_2: ItemType):
	name_1, point_1 = item_1
	name_2, point_2 = item_2
	midpoint = get_midpoint(point_1, point_2)
	name = f'{name_1} + {name_2}'
	return {'geometry': midpoint, 'name': name}


def _ensure_only_points(point_set: 'PointSet'):
	a: list[ItemType] = []
	for name, point in point_set.points.items():
		if not isinstance(point, Point):
			logger.warning(
				'Ignoring row %s in %s which was a %s and not a Point',
				name,
				point_set.name,
				type(point),
			)
		else:
			a.append((name, point))
	return a


async def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument('point_set', help='Path to file containing points, or player:/username:')
	argparser.add_argument(
		'point_set_2',
		nargs='?',
		help="Teammate's point set. If not specified, get midpoints from point_set and itself",
	)
	argparser.add_argument(
		'--min-distance',
		'--minimum-distance',
		type=float,
		help='If specified, only consider pairs of points that are this distance (in km) apart from each other.',
	)
	argparser.add_argument(
		'--out-path', type=Path, help='Optionally save midpoints to here (geojson/csv/etc)'
	)
	# TODO: All the lat_col/lng_col arguments, for now just don't be weird, and have a normal lat and lng col, and use a normal name column
	args = argparser.parse_args()

	ps_1 = await load_point_set_from_arg(args.point_set)
	points_1 = _ensure_only_points(ps_1)
	if args.point_set_2:
		ps_2 = await load_point_set_from_arg(args.point_set_2)
		points_2 = _ensure_only_points(ps_2)
		total = len(points_1) * len(points_2)
		combinations = itertools.product(points_1, points_2)
	else:
		combinations = tuple(itertools.combinations(points_1, 2))
		total = len(combinations)

	min_dist: float | None = args.min_distance
	if min_dist is not None:
		min_dist *= 1_000
		# There is probably a way to vectorize this but I thought too hard about it
		combinations = [
			(item_1, item_2)
			for item_1, item_2 in combinations
			if geod_distance(item_1[1], item_2[1]) >= min_dist
		]
		total = len(combinations)

	data = []
	for item1, item2 in tqdm(combinations, total=total):
		data.append(get_row_midpoint(item1, item2))

	gdf = geopandas.GeoDataFrame(data, crs='wgs84')
	print(gdf)
	if args.out_path:
		await asyncio.to_thread(output_geodataframe, gdf, args.out_path, index=False)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	with logging_redirect_tqdm():
		asyncio.run(main())

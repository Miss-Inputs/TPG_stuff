#!/usr/bin/env python3
"""Find the best pic for each point in a given set of points, so you can use it to predict how well you might do in a particular TPG, for example."""

import logging
from argparse import ArgumentParser, BooleanOptionalAction
from collections.abc import Hashable
from pathlib import Path
from typing import TYPE_CHECKING

import numpy
from shapely import Point
from tqdm.auto import tqdm
from travelpygame.util import (
	find_first_matching_column,
	format_distance,
	format_point,
	geodataframe_to_csv,
	load_points,
)

from lib.geo_utils import geod_distance_and_bearing, haversine_distance

if TYPE_CHECKING:
	import geopandas
	import pandas


def get_best_pic(
	dest_row: 'pandas.Series',
	sources: 'geopandas.GeoDataFrame',
	source_name_col: Hashable | None = 'name',
	*,
	use_haversine: bool = True,
):
	n = sources.index.size
	source_lat = sources.geometry.y.to_numpy()
	source_lng = sources.geometry.x.to_numpy()
	dest_lat = numpy.repeat(dest_row.geometry.y, n)
	dest_lng = numpy.repeat(dest_row.geometry.x, n)
	distances = (
		haversine_distance(source_lat, source_lng, dest_lat, dest_lng)
		if use_haversine
		else geod_distance_and_bearing(source_lat, source_lng, dest_lat, dest_lng)[0]
	)
	shortest = numpy.argmin(distances)
	shortest_dist = distances[shortest]
	if source_name_col:
		shortest_name = sources[source_name_col].iloc[shortest]
	else:
		shortest_point = sources.geometry[shortest]
		assert isinstance(shortest_point, Point), (
			f'shortest_point was {type(shortest_point)}, expected Point'
		)
		shortest_name = format_point(shortest_point)
	return shortest_name, shortest_dist


def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument(
		'points', type=Path, help='Path of file with existing points to find the best ones out of'
	)
	argparser.add_argument(
		'target_points', type=Path, help='Path of file with points to find the best distances to'
	)
	argparser.add_argument(
		'out_path', type=Path, nargs='?', help='Output distances/best pics to a file, optionally'
	)
	# TODO: All the lat_col/lng_col arguments, for now just don't be weird, and have a normal lat and lng col
	# TODO: Also name col arguments would be useful here
	argparser.add_argument(
		'--threshold',
		type=float,
		help='Report on how often each pic is better than this distance (in km)',
	)
	argparser.add_argument(
		'--use-haversine',
		action=BooleanOptionalAction,
		help='Use haversine for distances, defaults to true',
		default=True,
	)
	args = argparser.parse_args()

	sources = load_points(args.points)
	dests = load_points(args.target_points)
	source_name_col = find_first_matching_column(sources, ('name', 'desc', 'description'))

	best_pics = {}
	distances = {}
	with tqdm(dests.iterrows(), 'Finding best pics', dests.index.size, unit='target') as t:
		for index, dest_row in t:
			t.set_postfix(target='index')
			best_pics[index], distances[index] = get_best_pic(
				dest_row, sources, source_name_col, use_haversine=args.use_haversine
			)

	dests['best'] = best_pics
	dests['distance'] = distances
	dests = dests.sort_values('distance')

	print(dests)
	counts = dests['best'].value_counts()
	print('Number of times each pic was the best:')
	print(counts.to_string(header=False, name=False))
	print('Average distance:', format_distance(dests['distance'].mean()))

	if args.out_path:
		if args.out_path.suffix[1:].lower() == 'csv':
			geodataframe_to_csv(dests, args.out_path, index=False)
		else:
			dests.to_file(args.out_path)
	if args.threshold:
		threshold: float = args.threshold * 1_000
		counts = dests[dests['distance'] < threshold]['best'].value_counts()
		print(f'Number of times each pic was below {format_distance(threshold)}:', counts)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	main()

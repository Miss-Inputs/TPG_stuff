#!/usr/bin/env python3
"""Find the best pic for each point in a given set of points, so you can use it to predict how well you might do in a particular TPG, for example."""

import logging
from argparse import ArgumentParser, BooleanOptionalAction
from collections.abc import Hashable
from pathlib import Path
from typing import TYPE_CHECKING

import pandas
from shapely import Point
from tqdm.auto import tqdm
from travelpygame.util import (
	find_first_matching_column,
	first_unique_column_label,
	format_dataframe,
	format_distance,
	format_point,
	get_distances,
	load_points,
	output_dataframe,
)
from travelpygame.util.pandas_utils import maybe_name_cols

if TYPE_CHECKING:
	import geopandas


def get_best_pic(
	dest: Point,
	dest_name: str,
	sources: 'geopandas.GeoDataFrame',
	source_name_col: Hashable | None = 'name',
	*,
	use_haversine: bool = True,
):
	distances = get_distances(dest, sources.geometry, use_haversine=use_haversine)
	shortest_dist = distances.min()
	is_closest = distances == shortest_dist
	closest = sources[is_closest]
	if closest.index.size > 1:
		tqdm.write(f'Multiple sources were equally distant to {dest_name}: {closest}')

	if source_name_col:
		closest_name = closest[source_name_col].iloc[0]
	else:
		closest_point = closest.geometry.iloc[0]
		assert isinstance(closest_point, Point), (
			f'shortest_point was {type(closest_point)}, expected Point'
		)
		closest_name = format_point(closest_point)
	return closest_name, shortest_dist


def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument(
		'points', type=Path, help='Path of file with existing points to find the best ones out of'
	)
	argparser.add_argument(
		'targets',
		type=Path,
		help='Path of file (assumed to have points) to find the best distances to',
	)
	argparser.add_argument(
		'out_path', type=Path, nargs='?', help='Output distances/best pics to a file, optionally'
	)
	# TODO: All the lat_col/lng_col arguments, for now just don't be weird, and have a normal lat and lng col
	argparser.add_argument(
		'--source-name-col',
		help='Column label in points with the name of each point, or autodetect (it will be okay to not have any such column)',
	)
	argparser.add_argument(
		'--dest-name-col',
		help='Column label in targets with the name of each point, or autodetect (it will be okay to not have any such column)',
	)
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
	dests = load_points(args.targets)
	if not dests.active_geometry_name:
		raise ValueError('no geometry in dests?')
	source_name_col = (
		args.source_name_col
		or find_first_matching_column(sources, maybe_name_cols)
		or first_unique_column_label(sources)
	)
	dest_name_col = (
		args.dest_name_col
		or find_first_matching_column(dests, maybe_name_cols)
		or first_unique_column_label(dests)
	)

	best_pics = {}
	distances = {}
	names = {}
	with tqdm(dests.iterrows(), 'Finding best pics', dests.index.size, unit='target') as t:
		for index, dest_row in t:
			dest = dest_row[dests.active_geometry_name]
			name = dest_row[dest_name_col] if dest_name_col else f'{index}: {format_point(dest)}'  # pyright: ignore[reportArgumentType, reportCallIssue]
			t.set_postfix(target=name)
			names[index] = name
			best_pics[index], distances[index] = get_best_pic(
				dest, name, sources, source_name_col, use_haversine=args.use_haversine
			)

	df = pandas.DataFrame({'dest': names, 'best_pic': best_pics, 'distance': distances})
	if df['dest'].is_unique:
		df = df.set_index('dest')
	df = df.sort_values('distance')

	print(format_dataframe(df, 'distance'))
	counts = df['best_pic'].value_counts()
	print('Number of times each pic was the best:')
	print(counts.to_string(header=False, name=False))
	print('Average distance:', format_distance(df['distance'].mean()))

	if args.out_path:
		output_dataframe(df, args.out_path)
	if args.threshold:
		threshold: float = args.threshold * 1_000
		counts = df[df['distance'] < threshold]['best_pic'].value_counts()
		print(f'Number of times each pic was below {format_distance(threshold)}:', counts)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	main()

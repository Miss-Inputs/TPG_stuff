#!/usr/bin/env python3
"""Find the best pic for each point in a given set of points, so you can use it to predict how well you might do in a particular TPG, for example."""

import asyncio
import logging
from argparse import ArgumentParser, BooleanOptionalAction
from pathlib import Path

import pandas
from pyproj import CRS
from shapely import Point
from tqdm.auto import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from travelpygame.util import (
	find_first_matching_column,
	first_unique_column_label,
	format_dataframe,
	format_distance,
	format_point,
	load_points,
	output_dataframe,
)
from travelpygame.util.pandas_utils import maybe_name_cols

from lib.io_utils import load_point_set_from_path

logger = logging.getLogger()


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

	tqdm_args = argparser.add_argument_group('tqdm args')
	tqdm_args.add_argument(
		'--postfix',
		action=BooleanOptionalAction,
		default=True,
		help='Use tqdm set_postfix, defaults to true',
	)
	tqdm_args.add_argument(
		'--tqdm-miniters',
		'--miniters',
		help='Set miniters argument for tqdm, in case it is causing too much overhead',
	)

	args = argparser.parse_args()

	point_set = asyncio.run(
		load_point_set_from_path(args.points, point_name_col=args.source_name_col, force_wgs84=True)
	)
	dests = load_points(args.targets)
	dest_name_col = (
		args.dest_name_col
		or find_first_matching_column(dests, maybe_name_cols)
		or first_unique_column_label(dests)
	)

	wgs84 = CRS.from_user_input('WGS84')

	if dests.crs and not dests.crs.equals(wgs84):
		logger.info('Reprojecting targets from %s to WGS84', dests.crs)
		dests = dests.to_crs(wgs84)

	targets = dests.geometry
	target_names = (
		dests[dest_name_col].to_dict()
		if dest_name_col
		else {index: format_point(p) for index, p in targets.items()}
	)

	best_pics = {}
	distances = {}
	with tqdm(
		targets.items(),
		'Finding best pics',
		targets.size,
		unit='target',
		miniters=args.tqdm_miniters,
	) as t:
		for index, target in t:
			if not isinstance(target, Point):
				raise TypeError(f'Targets had {type(target)} at index {index}, expected Point')
			if args.postfix:
				name = target_names[index]
				t.set_postfix(target=name, refresh=args.tqdm_miniters is None)
			best_pics[index], distances[index] = point_set.get_closest_index(
				target, use_haversine=args.use_haversine
			)

	df = pandas.DataFrame({'dest': target_names, 'best_pic': best_pics, 'distance': distances})
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
		print(f'Number of times each pic was below {format_distance(threshold)}:')
		print(counts)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	with logging_redirect_tqdm():
		main()

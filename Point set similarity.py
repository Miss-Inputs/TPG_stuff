#!/usr/bin/env python3
"""Get distances (dissimilarity) between point set(s)."""

import asyncio
import itertools
import logging
from argparse import ArgumentParser, Namespace
from argparse import _ArgumentGroup as ArgumentGroup
from collections import defaultdict
from collections.abc import Collection
from operator import itemgetter
from pathlib import Path
from statistics import mean

import pandas
import pyproj
from tqdm.auto import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from travelpygame import PointSet, load_or_fetch_per_player_submissions
from travelpygame.point_set_stats import (
	PointSetDistanceMethod,
	PointSetDistanceMethodType,
	get_distance_method_combinations,
	get_point_set_distance,
)
from travelpygame.util import (
	format_dataframe,
	format_distance,
	format_number,
	output_dataframe,
)

from lib.io_utils import load_point_set_from_arg
from lib.settings import Settings

logger = logging.getLogger(Path(__file__).stem)

wgs84 = pyproj.CRS('WGS84')


def _add_args(arg_group: ArgumentGroup, side: str):
	arg_group.add_argument(
		f'--lat-column-{side}',
		f'--lat-col-{side}',
		f'--latitude-column-{side}',
		dest=f'lat_col_{side}',
		help=f'Force a specific column label for latitude in the {side} point set, defaults to autodetected',
	)
	arg_group.add_argument(
		f'--lng-column-{side}',
		f'--lng-col-{side}',
		f'--longitude-column-{side}',
		dest=f'lng_col_{side}',
		help=f'Force a specific column label for longitude in the {side} point set, defaults to autodetected',
	)
	arg_group.add_argument(
		f'--unheadered-{side}',
		action='store_true',
		help=f'Explicitly treat the {side} file if csv/Excel as not having a header, otherwise autodetect (and default to yes header if unknown)',
	)
	arg_group.add_argument(
		f'--crs-{side}',
		default='wgs84',
		help=f"Coordinate reference system that the {side} file's points are stored in, defaults to WGS84. Note that this will be converted to WGS84 if it is not that anyway.",
	)
	arg_group.add_argument(
		f'--name-col-{side}',
		help=f'Force a specific column label for the name of each point in the {side} file, otherwise autodetect, and if autodetection cannot find anything, uses the formatted coordinates as the names.',
	)


def compare_two(left: PointSet, right: PointSet, method: PointSetDistanceMethodType | None):
	if not method:
		scores = {}
		for i, meth in enumerate(PointSetDistanceMethod):
			distance = get_point_set_distance(left, right, meth, use_tqdm=False)
			if i == 0:
				print(
					f'Closest pair of points: {distance.closest_a} and {distance.closest_b}, {format_distance(distance.closest_distance)}'
				)
			scores[meth.name] = distance.distance
		for meth, score in scores.items():
			print(f'Dissimilarity from {meth}: {format_number(score)}')

	else:
		distance = get_point_set_distance(left, right, method)
		print(f'Dissimilarity: {format_number(distance.distance)}')
		print(
			f'Closest pair of points: {distance.closest_a} and {distance.closest_b}, {format_distance(distance.closest_distance)}'
		)


def compare_one_to_many(
	left: PointSet,
	point_sets: list[PointSet],
	method: PointSetDistanceMethodType | None,
	output_path: Path | None,
):
	rows = {}
	with tqdm(point_sets, 'Comparing point sets', unit='point set') as t:
		for point_set in t:
			t.set_postfix(name=point_set.name)
			row = {}
			if method:
				diff = get_point_set_distance(left, point_set, method, use_tqdm=False)
				row['closest_distance'] = diff.closest_distance
				row['closest_left'] = diff.closest_a
				row['closest_right'] = diff.closest_b
				row['dissimilarity'] = diff.distance
			else:
				for meth in PointSetDistanceMethod:
					diff = get_point_set_distance(left, point_set, meth, use_tqdm=False)
					row['closest_distance'] = diff.closest_distance
					row['closest_left'] = diff.closest_a
					row['closest_right'] = diff.closest_b
					row[meth.name] = diff.distance
			rows[point_set.name] = row
	df = pandas.DataFrame.from_dict(rows, 'index')
	diff_cols = df.columns[~df.columns.str.startswith('closest_')]
	print(
		format_dataframe(
			df.sort_values('closest_distance' if method is None else 'dissimilarity'),
			'closest_distance',
			number_cols=diff_cols,
		)
	)
	if output_path:
		output_dataframe(df, output_path)

	if method is None:
		closest_by_method = df[diff_cols].idxmin(axis='index')
		min_diff_by_method = df[diff_cols].min(axis='index')
		furthest_by_method = df[diff_cols].idxmax(axis='index')
		max_diff_by_method = df[diff_cols].max(axis='index')
		print(
			format_dataframe(
				pandas.DataFrame(
					{
						'most_similar': closest_by_method,
						'min_diff': min_diff_by_method,
						'least_similar': furthest_by_method,
						'max_diff': max_diff_by_method,
					}
				),
				number_cols=('min_diff', 'max_diff'),
			)
		)


def to_graph(
	df: pandas.DataFrame,
	source_col: str | None,
	dest_col: str,
	weight_col: str | None,
	output_path: Path,
):
	# TODO: This belongs in travelpygame probably
	with output_path.open('wt', encoding='utf8') as f:
		f.write('digraph "" {\n')
		for index, row in df.iterrows():
			source = str(index if source_col is None else row[source_col]).replace('"', '\\"')
			dest = str(row[dest_col]).replace('"', '\\"')
			line = f'"{source}" -> "{dest}"'
			if weight_col:
				line += f' [weight={row[weight_col]}]'
			f.write(f'{line};\n')
		f.write('}')


def compare_all(
	point_sets: Collection[PointSet],
	method: PointSetDistanceMethodType | None,
	raw_output_path: Path | None,
	output_path: Path | None,
	graph_output_path: Path | None,
):
	method = method or PointSetDistanceMethod.MeanMin

	scores: defaultdict[str, dict[str, float]] = defaultdict(dict)
	closests: defaultdict[str, dict[str, tuple[float, str, str]]] = defaultdict(dict)
	for left, right in tqdm(
		tuple(itertools.combinations(point_sets, 2)), 'Comparing point sets', unit='point set'
	):
		dist = get_point_set_distance(left, right, method, use_tqdm=False)
		scores[left.name][right.name] = scores[right.name][left.name] = dist.distance
		closests[left.name][right.name] = dist.closest_distance, dist.closest_a, dist.closest_b
		closests[right.name][left.name] = (
			dist.closest_distance,
			dist.closest_b,
			dist.closest_a,
		)  # That's symmetrical, right? Yeah nah should be

	if raw_output_path:
		output_dataframe(pandas.DataFrame(scores), raw_output_path)

	rows = {}
	for name, other_scores in scores.items():
		sorted_scores = sorted(other_scores.items(), key=itemgetter(1))
		most_similar, most_similar_amount = sorted_scores[0]
		least_similar, least_similar_amount = sorted_scores[-1]
		closest_to_similar_dist, closest_a, closest_b = closests[name][most_similar]
		row = {
			'most similar': most_similar,
			'most similar amount': most_similar_amount,
			'closest distance to most similar': closest_to_similar_dist,
			'closest pic to most similar': closest_a,
			'closest pic by most similar': closest_b,
			'least similar': least_similar,
			'least similar amount': least_similar_amount,
			'mean similarity': mean(other_scores.values()),
		}
		rows[name] = row

	df = pandas.DataFrame.from_dict(rows, 'index')
	df = df.sort_values('mean similarity')
	if graph_output_path:
		to_graph(df, None, 'most similar', 'most similar amount', graph_output_path)
	print(df)
	if output_path:
		output_dataframe(df, output_path)


async def load_data(args: Namespace):
	subs_path = args.subs_path
	if not subs_path:
		settings = Settings()
		subs_path = settings.subs_per_player_path

	all_subs = await load_or_fetch_per_player_submissions(subs_path)
	left_player = (
		await load_point_set_from_arg(
			args.left_player,
			args.lat_col_left,
			args.lng_col_left,
			args.crs_left,
			args.name_col_left,
			all_subs=all_subs,
			force_unheadered=args.unheadered_left,
		)
		if args.left_player
		else None
	)
	right_player_args: list[str] = args.right_player
	if right_player_args:
		right_players = [
			await load_point_set_from_arg(
				right_player,
				args.lat_col_right,
				args.lng_col_right,
				args.crs_right,
				args.name_col_right,
				all_subs=all_subs,
				force_unheadered=args.unheadered_right,
			)
			for right_player in right_player_args
		]
	else:
		right_players = []

	all_point_sets = [PointSet(gdf, name) for name, gdf in all_subs.items()]
	skipped: set[str] = set(args.exclude_player or ())
	threshold: int | None = args.threshold
	all_point_sets = [
		ps
		for ps in all_point_sets
		if not ((skipped and ps.name in skipped) or (threshold and ps.count < threshold))
	]
	return all_point_sets, left_player, right_players


def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	left_group = argparser.add_argument_group(
		'First point set args', 'Controls loading of the first point set (the "left" side).'
	)
	right_group = argparser.add_argument_group(
		'Second point set args',
		'Controls loading of the second point set (the "right" side). All except right_path will only have an effect if comparing one at a time.',
	)
	left_group.add_argument(
		'left_player',
		nargs='?',
		help='Path to left point set (.csv, .ods, .xls, .xlsx, pickled DataFrame, GeoJSON, etc) or player:<name> or username:<username>',
	)
	right_group.add_argument(
		'right_player',
		type=Path,
		nargs='*',
		help='Path to right point set (.csv, .ods, .xls, .xlsx, pickled DataFrame, GeoJSON, etc) or player:<name> or username:<username>. If this is specified multiple times, compares left_path to all sets. If not specified, compares left_path to every other TPG player.',
	)
	left_group.add_argument(
		'--player-name',
		help='The name (Discord display name) of the left data, so you can exclude yourself from being similar to yourself',
	)
	argparser.add_argument(
		'--subs-path',
		'--submissions-path',
		type=Path,
		help='Path to file to load submissions per player from (can be a TPG data file), or the value of the SUBS_PER_USER_PATH by default. If not set, loads from API',
	)
	argparser.add_argument('--exclude-player', nargs='*', help='Exclude player(s) by username')
	argparser.add_argument(
		'--threshold',
		type=int,
		help='Only include players with at least this amount of unique submissions',
	)
	argparser.add_argument(
		'--output-path', '--out-path', type=Path, help='Path to write results to CSV'
	)
	argparser.add_argument(
		'--graph-output-path',
		'--graph-path',
		type=Path,
		help='Path to write graph of most similar players to dot',
	)
	argparser.add_argument(
		'--raw-output-path',
		type=Path,
		help='Path to write all scores of all combinations of players to CSV',
	)

	methods = get_distance_method_combinations(one_name_per_method=True)
	argparser.add_argument(
		'--method',
		choices=methods.keys(),
		help='Method of calculating the dissimilarity. Defaults to Hausdorff distance (max of closest distances).',
	)

	_add_args(left_group, 'left')
	_add_args(right_group, 'right')

	args = argparser.parse_args()
	method = methods[args.method] if args.method else None

	all_players, left, right = asyncio.run(load_data(args))
	if left:
		if len(right) == 1:
			compare_two(left, right[0], method)
		else:
			if not right:
				right = [ps for ps in all_players if ps.name not in {left.name, args.player_name}]
			compare_one_to_many(left, right, method, args.output_path)
	else:
		compare_all(
			all_players, method, args.raw_output_path, args.output_path, args.graph_output_path
		)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	with logging_redirect_tqdm():
		main()

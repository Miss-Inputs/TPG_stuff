#!/usr/bin/env python3
"""Given an existing set of points (photos that one has for submitting to TPG, etc), and a set of new points to be added (e.g. new destinations to consider travelling to), finds out what is the most optimal."""

import logging
from argparse import ZERO_OR_MORE, ArgumentParser, BooleanOptionalAction
from operator import attrgetter
from pathlib import Path

import geopandas
import pandas
from pandas import Index, RangeIndex
from shapely import Point
from tqdm.auto import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from travelpygame import load_rounds
from travelpygame.best_pics import get_worst_point
from travelpygame.new_pic_eval import (
	find_if_new_pics_better,
	find_improvements_in_round,
	find_new_pics_better_individually,
	load_points_or_rounds,
	new_distance_rank,
)
from travelpygame.submission_comparison import (
	SubmissionDifference,
	compare_player_in_round,
	find_new_next_highest_distance,
)
from travelpygame.util import (
	find_first_geom_index,
	format_dataframe,
	format_distance,
	format_ordinal,
	format_point,
	get_closest_index,
	load_points,
	output_geodataframe,
	try_set_index_name_col,
)


def get_distances(points: geopandas.GeoDataFrame, new_points: geopandas.GeoDataFrame):
	points_geom = points.geometry.to_numpy()

	rows = []
	with tqdm(
		new_points.geometry.items(), 'Calculating distances', new_points.index.size, unit='point'
	) as t:
		for index, new_point in t:
			t.set_postfix(new_point=index)
			if not isinstance(new_point, Point):
				raise TypeError(
					f'new points contained {type(new_point)} at {index} instead of Point'
				)
			closest_index, distance = get_closest_index(new_point, points_geom)
			closest = points.index[closest_index]
			rows.append(
				{
					'new_point': index,
					'closest': closest,
					'distance': distance,
					'geometry': new_points.geometry.loc[index],  # pyright: ignore[reportArgumentType, reportCallIssue]]
				}
			)
	return geopandas.GeoDataFrame(rows, geometry='geometry', crs='wgs84').sort_values(
		'distance', ascending=False
	)


def get_where_pics_better(
	points: geopandas.GeoSeries,
	new_points: geopandas.GeoSeries,
	targets: geopandas.GeoDataFrame,
	*,
	use_haversine: bool = True,
):
	results = find_if_new_pics_better(points, new_points, targets, use_haversine=use_haversine)
	better = results[results['is_new_better']].copy().drop(columns='is_new_better')
	better['diff'] = better['current_distance'] - better['new_distance']
	return better.sort_values('diff', ascending=False)


def eval_with_targets(
	points: geopandas.GeoDataFrame,
	new_points: geopandas.GeoDataFrame,
	target_paths: list[Path],
	output_path: Path | None,
	*,
	use_haversine: bool = True,
	find_if_any_pics_better: bool = True,
):
	"""The function name kinda sucks but if it starts with test_ then Ruff thinks it's a test function and complains about things accordingly"""
	targets = load_points_or_rounds(target_paths)
	targets = try_set_index_name_col(targets)
	is_default_index = isinstance(targets.index, RangeIndex)
	dupe_geometry = targets.duplicated('geometry', keep=False)
	if dupe_geometry.any():
		print('Targets had duplicate geometries, only keeping first of each')
		print(targets[dupe_geometry])
		targets = targets.drop_duplicates('geometry')
	if is_default_index:
		# Try and get something more descriptive than just the default increasing index
		# We do the isinstance check before drop_duplicates as that may change it
		targets.index = Index(
			[format_point(geo) if isinstance(geo, Point) else str(geo) for geo in targets.geometry]
		)

	if find_if_any_pics_better:
		better = get_where_pics_better(
			points.geometry, new_points.geometry, targets, use_haversine=use_haversine
		)
		print('Number of times each pic was better (with all new pics at once):')
		print(
			better['new_best']
			.value_counts(sort=False)
			.reindex(new_points.index, fill_value=0)
			.sort_values(ascending=False)
		)
		if output_path:
			better.to_csv(output_path)

	worst_target, worst_dist, pic_for_worst = get_worst_point(
		points, targets, use_haversine=use_haversine
	)
	print(f'Worst case target: {worst_target}, {format_distance(worst_dist)} from {pic_for_worst}')
	combined = pandas.concat([points, new_points])
	assert isinstance(combined, geopandas.GeoDataFrame)
	worst_target, worst_dist, pic_for_worst = get_worst_point(
		combined, targets, use_haversine=use_haversine
	)
	print(
		f'Worst case target after adding new pics: {worst_target}, {format_distance(worst_dist)} from {pic_for_worst}'
	)

	diffs = find_new_pics_better_individually(
		points, new_points, targets, use_haversine=use_haversine
	)
	not_used = ', '.join(new_points.index.difference(diffs.index))
	if not_used:
		print(f'Never better: {not_used}')
	diffs = diffs.sort_values('mean', ascending=False)
	print(format_dataframe(diffs, ('total', 'best', 'mean')))


def _get_point_name(current_points: geopandas.GeoSeries, current_diff: SubmissionDifference):
	index = find_first_geom_index(current_points, current_diff.player_pic)
	if isinstance(index, str):
		return index
	return current_diff.player_pic_description or format_point(current_diff.player_pic)


def eval_with_rounds(
	current_points: geopandas.GeoSeries,
	new_points: geopandas.GeoSeries,
	rounds_path: Path,
	name: str,
	output_path: Path | None,
	*,
	use_haversine: bool = False,
):
	rounds = load_rounds(rounds_path)
	rounds.sort(key=attrgetter('number'))

	rows = []
	for r in rounds:
		current_diff = compare_player_in_round(r, name, use_haversine=use_haversine)
		if current_diff is None:
			# We already won the round or didn't submit in in the first place
			continue
		# First see what just time travel would do, with current pics instead of the actual submission at the time
		# The variable names kind of suck, sorry
		distance = None
		current_best = _get_point_name(current_points, current_diff)

		current_best_index, current_distance = get_closest_index(
			r.target, current_points.to_numpy(), use_haversine=use_haversine
		)
		if current_distance < current_diff.player_distance:
			old_best = current_best
			current_best = current_points.index[current_best_index]
			print(
				f'Round {r.name} would already be improved by {current_best} over {old_best}: {format_distance(current_distance)} < {format_distance(current_diff.player_distance)}'
			)
			distance = current_distance
			if current_distance < current_diff.rival_distance:
				new_rank = new_distance_rank(current_distance, r)
				rival_desc = current_diff.rival_pic_description or format_point(
					current_diff.rival_pic
				)
				print(
					f'This would also improve our placing, beating {current_diff.rival} at {rival_desc} ({format_distance(current_diff.rival_distance)}), going from {format_ordinal(current_diff.player_placing)} to {format_ordinal(new_rank)}'
				)
				# Now we need a new rival
				# Unless we just won the round
				if new_rank == 1:
					continue
				new_current_point = current_points[current_best]
				current_diff = find_new_next_highest_distance(
					r,
					name,
					new_current_point,
					current_distance,
					new_rank,
					current_best,
					use_haversine=use_haversine,
				)
				assert current_diff, (
					'current_diff is now None, which should never happen as we already checked if new_rank was 1'
				)

		old_rank = current_diff.player_placing
		old_desc = current_best or format_point(current_diff.player_pic)
		rival_desc = current_diff.rival_pic_description or format_point(current_diff.rival_pic)
		for improvement in find_improvements_in_round(
			r, name, new_points, distance, use_haversine=use_haversine
		):
			new_rank = new_distance_rank(improvement.new_distance, r)
			if new_rank == old_rank:
				# Can happen if the new pic won't help any more than something from current_points would
				continue
			row = {
				'round': r.name,
				'old_pic': old_desc,
				'old_rank': f'{old_rank}/{current_diff.round_num_players}',
				'rival': current_diff.rival,
				'rival_pic': rival_desc,
				'rival_dist': current_diff.rival_distance,
				'new_pic': improvement.new_location_name,
				'new_dist': improvement.new_distance,
				'new_rank': f'{new_rank}/{current_diff.round_num_players}',
				'rank_diff': old_rank - new_rank,
				'amount': current_diff.player_distance - improvement.new_distance,
			}
			rows.append(row)

	if not rows:
		print(
			f'You ({name}) would not improve your placements in any rounds with these new pics, sadge (or you were not found as submitting for any of these rounds, make sure --name is correct)'
		)
		return

	df = pandas.DataFrame(rows)
	df = df.dropna(how='all', axis='columns')
	if output_path:
		df.to_csv(output_path, index=False)
	print(
		format_dataframe(
			df.drop(columns='rival_pic', errors='ignore'), ('new_dist', 'rival_dist', 'amount')
		)
	)

	groupby = df.groupby('new_pic', sort=False)
	grouped = pandas.DataFrame(
		{
			'num_improved': groupby.size(),
			'total_rank_diff': groupby['rank_diff'].sum(),
			'mean_rank_diff': groupby['rank_diff'].mean(),
			'max_rank_diff': groupby['rank_diff'].max(),
			'total_diff': groupby['amount'].sum(),
			'mean_diff': groupby['amount'].mean(),
			'max_diff': groupby['amount'].max(),
		}
	)
	not_used = ', '.join(frozenset(new_points.index).difference(groupby.groups))
	print(f'Not used: {not_used}')
	grouped = grouped.sort_values('total_diff', ascending=False)
	print(format_dataframe(grouped, ('total_diff', 'mean_diff', 'max_diff')))


def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument(
		'existing_points', type=Path, help='Your existing set of points, as .csv/.ods/.geojson/etc'
	)
	argparser.add_argument(
		'new_points', type=Path, help='New points to evaluate, as .csv/.ods/.geojson/etc'
	)
	argparser.add_argument(
		'--threshold',
		type=float,
		help='Ignore new points that are <= threshold metres away from existing points',
	)
	argparser.add_argument(
		'--targets',
		nargs=ZERO_OR_MORE,
		type=Path,
		help='Test against locations or rounds loaded from this path (geojson/csv/ods/etc or submission tracker kml/kmz)',
	)
	argparser.add_argument(
		'--rounds-path',
		'--data-path',
		type=Path,
		help='Test against TPG data loaded from this path to see what rounds could have been improved',
	)
	argparser.add_argument(
		'--rounds-output-path',
		type=Path,
		help='Optionally save results of finding improvements in previous TPG rounds to this file',
	)
	argparser.add_argument(
		'--name',
		'--player-name',
		type=str,
		help='With --rounds-path, your name as it appears in the TPG data, otherwise results may be nonsensical',
	)
	argparser.add_argument(
		'--distances-output-path',
		type=Path,
		help='Optionally save distances of each new pic to closest existing pic',
	)
	argparser.add_argument(
		'--find-if-any-pics-better',
		action=BooleanOptionalAction,
		help='Find any instance of a point in new_points being the new closest point for anywhere in targets, has no effect if --targets is not specified. Defaults to true',
		default=True,
	)
	argparser.add_argument(
		'--use-haversine',
		action=BooleanOptionalAction,
		help='Use haversine for distances, defaults to true',
		default=True,
	)
	argparser.add_argument(
		'--output-path', type=Path, help='Optional path to save improved distances to targets'
	)
	# TODO: Option for new_points to just be one point
	# TODO: lat/lng/blah column name options
	# TODO: Allow eval_with_targets while just using the rounds from --rounds-path instead of specifying targets
	# TODO: Allow specifying either username or player display name for --player, in some intuitive/non-stupid way
	args = argparser.parse_args()

	points = load_points(args.existing_points)
	new_points = load_points(args.new_points)

	points = try_set_index_name_col(points)
	new_points = try_set_index_name_col(new_points)

	distances = get_distances(points, new_points)
	if args.threshold:
		num_under = (distances['distance'] < args.threshold).sum()
		if num_under:
			print(
				f'Ignoring {num_under} new points as they are within {format_distance(args.threshold)} from existing points'
			)
		distances = distances[distances['distance'] >= args.threshold]
	distances['distance'] = distances['distance'].map(format_distance)
	distances['coords'] = distances['geometry'].map(format_point)
	# Remember that coords here is for the new point, not closest, which might be unclear if you come back to look at this code later
	print('Distances from existing points:')
	print(distances.drop(columns='geometry').set_index('new_point'))
	distances = distances.drop(columns='coords')

	if args.distances_output_path:
		output_geodataframe(distances, args.distances_output_path, index=False)
	if args.targets:
		eval_with_targets(
			points,
			new_points,
			args.targets,
			args.output_path,
			find_if_any_pics_better=args.find_if_any_pics_better,
			use_haversine=args.use_haversine,
		)
	if args.rounds_path:
		eval_with_rounds(
			points.geometry,
			new_points.geometry,
			args.rounds_path,
			args.name,
			args.rounds_output_path,
			use_haversine=args.use_haversine,
		)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	with logging_redirect_tqdm():
		main()

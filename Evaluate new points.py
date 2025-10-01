#!/usr/bin/env python3
"""Given an existing set of points (photos that one has for submitting to TPG, etc), and a set of new points to be added (e.g. new destinations to consider travelling to), finds out what is the most optimal."""

import logging
from argparse import ZERO_OR_MORE, ArgumentParser, BooleanOptionalAction
from pathlib import Path

import geopandas
import pandas
from pandas import Index, RangeIndex
from shapely import Point
from tqdm.auto import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from travelpygame.new_pic_eval import (
	find_if_new_pics_better,
	find_new_pics_better_individually,
	get_worst_point,
	load_points_or_rounds,
)
from travelpygame.util import (
	format_distance,
	format_point,
	geodataframe_to_csv,
	get_closest_point_index,
	load_points,
	try_set_index_name_col,
)


def get_distances(points: geopandas.GeoDataFrame, new_points: geopandas.GeoDataFrame):
	points_geom = points.geometry.to_numpy()

	rows = []
	with tqdm(new_points.geometry.items(), 'Calculating distances', new_points.index.size, unit='point') as t:
		for index, new_point in t:
			t.set_postfix(new_point=index)
			if not isinstance(new_point, Point):
				raise TypeError(
					f'new points contained {type(new_point)} at {index} instead of Point'
				)
			closest_index, distance = get_closest_point_index(new_point, points_geom)
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
	points: geopandas.GeoDataFrame,
	new_points: geopandas.GeoDataFrame,
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
	if isinstance(targets.index, RangeIndex):
		# Try and get something more descriptive than just the default increasing index
		targets.index = Index(
			[format_point(geo) if isinstance(geo, Point) else str(geo) for geo in targets.geometry]
		)

	if find_if_any_pics_better:
		better = get_where_pics_better(points, new_points, targets, use_haversine=use_haversine)
		print('Number of times each pic was better (with all new pics at once):')
		print(better['new_best'].value_counts())
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
	diffs = diffs.sort_values('mean', ascending=False)
	for c in ('total', 'best', 'mean'):
		diffs[c] = diffs[c].map(format_distance)
	print(diffs)


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
	argparser.add_argument('--use-haversine', action=BooleanOptionalAction)
	argparser.add_argument('--output-path', type=Path)
	# TODO: Option for new_points to just be one point
	# TODO: lat/lng/blah column name options
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
		geodataframe_to_csv(distances, args.output_path, index=False)
	if args.targets:
		eval_with_targets(
			points,
			new_points,
			args.targets,
			args.output_path,
			find_if_any_pics_better=args.find_if_any_pics_better,
			use_haversine=args.use_haversine,
		)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	with logging_redirect_tqdm():
		main()

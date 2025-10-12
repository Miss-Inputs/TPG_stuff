#!/usr/bin/env python3
"""Get distances between point set(s)."""

import logging
from argparse import ArgumentParser, Namespace
from argparse import _ArgumentGroup as ArgumentGroup
from collections.abc import Hashable
from pathlib import Path

import pandas
import pyproj
from tqdm.contrib.logging import logging_redirect_tqdm
from travelpygame.point_set_stats import (
	PointSetDistanceMethod,
	PointSetDistanceMethodType,
	get_distance_method_combinations,
	get_point_set_distance,
	validate_points,
)
from travelpygame.util import (
	first_unique_column_label,
	format_dataframe,
	format_distance,
	format_number,
	format_point,
	load_points,
	maybe_set_index_name_col,
)

logger = logging.getLogger(Path(__file__).stem)

wgs84 = pyproj.CRS('WGS84')


def load_and_validate(
	path: Path,
	lat_col: Hashable | None = None,
	lng_col: Hashable | None = None,
	input_crs: str | None = None,
	name_col: str | None = None,
	*,
	unheadered: bool = False,
):
	gdf = load_points(
		path, lat_col, lng_col, input_crs or wgs84, has_header=False if unheadered else None
	)
	if not gdf.crs:
		logger.warning('%s had no CRS, which should never happen', path.name)
		gdf = gdf.set_crs(wgs84)
	elif not gdf.crs.equals(wgs84):
		logger.warning('%s had CRS %s, converting to WGS84', path.name, gdf.crs)
		gdf = gdf.to_crs(wgs84)

	gdf, new_name_col = maybe_set_index_name_col(gdf, name_col, path.name)
	if new_name_col:
		if not name_col:
			logger.info('Autodetected name column for %s as %s', path.name, new_name_col)
	else:
		first_unique = first_unique_column_label(gdf.drop(columns=gdf.active_geometry_name))
		gdf, new_name_col = maybe_set_index_name_col(gdf, first_unique, try_autodetect=False)
		if new_name_col and not name_col:
			logger.info(
				'Autodetected name column for %s as first unique column (%s)',
				path.name,
				new_name_col,
			)

	if not new_name_col:
		gdf.index = pandas.Index(gdf.geometry.map(format_point))
	print(f'{path.name}: {gdf.index.size} items')
	gs = validate_points(gdf, name_for_log=path)
	gs = gs.rename(path.stem)
	print(f'{path.name} after validation: {gs.size} items')
	return gs


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


def compare_two_paths(
	left_path: Path, right_path: Path, args: Namespace, method: PointSetDistanceMethodType | None
):
	left = load_and_validate(
		left_path,
		args.lat_col_left,
		args.lng_col_left,
		args.crs_left,
		args.name_col_left,
		unheadered=args.unheadered_left,
	)
	right = load_and_validate(
		right_path,
		args.lat_col_right,
		args.lng_col_right,
		args.crs_right,
		args.name_col_right,
		unheadered=args.unheadered_right,
	)

	if not method:
		scores = {}
		for i, meth in enumerate(PointSetDistanceMethod):
			score, closest_dist, closest_a, closest_b = get_point_set_distance(
				left, right, meth, use_tqdm=False
			)
			if i == 0:
				print(
					f'Closest pair of points: {closest_a} and {closest_b}, {format_distance(closest_dist)}'
				)
			scores[meth.name] = score
		for meth, score in scores.items():
			print(f'Dissimilarity from {meth}: {format_number(score)}')

	else:
		score, closest_dist, closest_a, closest_b = get_point_set_distance(left, right, method)
		print(f'Dissimilarity: {format_number(score)}')
		print(
			f'Closest pair of points: {closest_a} and {closest_b}, {format_distance(closest_dist)}'
		)


def compare_one_to_many(
	left_path: Path,
	right_paths: list[Path],
	args: Namespace,
	method: PointSetDistanceMethodType | None,
):
	left = load_and_validate(
		left_path,
		args.lat_col_left,
		args.lng_col_left,
		args.crs_left,
		args.name_col_left,
		unheadered=args.unheadered_left,
	)
	rows = {}
	for path in right_paths:
		right = load_and_validate(path)
		row = {}
		if method:
			diff, dist, closest_left, closest_right = get_point_set_distance(left, right, method)
			row['closest_distance'] = dist
			row['closest_left'] = closest_left
			row['closest_right'] = closest_right
			row['dissimilarity'] = diff
		else:
			for meth in PointSetDistanceMethod:
				diff, dist, closest_left, closest_right = get_point_set_distance(
					left, right, meth, use_tqdm=False
				)
				row['closest_distance'] = dist
				row['closest_left'] = closest_left
				row['closest_right'] = closest_right
				row[meth.name] = diff
		rows[right.name or path.stem] = row
	df = pandas.DataFrame.from_dict(rows, 'index')
	diff_cols = df.columns[~df.columns.str.startswith('closest_')]
	print(format_dataframe(df, 'closest_distance', number_cols=diff_cols))
	if method is None:
		closest_by_method = df[diff_cols].idxmin(axis='index')
		min_diff_by_method = df[diff_cols].min(axis='index')
		furthest_by_method = df[diff_cols].idxmax(axis='index')
		max_diff_by_method = df[diff_cols].max(axis='index')
		print(
			format_dataframe(
				pandas.DataFrame(
					{
						'closest': closest_by_method,
						'min_diff': min_diff_by_method,
						'furthest': furthest_by_method,
						'max_diff': max_diff_by_method,
					}
				),
				number_cols=('min_diff', 'max_diff'),
			)
		)


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
		'left_path',
		type=Path,
		help='Path to left point set (.csv, .ods, .xls, .xlsx, pickled DataFrame, GeoJSON, etc)',
	)
	right_group.add_argument(
		'right_path',
		type=Path,
		nargs='+',
		help='Path to right point set (.csv, .ods, .xls, .xlsx, pickled DataFrame, GeoJSON, etc)',
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
	right_paths = args.right_path
	if len(right_paths) == 1:
		compare_two_paths(args.left_path, right_paths[0], args, method)
	else:
		compare_one_to_many(args.left_path, right_paths, args, method)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	with logging_redirect_tqdm():
		main()

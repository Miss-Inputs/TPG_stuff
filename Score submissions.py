#!/usr/bin/env python3
"""
Scores an exported submission tracker and sorts the entries, can use KML or also CSV (but the CSV export sucks)

For CSV, use export data -> CSV from hamburger menu on folder in submission tracker, assumes the first line is the target location
Expects columns: WKT, name, description
"""

from argparse import ArgumentParser, BooleanOptionalAction
from collections import defaultdict
from pathlib import Path

import geopandas
import numpy
import pandas
from shapely import Point

from lib.geo_utils import geod_distance_and_bearing, haversine_distance
from lib.io_utils import geodataframe_to_csv
from lib.kml import parse_submission_kml
from lib.tpg_utils import custom_tpg_score


def calc_scores(
	target: Point,
	gdf: geopandas.GeoDataFrame,
	world_distance: float = 5000.0,
	*,
	use_haversine_for_score: bool = True,
) -> geopandas.GeoDataFrame:
	n = gdf.index.size
	target_y = numpy.repeat(target.y, n)
	target_x = numpy.repeat(target.x, n)
	distance_raw, bearing = geod_distance_and_bearing(
		gdf.geometry.y, gdf.geometry.x, target_y, target_x
	)
	gdf['bearing'] = bearing
	geod_distance = pandas.Series(distance_raw, index=gdf.index) / 1000.0
	haversine = (
		haversine_distance(gdf.geometry.y.to_numpy(), gdf.geometry.x.to_numpy(), target_y, target_x)
		/ 1000.0
	)

	if use_haversine_for_score:
		gdf['distance'] = haversine
		gdf['geod_distance'] = geod_distance
	else:
		gdf['distance'] = geod_distance
		gdf['haversine'] = haversine

	# TODO: Custom 5K threshold, or more usefully perhaps something to allow a custom 5K point (for when 5Ks should not be the exact location due to being private property etc)
	scores = custom_tpg_score(gdf['distance'], world_distance)
	gdf['score'] = scores
	gdf['rank'] = gdf['score'].rank(ascending=False).astype(int)
	return gdf.sort_values('score', ascending=False)


def parse_csv(path: Path):
	df = pandas.read_csv(path, on_bad_lines='warn')
	gs = geopandas.GeoSeries.from_wkt(df.pop('WKT'), crs='wgs84')
	gdf = geopandas.GeoDataFrame(df, geometry=gs)

	target = gdf.geometry.iloc[0]
	if not isinstance(target, Point):
		raise TypeError(f'uh oh target is {type(target)}')
	return target, gdf.tail(-1)


def _make_leaderboard(
	data: dict[str, dict[str, float]], name: str, *, ascending: bool = False, dropna: bool = False
):
	df = pandas.DataFrame(data)
	if dropna:
		df = df.dropna()
	df.insert(0, 'Total', df.sum(axis='columns'))
	return df.sort_values('Total', ascending=ascending).rename_axis(index=name)


def score_kml(path: Path, world_distance: float = 5000.0, *, use_haversine_for_score: bool = True):
	submission_tracker = parse_submission_kml(path)
	points_leaderboard: defaultdict[str, dict[str, float]] = defaultdict(dict)
	"""{round name: {submission name: score}}"""
	distance_leaderboard: defaultdict[str, dict[str, float]] = defaultdict(dict)
	"""{round name: {submission name: distance in km}}"""

	for r in submission_tracker.rounds:
		data = {
			submission.name: {
				'desc': submission.description,
				'style': submission.style,
				'point': submission.point,
			}
			for submission in r.submissions
		}
		df = pandas.DataFrame.from_dict(data, orient='index')
		gdf = geopandas.GeoDataFrame(df, geometry='point', crs='wgs84')
		gdf.index.name = r.name

		gdf = calc_scores(
			r.target, gdf, world_distance, use_haversine_for_score=use_haversine_for_score
		)
		print(gdf)
		print('-' * 10)
		out_path = path.with_name(f'{path.stem} - {r.name}.csv')
		geodataframe_to_csv(gdf, out_path)

		for name, row in gdf.iterrows():
			assert isinstance(name, str), f'name is {type(name)}'
			points_leaderboard[r.name][name] = row['score']
			distance_leaderboard[r.name][name] = row['distance']

	points_leaderboard_df = _make_leaderboard(points_leaderboard, 'Points')
	print(points_leaderboard_df)
	points_leaderboard_df.to_csv(path.with_name(f'{path.stem} - Points Leaderboard.csv'))
	distance_leaderboard_df = _make_leaderboard(
		distance_leaderboard, 'Distance', ascending=True, dropna=True
	)
	print(distance_leaderboard_df)
	distance_leaderboard_df.to_csv(path.with_name(f'{path.stem} - Distance Leaderboard.csv'))


def main() -> None:
	argparser = ArgumentParser()
	argparser.add_argument('path', type=Path, help='Path to CSV/KML file')
	argparser.add_argument(
		'--world-distance',
		type=float,
		help='Max distance in the world (in km), used for calculating scoring, defaults to 5000km',
		default=5_000.0,
	)
	argparser.add_argument(
		'--use-haversine',
		action=BooleanOptionalAction,
		help='Use haversine instead of WGS geod for scoring (less accurate as it assumes the earth is a sphere, but more consistent with other TPG things), defaults to True',
		default=True,
	)
	args = argparser.parse_args()

	path: Path = args.path
	world_distance: float = args.world_distance
	use_haversine: bool = args.use_haversine
	ext = path.suffix[1:].lower()

	if ext == 'csv':
		target, gdf = parse_csv(path)
		gdf = gdf.set_index('name', verify_integrity=True)
		gdf = calc_scores(target, gdf, world_distance, use_haversine_for_score=use_haversine)

		print(gdf)
		out_path = path.with_stem(f'{path.stem} scores')
		geodataframe_to_csv(gdf, out_path)
	elif ext == 'kml':
		score_kml(path)
	else:
		raise ValueError(f'Unknown extension: {ext}')


if __name__ == '__main__':
	main()

#!/usr/bin/env python3
"""
Scores an exported submission tracker and sorts the entries, can use KML or also CSV (but the CSV export sucks)

For CSV, use export data -> CSV from hamburger menu on folder in submission tracker, assumes the first line is the target location
Expects columns: WKT, name, description
"""

from argparse import ArgumentParser, BooleanOptionalAction
from collections import defaultdict
from collections.abc import Sequence
from pathlib import Path

import geopandas
import numpy
import pandas
from shapely import Point

from lib.geo_utils import geod_distance_and_bearing, haversine_distance
from lib.io_utils import geodataframe_to_csv
from lib.kml import SubmissionTrackerRound, parse_submission_kml
from lib.tpg_utils import Medal, count_medals, custom_tpg_score


def calc_scores(
	target: Point,
	gdf: geopandas.GeoDataFrame,
	world_distance: float = 5000.0,
	fivek_threshold: float = 0.1,
	*,
	use_haversine_for_score: bool = True,
	allow_negative: bool = False,
) -> geopandas.GeoDataFrame:
	n = gdf.index.size
	x = gdf.geometry.x
	y = gdf.geometry.y
	target_y = numpy.repeat(target.y, n)
	target_x = numpy.repeat(target.x, n)
	distance_raw, bearing = geod_distance_and_bearing(y, x, target_y, target_x)
	gdf['bearing'] = bearing
	geod_distance = pandas.Series(distance_raw, index=gdf.index) / 1000.0
	haversine = haversine_distance(y.to_numpy(), x.to_numpy(), target_y, target_x) / 1000.0

	if use_haversine_for_score:
		gdf['distance'] = haversine
		gdf['geod_distance'] = geod_distance
	else:
		gdf['distance'] = geod_distance
		gdf['haversine'] = haversine

	scores = custom_tpg_score(
		gdf['distance'],
		world_distance,
		fivek_threshold=fivek_threshold,
		allow_negative=allow_negative,
	)
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
	num_rounds = df.columns.size
	df.insert(0, 'Total', df.sum(axis='columns'))
	df.insert(1, 'Average', df['Total'] / num_rounds)
	return df.sort_values('Total', ascending=ascending).rename_axis(index=name)


def _iter_scored_rounds(
	rounds: Sequence[SubmissionTrackerRound],
	world_distance: float = 5000.0,
	fivek_threshold: float = 0.1,
	*,
	use_haversine_for_score: bool = True,
	allow_negative: bool = False,
):
	for r in rounds:
		#TODO: Check that there are no duplicates
		data = {
			submission.name: {
				'desc': submission.description,
				'style': submission.style,
				# TODO: I guess we should probably parse style
				'point': submission.point,
			}
			for submission in r.submissions
		}
		df = pandas.DataFrame.from_dict(data, orient='index')
		gdf = geopandas.GeoDataFrame(df, geometry='point', crs='wgs84')
		gdf.index.name = r.name

		yield (
			r,
			calc_scores(
				r.target,
				gdf,
				world_distance,
				fivek_threshold,
				use_haversine_for_score=use_haversine_for_score,
				allow_negative=allow_negative,
			),
		)


def score_kml(
	path: Path,
	world_distance: float = 5000.0,
	fivek_threshold: float = 0.1,
	*,
	use_haversine_for_score: bool = True,
	ignore_ongoing: bool = False,
	allow_negative: bool = False,
):
	submission_tracker = parse_submission_kml(path)
	points_leaderboard: defaultdict[str, dict[str, float]] = defaultdict(dict)
	"""{round name: {player name: score}}"""
	distance_leaderboard: defaultdict[str, dict[str, float]] = defaultdict(dict)
	"""{round name: {player name: distance in km}}"""
	medals: defaultdict[str, list[Medal]] = defaultdict(list)
	"""{player name: [medals from all rounds that were on the podium]}"""

	rounds = submission_tracker.rounds
	if ignore_ongoing:
		rounds = rounds[:-1]

	for r, gdf in _iter_scored_rounds(
		rounds,
		world_distance,
		fivek_threshold,
		use_haversine_for_score=use_haversine_for_score,
		allow_negative=allow_negative,
	):
		print(gdf.drop(columns='style'))
		print('-' * 10)
		out_path = path.with_name(f'{path.stem} - {r.name}.csv')
		geodataframe_to_csv(gdf, out_path)

		for name, row in gdf.iterrows():
			assert isinstance(name, str), f'name is {type(name)}'
			points_leaderboard[r.name][name] = row['score']
			distance_leaderboard[r.name][name] = row['distance']
			if row['rank'] <= 3:
				medals[name].append(Medal(4 - row['rank']))

	points_leaderboard_df = _make_leaderboard(points_leaderboard, 'Points')
	print(points_leaderboard_df)
	points_leaderboard_df.to_csv(path.with_name(f'{path.stem} - Points Leaderboard.csv'))
	distance_leaderboard_df = _make_leaderboard(
		distance_leaderboard, 'Distance', ascending=True, dropna=True
	)
	print(distance_leaderboard_df)
	distance_leaderboard_df.to_csv(path.with_name(f'{path.stem} - Distance Leaderboard.csv'))
	medals_leaderboard = count_medals(medals)
	print(medals_leaderboard)
	medals_leaderboard.to_csv(path.with_name(f'{path.stem} - Medals Leaderboard.csv'))


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
		'--fivek-threshold',
		type=float,
		help='Threshold for a submission being close enough to be considered a 5K, used for calculating scoring, defaults to 100m',
		default=0.1,
	)
	argparser.add_argument(
		'--use-haversine',
		action=BooleanOptionalAction,
		help='Use haversine instead of WGS geod for scoring (less accurate as it assumes the earth is a sphere, but more consistent with other TPG things), defaults to True',
		default=True,
	)
	argparser.add_argument(
		'--ongoing-round',
		action=BooleanOptionalAction,
		help='Ignore the last round for the leaderboard and treat it as currently ongoing, defaults to False (ie run this after the round finishes)',
		default=False,
	)
	argparser.add_argument(
		'--allow-negative',
		action=BooleanOptionalAction,
		help='Allow negative scores for distance if greater than --world-distance km, defaults to False which gives a score of 0 for very far away submissions instead',
		default=False,
	)
	args = argparser.parse_args()

	path: Path = args.path
	world_distance: float = args.world_distance
	fivek_threshold: float = args.fivek_threshold
	use_haversine: bool = args.use_haversine
	allow_negative: bool = args.allow_negative

	ext = path.suffix[1:].lower()

	if ext == 'csv':
		target, gdf = parse_csv(path)
		gdf = gdf.set_index('name', verify_integrity=True)
		gdf = calc_scores(
			target,
			gdf,
			world_distance,
			fivek_threshold,
			use_haversine_for_score=use_haversine,
			allow_negative=allow_negative,
		)

		print(gdf)
		out_path = path.with_stem(f'{path.stem} scores')
		geodataframe_to_csv(gdf, out_path)
	elif ext in {'kml', 'kmz'}:
		score_kml(
			path,
			world_distance,
			fivek_threshold,
			use_haversine_for_score=use_haversine,
			ignore_ongoing=args.ongoing_round,
			allow_negative=allow_negative,
		)
	else:
		raise ValueError(f'Unknown extension: {ext}')


if __name__ == '__main__':
	main()

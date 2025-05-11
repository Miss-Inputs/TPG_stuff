#!/usr/bin/env python3
"""
Scores an exported submission tracker and sorts the entries, can use KML or also CSV (but the CSV export sucks)

For CSV, use export data -> CSV from hamburger menu on folder in submission tracker, assumes the first line is the target location
Expects columns: WKT, name, description
"""

from argparse import ArgumentParser, BooleanOptionalAction
from pathlib import Path

import geopandas
import numpy
import pandas
from shapely import Point

from lib.geo_utils import geod_distance_and_bearing, haversine_distance
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


def score_kml(path: Path, world_distance: float = 5000.0, *, use_haversine_for_score: bool = True):
	submission_tracker = parse_submission_kml(path)
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
		print(r.name)
		print(gdf)
		print('-' * 10)


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
		gdf.to_csv(out_path)
	elif ext == 'kml':
		score_kml(path)
	else:
		raise ValueError(f'Unknown extension: {ext}')


if __name__ == '__main__':
	main()

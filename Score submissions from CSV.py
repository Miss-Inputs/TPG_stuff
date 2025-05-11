#!/usr/bin/env python3
"""Use export data -> CSV from hamburger menu on folder in submission tracker, assumes the first line is the target location
Expects columns: WKT, name, description
"""

from argparse import ArgumentParser, BooleanOptionalAction
from pathlib import Path

import geopandas
import numpy
import pandas
from shapely import Point

from lib.geo_utils import geod_distance_and_bearing, haversine_distance
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


def main() -> None:
	argparser = ArgumentParser()
	argparser.add_argument('csv', type=Path, help='Path to CSV file')
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

	path: Path = args.csv
	df = pandas.read_csv(path, on_bad_lines='warn')
	gs = geopandas.GeoSeries.from_wkt(df.pop('WKT'), crs='wgs84')
	gdf = geopandas.GeoDataFrame(df, geometry=gs)

	target = gdf.geometry.iloc[0]
	if not isinstance(target, Point):
		raise TypeError(f'uh oh target is {type(target)}')
	gdf = gdf.tail(-1)
	gdf = gdf.set_index('name', verify_integrity=True)
	gdf = calc_scores(target, gdf, args.world_distance, use_haversine_for_score=args.use_haversine)

	print(gdf)
	out_path = path.with_stem(f'{path.stem} scores')
	gdf.to_csv(out_path)


if __name__ == '__main__':
	main()

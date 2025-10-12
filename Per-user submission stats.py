#!/usr/bin/env python3

import asyncio
import logging
from argparse import ArgumentParser
from pathlib import Path

import geopandas
import pandas
import shapely
from tqdm.auto import tqdm
from travelpygame import find_furthest_point, get_submissions_per_user_with_path
from travelpygame.util import (
	circular_mean_points,
	format_dataframe,
	format_point,
	geod_distance,
	get_centroid,
	wgs84_geod,
)

from lib.settings import Settings


def concave_hull_of_user(all_points: shapely.MultiPoint):
	if len(all_points.geoms) == 1:
		return None, 0, 0
	if len(all_points.geoms) == 2:
		point_1, point_2 = all_points.geoms
		assert isinstance(point_1, shapely.Point), type(point_1)
		assert isinstance(point_2, shapely.Point), type(point_1)
		distance = geod_distance(point_1, point_2)
		return None, distance, distance
	hull = shapely.concave_hull(all_points, allow_holes=True)
	area, perimeter = wgs84_geod.geometry_area_perimeter(hull)
	area = abs(area)

	return hull, area, perimeter


def stats_for_each_user(
	per_user: dict[str, geopandas.GeoSeries], threshold: int | None = None
) -> pandas.DataFrame:
	if threshold:
		per_user = {name: points for name, points in per_user.items() if points.size >= threshold}
	data = {}
	with tqdm(per_user.items(), 'Calculating stats', unit='player') as t:
		for name, points in t:
			t.set_postfix(name=name)
			# TODO: These should all be parameters whether to calculate each particular stat or not
			# crs = geo.estimate_utm_crs()
			# Using that for get_centroid seems like a good idea, but it causes infinite coordinates for some people who have travelled too much, so that's no good
			all_points = points.to_numpy()
			all_points_mp = shapely.MultiPoint(all_points)
			hull = concave_hull_of_user(all_points_mp)
			furthest_point, furthest_distance = find_furthest_point(
				all_points, max_iter=1000, use_tqdm=False
			)
			stats = {
				'count': points.size,
				'average_point': circular_mean_points(points),
				'centroid': get_centroid(all_points_mp),
				'antipoint': furthest_point,
				'furthest_distance': furthest_distance,
				'concave_hull': hull[0],
				'concave_hull_area': hull[1],
				'concave_hull_perimeter': hull[2],
			}
			data[name] = stats
	df = pandas.DataFrame.from_dict(data, 'index')
	df = df.reset_index(names='name')
	return df.sort_values('concave_hull_area', ascending=False, ignore_index=True)


async def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument(
		'path',
		nargs='?',
		type=Path,
		help='Path to load submissions from, if this is not specified will try the SUBS_PER_USER_PATH environment variable if set, or if that is not set then MAIN_TPG_DATA_FILE.',
	)
	argparser.add_argument(
		'--threshold',
		type=int,
		help='Only take into account players who have submitted at least this amount of pics.',
	)
	args = argparser.parse_args()
	path: Path | None = args.path
	if not path:
		settings = Settings()
		path = settings.subs_per_user_path or settings.main_tpg_data_path
	subs = await get_submissions_per_user_with_path(path)

	stats = stats_for_each_user(subs, args.threshold)

	print(
		format_dataframe(
			stats,
			distance_cols=('concave_hull_perimeter', 'furthest_distance'),
			point_cols=('centroid', 'average_point', 'antipoint'),
			area_cols='concave_hull_area',
		)
	)
	# I should make these paths configurable but I didn't and haven't, and should
	antipoint_stats = stats[['name', 'antipoint', 'furthest_distance', 'count']].sort_values(
		'furthest_distance'
	)
	antipoint_stats['antipoint'] = antipoint_stats['antipoint'].map(format_point)
	antipoint_stats.to_csv('/tmp/antipoint_stats.csv', index=False)
	stats.to_csv('/tmp/stats.csv', index=False)
	geopandas.GeoDataFrame(stats[['name', 'antipoint']], geometry='antipoint', crs='wgs84').to_file(
		'/tmp/antipoints.geojson'
	)
	geopandas.GeoDataFrame(
		stats[['name', 'average_point']], geometry='average_point', crs='wgs84'
	).to_file('/tmp/average_points.geojson')
	geopandas.GeoDataFrame(stats[['name', 'centroid']], geometry='centroid', crs='wgs84').to_file(
		'/tmp/centroids.geojson'
	)
	geopandas.GeoDataFrame(
		stats[['name', 'concave_hull', 'concave_hull_area', 'concave_hull_perimeter']],
		geometry='concave_hull',
		crs='wgs84',
	).dropna().to_file('/tmp/concave_hulls.geojson')


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	asyncio.run(main())

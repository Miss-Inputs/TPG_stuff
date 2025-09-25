#!/usr/bin/env python3

import logging

import geopandas
import pandas
import shapely
from tqdm.auto import tqdm
from travelpygame.util import (
	circular_mean_points,
	format_point,
	geod_distance,
	get_centroid,
	wgs84_geod,
)

from lib.geo_utils import find_furthest_point_via_optimization
from lib.io_utils import latest_file_matching_format_pattern
from settings import Settings


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


def stats_for_each_user(submissions: geopandas.GeoDataFrame):
	grouper = submissions.groupby('name')
	data = {}
	with tqdm(grouper, 'Calculating stats', grouper.ngroups) as t:
		for name, group in t:
			t.set_postfix(name=name)
			# TODO: These should all be parameters whether to calculate each particular stat or not
			geo = group.geometry
			assert isinstance(geo, geopandas.GeoSeries), type(geo)
			# crs = geo.estimate_utm_crs()
			# Using that for get_centroid seems like a good idea, but it causes infinite coordinates for some people who have travelled too much, so that's no good
			geo = geo.drop_duplicates()
			all_points = geo.to_numpy()
			all_points_mp = shapely.MultiPoint(all_points)
			hull = concave_hull_of_user(all_points_mp)
			furthest_point, furthest_distance = find_furthest_point_via_optimization(
				all_points, max_iter=1000, use_tqdm=False
			)
			stats = {
				'average_point': circular_mean_points(all_points),
				'centroid': get_centroid(all_points_mp),
				'antipoint': furthest_point,
				'furthest_distance': furthest_distance,
				'concave_hull': hull[0],
				'concave_hull_area': hull[1],
				'concave_hull_perimeter': hull[2],
			}
			data[name] = stats
	df = pandas.DataFrame.from_dict(data, 'index')
	df = df.merge(grouper.size().rename('count'), how='left', left_index=True, right_index=True)
	df = df.reset_index(names='name')
	return df.sort_values('concave_hull_area', ascending=False)


def main() -> None:
	settings = Settings()
	if not settings.submissions_path:
		raise RuntimeError('need submissions_path, run All TPG submissions.py first')
	# TODO: This should be more generic and should be able to take some other file so it can work with spinoffs and such
	path = latest_file_matching_format_pattern(settings.submissions_path.with_suffix('.geojson'))

	submissions: geopandas.GeoDataFrame = geopandas.read_file(path)
	stats = stats_for_each_user(submissions)

	print(stats)
	# I should make these paths configurable but I didn't and haven't, and should
	antipoint_stats = stats[['name', 'antipoint', 'furthest_distance', 'count']].copy()
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
	main()

#!/usr/bin/env python3

import geopandas

from lib.geo_utils import circular_mean_points
from lib.io_utils import format_path, latest_file_matching_format_pattern
from settings import Settings


def _circular_mean_group(points: geopandas.GeoDataFrame):
	return circular_mean_points(points.to_numpy())


def get_average_by_user(submissions: geopandas.GeoDataFrame):
	grouper = submissions.groupby('name')
	centroids = grouper['geometry'].agg(_circular_mean_group)
	centroids = centroids.reset_index()
	centroids = centroids.merge(
		grouper.size().rename('count'), how='left', left_on='name', right_index=True
	)
	assert isinstance(centroids, geopandas.GeoDataFrame), type(centroids)
	return centroids.set_crs('wgs84')


def main() -> None:
	settings = Settings()
	if not settings.submissions_path:
		raise RuntimeError('need submissions_path, run All TPG submissions.py first')
	path = latest_file_matching_format_pattern(settings.submissions_path.with_suffix('.geojson'))

	# Using the .geojson just to filter out duplicate (reused) submissions, do we actually want that?
	submissions: geopandas.GeoDataFrame = geopandas.read_file(path)
	centroids = get_average_by_user(submissions)
	print(centroids)

	if settings.average_per_user_path:
		output_path = format_path(settings.average_per_user_path, submissions['latest_round'].max())
		centroids.to_file(output_path)
		centroids[centroids['count'] > 1].to_file(
			output_path.with_stem(f'{output_path.stem} (only multiple submissions)')
		)


if __name__ == '__main__':
	main()

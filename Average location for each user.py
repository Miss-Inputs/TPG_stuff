#!/usr/bin/env python3

import geopandas

from lib.geo_utils import circular_mean_points
from lib.io_utils import latest_file_matching_format_pattern
from settings import Settings


def _circular_mean_group(points: geopandas.GeoDataFrame):
	return circular_mean_points(points.to_numpy())


def main() -> None:
	settings = Settings()
	if not settings.submissions_path:
		raise RuntimeError('need submissions_path, run All TPG submissions.py first')
	# Using the .geojson just to filter out duplicate (reused) submissions, do we actually want that?
	path = latest_file_matching_format_pattern(settings.submissions_path.with_suffix('.geojson'))
	print(path)

	gdf: geopandas.GeoDataFrame = geopandas.read_file(path)
	centroids = gdf.groupby('name')['geometry'].agg(_circular_mean_group)
	print(centroids)
	centroids = centroids.reset_index()
	assert isinstance(centroids, geopandas.GeoDataFrame)
	centroids = centroids.set_crs('wgs84')
	print(centroids)

	if settings.average_per_user_path:
		output_path = settings.average_per_user_path.with_stem(
			settings.average_per_user_path.stem.format(gdf['latest_round'].max())
		)
		centroids.to_file(output_path)

	# Test: Between Wairiki on Taveuni Island in Fiji, and Bourma on the other side (+ a bit south). Should be somewhere in the middle!
	# yeah nah that looks alright mate
	# print(
	# 	circular_mean_points(
	# 		[
	# 			shapely.Point(179.99220627435636, -16.8084711158926),
	# 			shapely.Point(-179.87360486980262, -16.82108453972491),
	# 		]
	# 	)
	# )


if __name__ == '__main__':
	main()

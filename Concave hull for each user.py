#!/usr/bin/env python3

import geopandas
import pandas
import shapely
from tqdm.auto import tqdm

from lib.geo_utils import geod, geod_distance
from lib.io_utils import latest_file_matching_format_pattern
from settings import Settings


def _concave_hull_group(group: pandas.DataFrame):
	assert isinstance(group, geopandas.GeoDataFrame)
	if group.index.size == 1:
		return {'geometry': None, 'area': 0}
	if group.index.size == 2:
		point_1, point_2 = group.geometry
		assert isinstance(point_1, shapely.Point), type(point_1)
		assert isinstance(point_2, shapely.Point), type(point_1)
		return {'geometry': None, 'area': geod_distance(point_1, point_2)}
	union = group.union_all()
	hull = shapely.concave_hull(union, allow_holes=True)
	area, perimeter = geod.geometry_area_perimeter(hull)
	area = abs(area)

	return {
		'geometry': hull,
		'area': area,
		'perimeter': perimeter,
		'latest_round': group['latest_round'].max(),
	}


def get_concave_for_each_user(submissions: geopandas.GeoDataFrame):
	grouper = submissions.groupby('name')
	data = {
		name: _concave_hull_group(group)
		for name, group in tqdm(grouper, 'Calculating hulls', grouper.ngroups)
	}
	hulls = pandas.DataFrame.from_dict(data, 'index')
	hulls = hulls.merge(
		grouper.size().rename('count'), how='left', left_index=True, right_index=True
	)
	hulls = hulls.reset_index(names='name')
	if isinstance(hulls, geopandas.GeoDataFrame):
		print(hulls, hulls.crs)
	hulls = geopandas.GeoDataFrame(hulls, crs='wgs84')
	return hulls.sort_values('area', ascending=False)


def main() -> None:
	settings = Settings()
	if not settings.submissions_path:
		raise RuntimeError('need submissions_path, run All TPG submissions.py first')
	path = latest_file_matching_format_pattern(settings.submissions_path.with_suffix('.geojson'))

	submissions: geopandas.GeoDataFrame = geopandas.read_file(path)
	hulls = get_concave_for_each_user(submissions)

	#I should make these paths configurable but I didn't
	print(hulls)
	hulls.drop(columns='geometry').to_csv('/tmp/hull_areas.csv', index=False)
	hulls.dropna(subset='geometry').to_file('/tmp/hulls.geojson')
	hulls.dropna(subset='geometry').head(1).to_file('/tmp/hulls top 1.geojson')
	hulls.dropna(subset='geometry').tail(30).to_file('/tmp/hulls bottom 30.geojson')


if __name__ == '__main__':
	main()

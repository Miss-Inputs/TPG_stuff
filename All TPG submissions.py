#!/usr/bin/env python3

import geopandas
import pandas

from lib.io_utils import format_path, latest_file_matching_format_pattern
from lib.tastycheese_map import load_or_get_submissions
from settings import Settings


def _group_rounds(group: pandas.DataFrame):
	first = group.iloc[0].copy()
	rounds = sorted(group['round'].unique())
	first['round'] = ', '.join(str(n) for n in rounds)
	first['first_round'] = rounds[0]
	first['latest_round'] = rounds[-1]
	return first


def main() -> None:
	settings = Settings()
	if settings.rounds_path:
		rounds_path = latest_file_matching_format_pattern(settings.rounds_path)
		rounds = geopandas.read_file(rounds_path)
		assert isinstance(rounds, geopandas.GeoDataFrame), type(rounds)
		max_round_num = rounds['number'].max()
	else:
		max_round_num = None

	subs = load_or_get_submissions(settings.submissions_path, max_round_num)
	rows = []
	for round_num, round_subs in subs.items():
		rows += [{'round': round_num, **sub, 'total_subs': len(round_subs)} for sub in round_subs]
	df = pandas.DataFrame(rows)

	if settings.submissions_path:
		path = format_path(settings.submissions_path, df['round'].max())
		df.to_pickle(path.with_suffix('.pickle'))
	else:
		path = None
	print(df)
	df = (
		df.groupby(['latitude', 'longitude'])
		.apply(_group_rounds, include_groups=False)
		.reset_index()
	)
	print(df)

	geom = geopandas.points_from_xy(df['longitude'], df['latitude'], crs='wgs84')
	gdf = geopandas.GeoDataFrame(df.drop(columns=['latitude', 'longitude']), geometry=geom)
	print(gdf)
	if path:
		gdf.to_file(path.with_suffix('.geojson'))


if __name__ == '__main__':
	main()

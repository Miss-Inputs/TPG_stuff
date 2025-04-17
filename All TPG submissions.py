#!/usr/bin/env python3

import logging
from argparse import ArgumentParser
from pathlib import Path
from typing import Any

import geopandas
import pandas
from pydantic_settings import CliApp, CliSettingsSource

from lib.io_utils import format_path, latest_file_matching_format_pattern
from lib.tastycheese_map import load_or_get_submissions, submission_json_adapter
from settings import Settings

logger = logging.getLogger(__name__)


def _group_rounds(group: pandas.DataFrame):
	first = group.iloc[0].copy()
	rounds = sorted(group['round'].unique())
	first['num_rounds'] = len(rounds)
	first['round'] = ', '.join(str(n) for n in rounds)
	first['first_round'] = rounds[0]
	first['latest_round'] = rounds[-1]
	return first


def _add_additional_data(subs: dict[int, list[dict[str, Any]]], path: Path):
	"""For loading additional data from JSON converted from KML exported from submission tracker"""
	data = submission_json_adapter.validate_json(path.read_bytes())
	for round_num, new_subs in data.items():
		subs.setdefault(round_num, [])
		existing_names = {s['name'] for s in subs[round_num]}
		existing_usernames = {s['username'] for s in subs[round_num]}
		for sub in new_subs:
			del sub['place'] #Will recalculate this when/if recalculating scores to avoid screwiness
			if sub['name'] in existing_names or sub['username'] in existing_usernames:
				# Trust the existing data if it is there
				logger.info(
					'Not overwriting existing submission for %s (%s) in round %s',
					sub['name'],
					sub['username'],
					round_num,
				)
				continue
			subs[round_num].append(sub)


def main() -> None:
	argparser = ArgumentParser()
	argparser.add_argument(
		'--additional-data-paths',
		type=Path,
		nargs='*',
		help='Load more submissions from other JSON files as well',
	)
	settings_source = CliSettingsSource(Settings, root_parser=argparser)
	settings = CliApp.run(Settings, cli_settings_source=settings_source)
	args = argparser.parse_args()
	additional_paths = args.additional_data_paths

	if settings.rounds_path:
		rounds_path = latest_file_matching_format_pattern(settings.rounds_path)
		rounds = geopandas.read_file(rounds_path)
		assert isinstance(rounds, geopandas.GeoDataFrame), type(rounds)
		max_round_num = rounds['number'].max()
	else:
		max_round_num = None

	subs = load_or_get_submissions(settings.submissions_path, max_round_num)
	if additional_paths:
		for additional_path in additional_paths:
			_add_additional_data(subs, additional_path)
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
	print(df[df['round'] == df['round'].max()].index.size, 'submissions for latest round')
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
	logging.basicConfig(level=logging.INFO)
	main()

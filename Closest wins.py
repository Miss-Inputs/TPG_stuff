#!/usr/bin/env python3

import asyncio
import logging
import sys
from argparse import ArgumentParser
from typing import Any

import pandas
from pydantic import TypeAdapter
from travelpygame.tpg_api import get_rounds
from travelpygame.util import (
	format_distance,
	format_xy,
	geod_distance_and_bearing,
	read_geodataframe_async,
	wgs84_geod,
)

from lib.io_utils import latest_file_matching_format_pattern
from settings import Settings

submission_json_adapter = TypeAdapter(dict[int, list[dict[str, Any]]])


async def main() -> None:
	if 'debugpy' in sys.modules:
		discord_id = '152814736742809600'
	else:
		argparser = ArgumentParser(description=__doc__)
		argparser.add_argument('--discord-id')
		# TODO: --username, --name exclusive arguments
		args = argparser.parse_args()
		discord_id = args.discord_id

	settings = Settings()
	if settings.rounds_path:
		rounds_path = await asyncio.to_thread(
			latest_file_matching_format_pattern, settings.rounds_path
		)
		rounds_gdf = await read_geodataframe_async(rounds_path)
		rounds = {
			round_num: (point.y, point.x)  # pyright: ignore[reportAttributeAccessIssue]
			for round_num, point in rounds_gdf.set_index('number').geometry.items()
		}
	else:
		# We don't particularly need country/start time/etc
		rounds = {r.number: (r.latitude, r.longitude) for r in await get_rounds()}

	if not settings.submissions_path:
		raise RuntimeError('need submissions_path, run All TPG submissions.py first')
	# TODO: This should be more generic and should be able to take some other file so it can work with spinoffs and such
	# TODO: Also just grab the submissions if the path is not provided

	path = latest_file_matching_format_pattern(settings.submissions_path)
	submissions = submission_json_adapter.validate_json(await asyncio.to_thread(path.read_bytes))
	if not isinstance(submissions, dict):
		raise TypeError('Whoops, submissions json was not a dict')

	rows = []
	for round_num, subs in submissions.items():
		df = pandas.DataFrame(subs)
		if discord_id not in frozenset(df['discord_id']):
			continue
		n = len(subs)
		target_lat = [rounds[round_num][0]] * n
		target_lng = [rounds[round_num][1]] * n
		df['distance'], df['bearing'] = geod_distance_and_bearing(
			target_lat, target_lng, df['latitude'], df['longitude']
		)
		df = df.sort_values('distance', ascending=True)
		my_subs = df[df['discord_id'] == discord_id].squeeze()
		assert isinstance(my_subs, pandas.Series)
		my_dist = my_subs['distance']
		assert isinstance(my_dist, float)

		closer = df[df['distance'] < my_dist]
		next_highest = closer.iloc[-1]
		diff = my_dist - next_highest['distance']
		forward_lng, forward_lat, _ = wgs84_geod.fwd(
			my_subs['longitude'], my_subs['latitude'], my_subs['bearing'], diff
		)
		rows.append(
			{
				'round': round_num,
				'target': format_xy(rounds[round_num][1], rounds[round_num][0]),
				'distance': my_dist,
				'rival': next_highest['name'],
				'rival_distance': next_highest['distance'],
				'diff': diff,
				'bearing': my_subs['bearing'],
				'forward': format_xy(forward_lng, forward_lat),
			}
		)
	df = pandas.DataFrame(rows)
	df = df.sort_values('diff').set_index('round')
	for col in ('distance', 'rival_distance', 'diff'):
		df[col] = df[col].map(format_distance)
	print(df)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	asyncio.run(main())

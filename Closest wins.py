#!/usr/bin/env python3
"""Shows who was one place ahead of you in previous TPG rounds, and by how much. Attempts to figure out what point would be exactly as close from where you are, but that's a bit vague and doesn't always work."""

import asyncio
import logging
import sys
from argparse import ArgumentParser, BooleanOptionalAction
from pathlib import Path

import numpy
import pandas
from pydantic_settings import CliApp, CliSettingsSource
from travelpygame import get_main_tpg_rounds_with_path, load_rounds_async
from travelpygame.util import (
	format_distance,
	format_xy,
	geod_distance_and_bearing,
	haversine_distance,
	wgs84_geod,
)

from lib.settings import Settings


async def main() -> None:
	if 'debugpy' in sys.modules:
		name = 'Miss Inputs 🐈'
		use_haversine = True
		rounds_path = None
		project_forward = True
		settings = Settings()
	else:
		argparser = ArgumentParser(description=__doc__)
		argparser.add_argument('name', help='Name of user to look at submissions of')
		argparser.add_argument(
			'--rounds-path',
			type=Path,
			help='Path to JSON containing rounds/submissions data, or use main TPG if not specified',
		)
		argparser.add_argument(
			'--haversine',
			action=BooleanOptionalAction,
			help='Use haversine instead of geodetic distance, defaults to true',
			default=True,
		)
		argparser.add_argument(
			'--project-forward',
			action=BooleanOptionalAction,
			help='Find the point forward from your pic towards the target that gets as close as your rival (so you would have gone up one place if you had anywhere more forward than that), defaults to false',
			default=False,
		)
		settings_source = CliSettingsSource(Settings, root_parser=argparser)
		settings = CliApp.run(Settings, cli_settings_source=settings_source)
		args = argparser.parse_args()
		use_haversine = args.haversine
		project_forward = args.project_forward
		name = args.name
		rounds_path = args.rounds_path

		# TODO: Should set this up to parse properly but eh

	rows = []
	rounds = (
		await load_rounds_async(rounds_path)
		if rounds_path
		else await get_main_tpg_rounds_with_path(settings.main_tpg_data_path)
	)
	for r in rounds:
		df = pandas.DataFrame([s.model_dump(exclude_none=True) for s in r.submissions])
		if name not in frozenset(df['name']):
			# We did not submit for this round, and that's okay
			continue
		n = df.index.size
		target_lat = numpy.repeat(r.latitude, n)
		target_lng = numpy.repeat(r.longitude, n)
		if use_haversine:
			df['distance'] = haversine_distance(
				df['latitude'].to_numpy(), df['longitude'].to_numpy(), target_lat, target_lng
			)
			if project_forward:
				# Just to get the bearing. I guess we should have something that calculates bearing from point A to point B while assuming the earth is spherical, for consistency? Meh
				df['geod_distance'], df['bearing'] = geod_distance_and_bearing(
					df['latitude'], df['longitude'], target_lat, target_lng
				)
		else:
			df['distance'], df['bearing'] = geod_distance_and_bearing(
				df['latitude'], df['longitude'], target_lat, target_lng
			)
		df = df.sort_values('distance', ascending=True)
		my_subs = df[df['name'] == name]
		my_sub = my_subs.iloc[0]
		my_dist = my_sub['distance']
		assert isinstance(my_dist, float), f'my_dist is {type(my_dist)}'

		closer = df[df['distance'] < my_dist]
		if closer.empty:
			# We won! Obviously, that's okay
			continue
		next_highest = closer.iloc[-1]
		diff = my_dist - next_highest['distance']
		row = {
			'round': r.name or r.number,
			'season': r.season,
			'target': format_xy(r.longitude, r.latitude),
			'distance': my_dist,
			'rival': next_highest['name'],
			'rival_distance': next_highest['distance'],
			'diff': diff,
		}
		if project_forward:
			forward_lng, forward_lat, _ = wgs84_geod.fwd(
				my_sub['longitude'], my_sub['latitude'], my_sub['bearing'], diff
			)
			row['bearing'] = my_sub['bearing']
			row['forward'] = format_xy(forward_lng, forward_lat)
		rows.append(row)
	df = pandas.DataFrame(rows)
	df = df.sort_values('diff').set_index('round').dropna(axis='columns', how='all')
	for col in ('distance', 'rival_distance', 'diff'):
		df[col] = df[col].map(format_distance)
	print(df)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	asyncio.run(main())

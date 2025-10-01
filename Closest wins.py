#!/usr/bin/env python3
"""Shows who was one place ahead of you in previous TPG rounds, and by how much. Attempts to figure out what point would be exactly as close from where you are, but that's a bit vague and doesn't always work."""

import asyncio
import logging
import sys
from argparse import ArgumentParser, BooleanOptionalAction
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import numpy
import pandas
from pydantic import TypeAdapter
from travelpygame.tpg_api import get_rounds
from travelpygame.util import (
	format_distance,
	format_xy,
	geod_distance_and_bearing,
	haversine_distance,
	parse_submission_kml,
	read_geodataframe_async,
	wgs84_geod,
)

from lib.io_utils import latest_file_matching_format_pattern
from settings import Settings

submission_json_adapter = TypeAdapter(dict[int, list[dict[str, Any]]])


async def get_rounds_and_subs():
	"""This will yield DataFrames for now because I haven't invented a generic way of representing either main TPG submissions (from API) or spinoff submissions (from tracker)"""
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

	for round_num, subs in submissions.items():
		df = pandas.DataFrame(subs)
		lat, lng = rounds[round_num]
		yield round_num, lat, lng, df


def get_rounds_subs_from_tracker(paths: Path | Sequence[Path]):
	tracker = parse_submission_kml(paths)
	for rnd in tracker.rounds:
		lat = rnd.target.y
		lng = rnd.target.x
		submissions = [
			{'name': sub.name, 'latitude': sub.point.y, 'longitude': sub.point.x}
			for sub in rnd.submissions
		]
		yield rnd.name, lat, lng, pandas.DataFrame(submissions)


async def main() -> None:
	if 'debugpy' in sys.modules:
		discord_id = None
		name = 'Miss Inputs 🐈'
		use_haversine = True
		tracker_path = None
		project_forward = True
	else:
		argparser = ArgumentParser(description=__doc__)
		user_args = argparser.add_mutually_exclusive_group(required=True)
		user_args.add_argument(
			'--discord_id',
			help='Discord ID if using main TPG, otherwise this argument is not allowed',
		)
		user_args.add_argument('--name')
		# TODO: We should have --username too but I'd have to look up that with get_players() and I can't be bothered
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
		argparser.add_argument(
			'--submission-tracker',
			type=Path,
			nargs='*',
			help='Path to submission tracker KMZ/KML file(s) to use that instead',
		)
		args = argparser.parse_args()
		discord_id = args.discord_id
		use_haversine = args.haversine
		project_forward = args.project_forward
		name = args.name
		tracker_path = args.submission_tracker

	rows = []
	rounds_and_subs = (
		get_rounds_subs_from_tracker(tracker_path)
		if tracker_path
		else [a async for a in get_rounds_and_subs()]
	)
	for round_num, lat, lng, df in rounds_and_subs:
		if (discord_id and discord_id not in frozenset(df['discord_id'])) or (
			name and name not in frozenset(df['name'])
		):
			# We did not submit for this round, and that's okay
			continue
		n = df.index.size
		target_lat = numpy.repeat(lat, n)
		target_lng = numpy.repeat(lng, n)
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
		my_subs = df[df['discord_id'] == discord_id] if discord_id else df[df['name'] == name]
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
			'round': round_num,
			'target': format_xy(lng, lat),
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
	df = df.sort_values('diff').set_index('round')
	for col in ('distance', 'rival_distance', 'diff'):
		df[col] = df[col].map(format_distance)
	print(df)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	asyncio.run(main())

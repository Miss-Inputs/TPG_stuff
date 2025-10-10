#!/usr/bin/env python3
"""Shows who was one place ahead of you in previous TPG rounds, and by how much. Attempts to figure out what point would be exactly as close from where you are, but that's a bit vague and doesn't always work."""

import asyncio
import logging
import sys
from argparse import ArgumentParser, BooleanOptionalAction
from pathlib import Path

import pandas
from geopandas import GeoDataFrame
from shapely import Point
from travelpygame import Round, get_main_tpg_rounds_with_path, load_rounds, output_geodataframe
from travelpygame.submission_comparison import find_next_highest_placing
from travelpygame.util import (
	format_dataframe,
	format_xy,
	geod_distance_and_bearing,
	wgs84_geod,
)

from lib.settings import Settings


def get_closest_placings(
	rounds: list[Round], name: str, *, use_haversine: bool = True, project_forward: bool = True
) -> pandas.DataFrame:
	rows = []
	for r in rounds:
		try:
			player_submission = next(sub for sub in r.submissions if sub.name == name)
		except StopIteration:
			# We did not submit for this round, and that's okay
			continue
		rival_diff = find_next_highest_placing(r, player_submission, use_haversine=use_haversine)
		if rival_diff is None:
			# We won! That's certainly okay
			continue

		row = {
			'round': r.name or r.number,
			'season': r.season,
			'target': format_xy(r.longitude, r.latitude),
			'distance': rival_diff.player_distance,
			'rival': rival_diff.rival,
			'rival_distance': rival_diff.rival_distance,
			'diff': rival_diff.distance_diff,
		}
		if project_forward:
			bearing = geod_distance_and_bearing(
				r.latitude, r.longitude, player_submission.latitude, player_submission.longitude
			)[1]
			forward_lng, forward_lat, _ = wgs84_geod.fwd(
				player_submission.longitude,
				player_submission.latitude,
				bearing,
				rival_diff.distance_diff,
			)
			row['bearing'] = bearing
			row['forward'] = Point(forward_lng, forward_lat)
		rows.append(row)
	df = pandas.DataFrame(rows)
	return df.sort_values('diff').set_index('round').dropna(axis='columns', how='all')


def main() -> None:
	if 'debugpy' in sys.modules:
		name = 'Miss Inputs 🐈'
		use_haversine = True
		rounds_path = None
		project_forward = True
		output_path = None
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
			help='Find the point forward from your pic towards the target that gets as close as your rival (so you would have gone up one place if you had anywhere more forward than that, although this does not check that you never submitted a pic later on that would have done that), defaults to false',
			default=False,
		)
		argparser.add_argument(
			'--output-path',
			type=Path,
			help='Path to save results as CSV. If using --project-forward, this can also be GeoJSON/gpkg/etc',
		)
		args = argparser.parse_args()
		use_haversine = args.haversine
		project_forward = args.project_forward
		name = args.name
		rounds_path = args.rounds_path
		output_path = args.output_path

	settings = Settings()
	rounds = (
		load_rounds(rounds_path)
		if rounds_path
		else asyncio.run(get_main_tpg_rounds_with_path(settings.main_tpg_data_path))
	)
	df = get_closest_placings(
		rounds, name, use_haversine=use_haversine, project_forward=project_forward
	)
	if output_path:
		if project_forward:
			output_geodataframe(
				GeoDataFrame(df, geometry='forward', crs='wgs84'),
				output_path,
				'forward_lat',
				'forward_lng',
				index=True,
			)
		else:
			df.to_csv(output_path)

	print(
		format_dataframe(
			df, ('distance', 'rival_distance', 'diff'), 'forward' if project_forward else None
		)
	)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	main()

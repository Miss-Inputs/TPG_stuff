#!/usr/bin/env python3
"""Finds where players have a submission that is close to that of another player and makes a network (graph, whatever you call it) by pretending that both players have met each other there.
WIP
"""

import asyncio
import itertools
import logging
from argparse import ArgumentParser
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import numpy
from geopandas import GeoDataFrame
from shapely import Point
from tqdm.auto import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from travelpygame.util import format_dataframe
from travelpygame.util.distance import (
	EQ_LONGITUDE_DEG_M,
	DistanceMethod,
	FloatNDArray,
	get_distances,
)
from travelpygame.util.io_utils import output_geodataframe

from lib.io_utils import load_or_fetch_submission_summary
from lib.settings import Settings

if TYPE_CHECKING:
	from travelpygame.submission_data import SubmissionSummary
	from travelpygame.tpg_data import PlayerName


def possibly_close_pics(lat1: float, lng1: float, pics_2: FloatNDArray) -> FloatNDArray:
	"""Prunes pics that cannot possibly be close to lat1/lng1 by checking that the coordinates are 0 or 1 graticules away."""
	lat2 = pics_2[:, 1]
	lng2 = pics_2[:, 0]
	# hrm I can't help but think there was a less awkward way to write this
	maybe_close_lat = abs(lat2 - lat1) <= 1
	if lat1 >= 179:
		maybe_close_lat |= lat2 <= -179
	elif lat1 <= -179:
		maybe_close_lat |= lat2 >= 179
	maybe_close_lng = abs(lng2 - lng1) <= 1
	is_possibly_close = maybe_close_lat | maybe_close_lng
	return pics_2[is_possibly_close]


def get_by_player(
	subs: 'SubmissionSummary', pic_count_threshold: int, min_date: datetime | None
) -> dict['PlayerName', FloatNDArray]:
	by_player = {}
	for player, player_subs in subs.per_player.items():
		if pic_count_threshold and len(player_subs) < pic_count_threshold:
			continue
		if min_date:
			dates = [
				sub.latest_known.replace(tzinfo=None) for sub in player_subs if sub.latest_known
			]
			if not dates or max(dates) < min_date:
				continue
		by_player[player] = numpy.asarray([(sub.point.x, sub.point.y) for sub in player_subs])
		# well, there's definitely a better way to write that one
	print(
		f'{len(by_player)} players (before threshold: {len({s.player for s in subs.submissions})})'
	)
	print(f'Total {sum(s.size for s in by_player.values())} submissions')
	return by_player


def get_shared_pics(
	subs: 'SubmissionSummary',
	pic_count_threshold: int,
	min_date: datetime | None,
	threshold: float,
	distance_method: DistanceMethod,
	*,
	allow_multiple: bool,
) -> GeoDataFrame:
	by_player = get_by_player(subs, pic_count_threshold, min_date)
	# Getting a list of every combination of subs.submissions where player names are different doesn't work and will just explode your computer with memory usage, so stop trying to think of doing that
	player_names = set(by_player.keys())
	combinations = list(itertools.combinations(player_names, 2))

	rows = []
	with tqdm(combinations, unit='pair of players') as t:
		for player_1, player_2 in t:
			pics_1 = by_player[player_1]
			pics_2 = by_player[player_2]

			for pic_1 in pics_1:
				lng1, lat1 = pic_1
				maybe_pics_2 = (
					possibly_close_pics(lat1, lng1, pics_2)
					if threshold < EQ_LONGITUDE_DEG_M
					else pics_2
				)
				if maybe_pics_2.size == 0:
					continue
				distances = get_distances((lat1, lng1), maybe_pics_2, distance_method)
				# TODO: Properly handle the allow_multiple case by adding everything with distance <= threshold
				index = distances.argmin()
				dist = distances[index]
				if dist <= threshold:
					lng2, lat2 = maybe_pics_2[index]
					rows.append(
						{
							'player_1': player_1,
							'point_1': Point(lng1, lat1),
							'player_2': player_2,
							'point_2': Point(lng2, lat2),
							'distance': dist,
						}
					)
					if not allow_multiple:
						break

	gdf = GeoDataFrame(rows, geometry='point_1', crs='wgs84')
	found_players = set(gdf['player_1']) | set(gdf['player_2'])
	print('Isolated ☹:', player_names - found_players)

	return gdf


async def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument(
		'--distance-method',
		choices=DistanceMethod,
		default='geodetic',
		help='Can be geodetic (default, most accurate), haversine (faster, assumes Earth is a sphere), euclidean (fastest, assumes Earth is flat), defaults to geodetic',
	)
	argparser.add_argument(
		'--threshold',
		type=float,
		default=100,
		help='Distance in metres, where two pics within this threshold are considered close to each other. Defaults to 100m',
	)
	player_args = argparser.add_argument_group('Player args', 'Options to filter players')
	player_args.add_argument(
		'--pic-count-threshold',
		type=int,
		default=2,
		help='Only include pics from players who have submitted at least this amount of unique pics. Defaults to 2, setting it to 0 or lower is effectively disabling it',
	)
	player_args.add_argument(
		'--date-threshold',
		'--min-date',
		type=datetime.fromisoformat,
		help='Only include players who have known submissions at least after this date (YYYY-MM-DD format only, no being weird allowed). This can help speed things up',
	)
	output_args = argparser.add_argument_group('Output args')
	output_args.add_argument(
		'--output-path',
		type=Path,
		help='Where to save list of shared submissions, can be .csv or .geojson/.gpkg/etc',
	)
	args = argparser.parse_args()
	output_path: Path | None = args.output_path
	distance_method = DistanceMethod(args.distance_method)

	settings = Settings()
	# TODO: Have this be an argument instead
	subs_path = settings.submission_summary_path
	subs = await load_or_fetch_submission_summary(
		subs_path, settings.tpg_export_path, aliases_path=settings.aliases_path
	)

	# TODO: Argument for allow_multiple once I implement that properly (zzzz)
	gdf = get_shared_pics(
		subs,
		args.pic_count_threshold,
		args.date_threshold,
		args.threshold,
		distance_method,
		allow_multiple=False,
	)
	# I don't think we really need to hang on to the full list of players including those not in the network
	gdf = gdf.sort_values(['player_1', 'player_2'])
	print(format_dataframe(gdf, distance_cols={'distance'}, point_cols={'point_1', 'point_2'}))
	if output_path:
		# Maybe put each direction on one line? Or something like that, I dunno, I'll figure out what I want to do output-wise later
		await asyncio.to_thread(output_geodataframe, gdf, output_path, index=False)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	with logging_redirect_tqdm():
		asyncio.run(main())

#!/usr/bin/env python3

import asyncio
import itertools
import logging

import numpy
import pandas
from aiohttp import ClientSession
from tqdm.auto import tqdm
from tqdm.contrib.concurrent import process_map
from travelpygame.scoring import tpg_score
from travelpygame.tpg_api import get_rounds
from travelpygame.util import haversine_distance, read_dataframe_pickle

from lib.io_utils import format_path, latest_file_matching_format_pattern
from settings import Settings


def get_best_pic(pics: pandas.DataFrame, target_lat: float, target_lng: float):
	size = pics.index.size
	distances = haversine_distance(
		pics['latitude'].to_numpy(),
		pics['longitude'].to_numpy(),
		numpy.repeat(target_lat, size),
		numpy.repeat(target_lng, size),
	)
	pics['distance'] = distances
	pics = pics.sort_values('distance', ascending=True)
	return pics.iloc[0]


def _get_new_round_rows(
	round_num: int,
	cc: str | None,
	target_lat: float,
	target_lng: float,
	pics_per_user: dict[str, pandas.DataFrame],
):
	rows = []
	with tqdm(
		pics_per_user.items(),
		f'Finding best pic for all users for round {round_num}',
		leave=False,
		disable=True,
	) as t:
		for username, pics in t:
			t.set_postfix(username=username)
			best = get_best_pic(pics, target_lat, target_lng)
			rows.append(
				{
					'round': round_num,
					'country': cc,
					'target_lat': target_lat,
					'target_lng': target_lng,
					'username': username,
					'latitude': best['latitude'],
					'longitude': best['longitude'],
					'distance': best['distance'],
					# These three will be filled in later
					'score': None,
					'place': None,
					'total_subs': None,
				}
			)
	return rows


async def get_theoretical_best_submissions(submissions: pandas.DataFrame, session: ClientSession):
	rounds = {r.number: (r.country, r.latitude, r.longitude) for r in await get_rounds(session)}
	submissions['country'], submissions['target_lat'], submissions['target_lng'] = zip(
		*submissions['round'].apply(lambda round_num: rounds[int(round_num)]), strict=True
	)
	pics_per_user = {
		username: group[['latitude', 'longitude']].drop_duplicates()
		for username, group in submissions.groupby('name', sort=False)
	}

	rows = list(
		itertools.chain.from_iterable(
			process_map(
				_get_new_round_rows,
				*zip(
					*(
						(round_num, cc, target_lat, target_lng, pics_per_user)
						for round_num, (cc, target_lat, target_lng) in rounds.items()
					),
					strict=True,
				),
				desc='Finding best pics for all rounds',
				total=len(rounds),
			)
		)
	)

	return pandas.DataFrame(rows)


async def main() -> None:
	settings = Settings()
	if not settings.submissions_path:
		raise RuntimeError('Need submissions_path, run All TPG submissions.py first')

	path = latest_file_matching_format_pattern(settings.submissions_path.with_suffix('.pickle'))
	submissions = read_dataframe_pickle(path, desc='Loading submissions', leave=False)

	async with ClientSession() as sesh:
		# TODO: This should be refactored to take arguments, so you have any arbitrary set of rounds instead
		new_subs = await get_theoretical_best_submissions(submissions, sesh)

	for _, round_group in new_subs.groupby('round', as_index=False, sort=False, group_keys=False):
		round_scores = tpg_score(round_group['distance'] / 1000)
		round_results = pandas.DataFrame(
			{
				'score': round_scores,
				'place': round_group['distance'].rank(method='max', ascending=True),
			}
		)
		round_results['total_subs'] = round_group.index.size
		new_subs.update(round_results)
	new_subs = new_subs.astype({'place': int, 'total_subs': int})
	orig_placement = {(row.round, row.name): row.place for row in submissions.itertuples()}
	new_subs['orig_placement'] = new_subs.apply(
		lambda row: orig_placement.get((row['round'], row['username'])),  # type: ignore[overload] #what? Are you stupid
		axis='columns',
	)
	if settings.theoretical_best_path:
		output_path = format_path(settings.theoretical_best_path, new_subs['round'].max())
		new_subs.to_pickle(output_path)
		new_subs.to_csv(output_path.with_suffix('.csv'), index=False)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	asyncio.run(main())

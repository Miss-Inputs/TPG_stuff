#!/usr/bin/env python3

import itertools

import numpy
import pandas
from tqdm.auto import tqdm
from tqdm.contrib.concurrent import process_map

from lib.geo_utils import haversine_distance
from lib.io_utils import latest_file_matching_format_pattern, read_dataframe_pickle
from lib.tastycheese_map import get_tpg_rounds
from lib.tpg_utils import tpg_score
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


def main() -> None:
	settings = Settings()
	if not settings.submissions_path:
		raise RuntimeError('Need submissions_path, run All TPG submissions.py first')

	path = latest_file_matching_format_pattern(settings.submissions_path.with_suffix('.pickle'))
	df = read_dataframe_pickle(path, desc='Loading submissions', leave=False)
	df['username'] = df['username'].combine_first(df['name'])
	print(df)

	rounds = {r.number: (r.country, r.latitude, r.longitude) for r in get_tpg_rounds()}
	df['country'], df['target_lat'], df['target_lng'] = zip(
		*df['round'].apply(lambda round_num: rounds[int(round_num)]), strict=True
	)
	pics_per_user = {
		username: group[['latitude', 'longitude']].drop_duplicates()
		for username, group in df.groupby('username', sort=False)
	}
	orig_placement = {(row.round, row.username): row.place for row in df.itertuples()}

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

	df = pandas.DataFrame(rows)
	for _, round_group in df.groupby('round', as_index=False, sort=False, group_keys=False):
		round_scores = tpg_score(round_group['distance'] / 1000)
		round_results = pandas.DataFrame(
			{
				'score': round_scores,
				'place': round_group['distance'].rank(method='max', ascending=True),
			}
		)
		round_results['total_subs'] = round_group.index.size
		df.update(round_results)
	df = df.astype({'place': int, 'total_subs': int})
	df['orig_placement'] = df.apply(
		lambda row: orig_placement.get((row['round'], row['username'])),  # type: ignore[overload] #what? Are you stupid
		axis='columns',
	)
	print(df)
	print(df[df['round'] == 177].sort_values('distance'))
	if settings.theoretical_best_path:
		output_path = settings.theoretical_best_path.with_stem(
			settings.theoretical_best_path.stem.format(max(rounds))
		)
		df.to_pickle(output_path)
		df.to_csv(output_path.with_suffix('.csv'), index=False)


if __name__ == '__main__':
	main()

#!/usr/bin/env python3


from typing import TYPE_CHECKING

from lib.geo_utils import geod_distance_and_bearing, haversine_distance
from lib.io_utils import format_path, latest_file_matching_format_pattern, read_dataframe_pickle
from lib.tastycheese_map import get_tpg_rounds
from lib.tpg_utils import tpg_score
from settings import Settings

if TYPE_CHECKING:
	import pandas


def add_scores(submissions: 'pandas.DataFrame'):
	rounds = {r.number: (r.country, r.latitude, r.longitude) for r in get_tpg_rounds()}
	submissions['country'], submissions['target_lat'], submissions['target_lng'] = zip(
		*submissions['round'].apply(lambda round_num: rounds[int(round_num)]), strict=True
	)
	submissions['geod_distance'], submissions['heading'] = geod_distance_and_bearing(
		submissions['latitude'].to_list(),
		submissions['longitude'].to_list(),
		submissions['target_lat'].to_list(),
		submissions['target_lng'].to_list(),
	)
	submissions['distance'] = haversine_distance(
		submissions['latitude'].to_numpy(),
		submissions['longitude'].to_numpy(),
		submissions['target_lat'].to_numpy(),
		submissions['target_lng'].to_numpy(),
	)

	# should this use apply/agg? Ah well
	submissions['score'] = [None] * submissions.index.size
	for _, round_group in submissions.groupby(
		'round', as_index=False, sort=False, group_keys=False
	):
		round_scores = tpg_score(round_group['distance'] / 1000)
		round_scores.name = 'score'
		submissions.update(round_scores)


def main() -> None:
	settings = Settings()
	if not settings.submissions_path:
		raise RuntimeError('Need submissions_path, run All TPG submissions.py first')

	path = latest_file_matching_format_pattern(settings.submissions_path.with_suffix('.pickle'))
	submissions = read_dataframe_pickle(path, desc='Loading submissions', leave=False)

	add_scores(submissions)
	if settings.submissions_with_scores_path:
		output_path = format_path(settings.submissions_with_scores_path, submissions['round'].max())
		submissions.to_pickle(output_path)


if __name__ == '__main__':
	main()

#!/usr/bin/env python3

from lib.geo_utils import geod_distance_and_bearing, haversine_distance
from lib.io_utils import latest_file_matching_format_pattern, read_dataframe_pickle
from lib.tastycheese_map import get_tpg_rounds
from lib.tpg_utils import tpg_score
from settings import Settings


def main() -> None:
	settings = Settings()
	if not settings.submissions_path:
		raise RuntimeError('Need submissions_path, run All TPG submissions.py first')

	path = latest_file_matching_format_pattern(settings.submissions_path.with_suffix('.pickle'))
	df = read_dataframe_pickle(path, desc='Loading submissions', leave=False)
	print(df)

	rounds = {r.number: (r.country, r.latitude, r.longitude) for r in get_tpg_rounds()}
	df['country'], df['target_lat'], df['target_lng'] = zip(
		*df['round'].apply(lambda round_num: rounds[int(round_num)]), strict=True
	)
	df['geod_distance'], df['heading'] = geod_distance_and_bearing(
		df['latitude'].to_list(),
		df['longitude'].to_list(),
		df['target_lat'].to_list(),
		df['target_lng'].to_list(),
	)
	df['distance'] = haversine_distance(
		df['latitude'].to_numpy(),
		df['longitude'].to_numpy(),
		df['target_lat'].to_numpy(),
		df['target_lng'].to_numpy(),
	)

	# should this use apply/agg? Ah well
	df['score'] = [None] * df.index.size
	for _, round_group in df.groupby('round', as_index=False, sort=False, group_keys=False):
		round_scores = tpg_score(round_group['distance'] / 1000)
		round_scores.name = 'score'
		df.update(round_scores)
	print(df)
	if settings.submissions_with_scores_path:
		output_path = settings.submissions_with_scores_path.with_stem(
			settings.submissions_with_scores_path.stem.format(df['round'].max())
		)
		df.to_pickle(output_path)


if __name__ == '__main__':
	main()

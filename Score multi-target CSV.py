#!/usr/bin/env python3
"""!!! This is extremely quick and dirty and will potentially be replaced by a better one here"""

from argparse import ArgumentParser
from pathlib import Path

import pandas
from travelpygame.scoring import main_tpg_scoring, score_distances
from travelpygame.util import format_dataframe
from travelpygame.util.distance import geod_distances, haversine_distance


def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument(
		'path',
		type=Path,
		help='Path to CSV, must have columns: lat, lng, target_lat, target_lng, optionally is_5k',
	)
	argparser.add_argument('--use-haversine', action='store_true', help='Default false')
	args = argparser.parse_args()

	path: Path = args.path
	df = pandas.read_csv(path)
	dist_func = haversine_distance if args.use_haversine else geod_distances
	df['distance'] = dist_func(
		df['lat'].to_numpy(),
		df['lng'].to_numpy(),
		df['target_lat'].to_numpy(),
		df['target_lng'].to_numpy(),
	)
	if 'is_5k' not in df.columns:
		df['is_5k'] = False
	df['score'] = score_distances(df['distance'], df['is_5k'], None, main_tpg_scoring)
	df = df.sort_values('score', ascending=False)
	print(format_dataframe(df, 'distance'))


if __name__ == '__main__':
	main()

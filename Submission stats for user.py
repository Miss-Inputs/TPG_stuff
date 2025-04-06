#!/usr/bin/env python3

import sys
from argparse import ArgumentParser
from typing import Any

import requests
from pydantic_settings import CliApp, CliSettingsSource

from lib.io_utils import latest_file_matching_format_pattern, read_dataframe_pickle
from lib.tpg_utils import print_round
from settings import Settings


def main() -> None:
	if 'debugpy' in sys.modules:
		username = 'miss_inputs'
		settings = Settings()
	else:
		argparser = ArgumentParser()
		argparser.add_argument('username')
		settings_source = CliSettingsSource(Settings, root_parser=argparser)
		settings = CliApp.run(Settings, cli_settings_source=settings_source)
		args = argparser.parse_args()
		username = args.username

	if not settings.submissions_with_scores_path:
		raise RuntimeError('needs submissions_with_scores_path, run TPG submissions with scores.py first')
	path = latest_file_matching_format_pattern(settings.submissions_with_scores_path)
	df = read_dataframe_pickle(path, desc='Loading submissions', leave=False)
	print(df)
	user_scores = df[df['username'] == username].sort_values('score', ascending=False)

	with requests.Session() as sesh:
		row: Any  # bleh, pyright does not understand itertuples
		for n, row in enumerate(user_scores.itertuples(), 1):
			print_round(n, row, sesh)

		if settings.theoretical_best_path:
			print('Theoretical retroactive score using best pics:')
			best = read_dataframe_pickle(
				latest_file_matching_format_pattern(settings.theoretical_best_path)
			)
			user_best = best[best['username'] == username]
			for n, row in enumerate(
				user_best.sort_values('score', ascending=False).head(10).itertuples(), 1
			):
				print_round(n, row, sesh)
			for n, row in enumerate(
				user_best.sort_values('score', ascending=True).head(10).itertuples(), 1
			):
				print_round(n, row, sesh)


if __name__ == '__main__':
	main()

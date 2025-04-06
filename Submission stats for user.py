#!/usr/bin/env python3

import sys
from argparse import ArgumentParser
from typing import TYPE_CHECKING, Any

import requests
from pydantic_settings import CliApp, CliSettingsSource

from lib.io_utils import latest_file_matching_format_pattern, read_dataframe_pickle
from lib.tpg_utils import print_round
from settings import Settings

if TYPE_CHECKING:
	import pandas


def print_user_rounds_by_score(user_scores: 'pandas.DataFrame', sesh: requests.Session | None = None):
	row: Any  # bleh, pyright does not understand itertuples
	for n, row in enumerate(user_scores.itertuples(), 1):
		print_round(n, row, sesh)


def print_theoretical_best_user_rounds(
	user_best: 'pandas.DataFrame', sesh: requests.Session | None = None, amount: int = 10
):
	for n, row in enumerate(
		user_best.sort_values('score', ascending=False).head(amount).itertuples(), 1
	):
		print_round(n, row, sesh)
	for n, row in enumerate(
		user_best.sort_values('score', ascending=True).head(amount).itertuples(), 1
	):
		print_round(n, row, sesh)


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
		raise RuntimeError(
			'needs submissions_with_scores_path, run TPG submissions with scores.py first'
		)
	path = latest_file_matching_format_pattern(settings.submissions_with_scores_path)
	submissions_with_scores = read_dataframe_pickle(
		path, desc='Loading submissions with scores', leave=False
	)
	user_scores = submissions_with_scores[
		submissions_with_scores['username'] == username
	].sort_values('score', ascending=False)

	with requests.Session() as sesh:
		print_user_rounds_by_score(user_scores, sesh)

		if settings.theoretical_best_path:
			print('Theoretical retroactive score using best pics:')
			theoretical_best_path = latest_file_matching_format_pattern(
				settings.theoretical_best_path
			)
			best = read_dataframe_pickle(theoretical_best_path)
			user_best = best[best['username'] == username]
			print_theoretical_best_user_rounds(user_best, sesh)


if __name__ == '__main__':
	main()

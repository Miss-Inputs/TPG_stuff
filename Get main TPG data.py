#!/usr/bin/env python3
"""Fetches current main TPG data if it is not there"""

import asyncio
from argparse import ArgumentParser, BooleanOptionalAction
from pathlib import Path

from tqdm.auto import tqdm
from travelpygame import (
	get_main_tpg_rounds,
	get_main_tpg_rounds_with_path,
	main_tpg_scoring,
	rounds_to_json,
	score_round,
)

from lib.settings import Settings


async def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument(
		'data_path',
		nargs='?',
		type=Path,
		help='Path to save data to, defaults to MAIN_TPG_DATA_PATH environment variable if set. If that is not set and this argument is not given, this is not very useful.',
	)
	argparser.add_argument(
		'--subs-per-player-path',
		type=Path,
		help='Path to save per-player submissions to, defaults to SUBS_PER_PLAYER_PATH environment variable if set. If that is not set and this argument is not given, this is not very useful.',
	)
	argparser.add_argument(
		'--refresh',
		action=BooleanOptionalAction,
		default=True,
		help='Fetch data again instead of loading from file, defaults to true',
	)
	argparser.add_argument(
		'--score',
		action=BooleanOptionalAction,
		default=True,
		help='Calculate scores, defaults to true',
	)

	args = argparser.parse_args()
	settings = Settings()

	data_path: Path | None = args.data_path or settings.main_tpg_data_path

	rounds = (
		await get_main_tpg_rounds()
		if args.refresh
		else await get_main_tpg_rounds_with_path(data_path)
	)
	if args.score:
		rounds = [
			score_round(r, main_tpg_scoring, fivek_threshold=None)
			for r in tqdm(rounds, 'Calculating scores', unit='round')
		]
		if data_path:
			j = rounds_to_json(rounds)
			await asyncio.to_thread(data_path.write_text, j, encoding='utf-8')
	print(
		f'{len(rounds)} rounds, {len({r.season for r in rounds if r.season is not None})} seasons'
	)


if __name__ == '__main__':
	asyncio.run(main())

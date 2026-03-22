#!/usr/bin/env python3
"""Fetches current main TPG data if it is not there. This is also not used as much anymore."""

import asyncio
from argparse import ArgumentParser, BooleanOptionalAction
from operator import attrgetter
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
		'--refresh',
		action=BooleanOptionalAction,
		default=True,
		help='Fetch data again instead of loading from file, defaults to true',
	)
	argparser.add_argument(
		'--game-id', type=int, default=1, help='ID of game to get, defaults to 1 (main world TPG)'
	)
	argparser.add_argument(
		'--score',
		action=BooleanOptionalAction,
		default=True,
		help='Calculate scores, defaults to true',
	)

	args = argparser.parse_args()

	data_path: Path | None = args.data_path
	game_id: int = args.game_id
	if not data_path and game_id == 1:
		settings = Settings()
		data_path = settings.main_tpg_data_path

	rounds = (
		await get_main_tpg_rounds(game_id)
		if args.refresh
		else await get_main_tpg_rounds_with_path(data_path, game_id)
	)
	rounds.sort(key=attrgetter('number'))
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

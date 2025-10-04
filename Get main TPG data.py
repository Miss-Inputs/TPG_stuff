#!/usr/bin/env python3
"""Fetches current main TPG data if"""

import asyncio
from argparse import ArgumentParser, BooleanOptionalAction

from pydantic_settings import CliApp, CliSettingsSource
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
	#TODO: We really just want settings.main_tpg_data_path as an argument with defaults loaded from environment variables
	settings_source = CliSettingsSource(Settings, root_parser=argparser)
	settings = CliApp.run(Settings, cli_settings_source=settings_source)
	args = argparser.parse_args()

	rounds = (
		await get_main_tpg_rounds()
		if args.refresh
		else await get_main_tpg_rounds_with_path(settings.main_tpg_data_path)
	)
	if args.score:
		rounds = [score_round(r, main_tpg_scoring, fivek_threshold=None) for r in rounds]
		if settings.main_tpg_data_path:
			j = rounds_to_json(rounds)
			await asyncio.to_thread(settings.main_tpg_data_path.write_text, j, encoding='utf-8')


if __name__ == '__main__':
	asyncio.run(main())

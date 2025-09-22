#!/usr/bin/env python3

import asyncio
import logging
from pathlib import Path
from typing import Any

import geopandas
import pandas
import pydantic_core
from aiohttp import ClientSession
from pydantic import TypeAdapter

from lib.io_utils import format_path, latest_file_matching_format_pattern
from lib.tpg_api import get_all_submissions, get_players, get_rounds
from settings import Settings

logger = logging.getLogger(__name__)


def _group_rounds(group: pandas.DataFrame):
	first = group.iloc[0].copy()
	rounds = sorted(group['round'].unique())
	first['num_rounds'] = len(rounds)
	first['round'] = ', '.join(str(n) for n in rounds)
	first['first_round'] = rounds[0]
	first['latest_round'] = rounds[-1]
	return first


async def get_submissions(
	max_round_num: int, session: ClientSession, path: Path | None = None
) -> dict[int, list[dict[str, Any]]]:
	"""Gets all submissions and optionally saves it to a file as JSON.

	Arguments:
		path: Path which doesn't need to exist yet. {} will be replaced with the number of the latest round.
		max_round_num: Latest round number if known, this will also display the progress bar better.

	Returns:
		{round number: list of submissions converted to dict}."""
	all_submissions = await get_all_submissions(max_round_num, session)
	players = await get_players(session)
	names = {p.discord_id: p.name for p in players}
	data = {
		round_num: [s.model_dump() | {'name': names.get(s.discord_id, s.discord_id)} for s in subs]
		for round_num, subs in all_submissions.items()
	}

	if path:
		j = pydantic_core.to_json(data)
		path = format_path(path, max(all_submissions.keys()))
		path.write_bytes(j)
	return data


submission_json_adapter = TypeAdapter(dict[int, list[dict[str, Any]]])


async def load_or_get_submissions(
	max_round_num: int, session: ClientSession, path: Path | None = None
) -> dict[int, list[dict[str, Any]]]:
	"""If path is provided, loads all submissions from that file if it exists (and if max_round_num is known, that the file is up to the most recent round), or gets them and saves them as JSON if not. Recommended to avoid spamming the endpoint every time.
	If path is None, behaves the same as get_submissions.

	Arguments:
		path: Path to a JSON. If it doesn't exist, {} will be replaced with the number of the latest round.
		max_round_num: Latest round number if known, this will also display the progress bar better.

	Returns:
		{round number: list of submissions converted to dict}.
	"""
	if not path:
		return await get_submissions(max_round_num, session, path)
	try:
		latest_path = latest_file_matching_format_pattern(path)
	except ValueError:
		# file not there
		return await get_submissions(max_round_num, session, path)

	# Ensure we create a new file if we have more rounds
	latest_path_stem = path.stem.format(max_round_num)
	if latest_path.stem != latest_path_stem:
		latest_path = latest_path.with_stem(latest_path_stem)

	try:
		contents = latest_path.read_bytes()
		subs = submission_json_adapter.validate_json(contents)
	except FileNotFoundError:
		subs = await get_submissions(max_round_num, session, path)
	return subs


async def main() -> None:
	settings = Settings()

	async with ClientSession() as sesh:
		if settings.rounds_path:
			rounds_path = latest_file_matching_format_pattern(settings.rounds_path)
			rounds = geopandas.read_file(rounds_path)
			assert isinstance(rounds, geopandas.GeoDataFrame), type(rounds)
			max_round_num = rounds['number'].max()
		else:
			max_round_num = max(r.number for r in await get_rounds(sesh))

		subs = await load_or_get_submissions(max_round_num, sesh, settings.submissions_path)

	rows = []
	for round_subs in subs.values():
		rows += [{**sub, 'total_subs': len(round_subs)} for sub in round_subs]
	df = pandas.DataFrame(rows)

	if settings.submissions_path:
		path = format_path(settings.submissions_path, df['round'].max())
		df.to_pickle(path.with_suffix('.pickle'))
	else:
		path = None
	print(df)
	print(df[df['round'] == df['round'].max()].index.size, 'submissions for latest round')
	df = (
		df.groupby(['latitude', 'longitude'])
		.apply(_group_rounds, include_groups=False) # pyright: ignore[reportCallIssue]
		.reset_index()
	)
	print(df)

	geom = geopandas.points_from_xy(df['longitude'], df['latitude'], crs='wgs84')
	gdf = geopandas.GeoDataFrame(df.drop(columns=['latitude', 'longitude']), geometry=geom)
	print(gdf)
	if path:
		gdf.to_file(path.with_suffix('.geojson'))


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	asyncio.run(main(), debug=False)

#!/usr/bin/env python3

import asyncio

from aiohttp import ClientSession
from travelpygame.util import read_dataframe_pickle_async

from lib.format_utils import describe_coord
from settings import Settings


async def main() -> None:
	settings = Settings()
	if not settings.tpg_wrapped_output_path:
		raise RuntimeError('Nope!')
	all_rows_path = settings.tpg_wrapped_output_path / 'all_rows.pickle'
	all_rows = await read_dataframe_pickle_async(all_rows_path)
	print(all_rows)

	# I didn't really check if any display names are actually non-unique or otherwise not 1:1 with username, oh well
	unique_pics_count = all_rows[all_rows['first_use']].groupby(['name', 'username']).size()
	print(
		'Players with 25 unique photos:',
		[n[0] for n in unique_pics_count[unique_pics_count == 25].index],
	)

	# Closest player to exactly 50% average placement
	player_average_placement = all_rows.groupby(['name', 'username'])['place_percent'].mean()
	best_average, best_average_player = player_average_placement.agg(['min', 'idxmin'])
	print(
		f'Highest average: {best_average_player} (top {best_average:%} of all players)'
	)  # We aren't looking for that though, just printing that because I can
	distance_to_50 = (player_average_placement - 0.5).abs()
	most_average = distance_to_50.idxmin()
	print(
		f'Most average player: {most_average} (top {player_average_placement.loc[most_average]:%} of all players)'
	)

	# Player with the highest count of submitted countries (hmm, be careful with unknowns)
	player_country_count = all_rows.groupby(['name', 'username'])['cc'].apply(
		lambda group: group.nunique(dropna=True)
	)
	max_country_count = player_country_count.max()
	for name, _ in player_country_count[player_country_count == max_country_count].index:
		print(
			f'{name} has been to at least {max_country_count} countries (not counting anything not identifiable as any country)'
		)
	player_maybe_country_count = all_rows.groupby(['name', 'username'])['cc'].apply(
		lambda group: group.nunique(dropna=False)
	)
	max_maybe_country_count = player_maybe_country_count.max()
	for name, _ in player_maybe_country_count[
		player_maybe_country_count == max_maybe_country_count
	].index:
		print(
			f'{name} has been to at least {max_country_count} countries including unidentifiable locations'
		)

	# Player that submitted That One Picture the most of everyone
	times_used = all_rows.groupby(['name', 'username', 'latitude', 'longitude'], sort=False).size()
	max_times_used = times_used.max()
	async with ClientSession() as sesh:
		for name, _, latitude, longitude in times_used[times_used == max_times_used].index:
			print(
				f'{name} submitted {latitude}, {longitude} {max_times_used} times',
				await describe_coord(latitude, longitude, sesh),
			)


if __name__ == '__main__':
	asyncio.run(main(), debug=False)

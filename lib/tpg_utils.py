from collections import Counter
from collections.abc import Collection, Mapping
from enum import IntEnum

import pandas


class Medal(IntEnum):
	"""Medals that are worth points for 1st/2nd/3rd place in a round."""

	Gold = 3
	Silver = 2
	Bronze = 1


def count_medals(medals: Mapping[str, Collection[Medal]]):
	"""Tallies medals from podium placements.

	Arguments:
		medals: {player name: [all medals obtained in the season]}

	Returns:
		DataFrame, indexed by player name, with columns for counts of each medal and a "Medal Score" column for total medal points (with gold medals being 3 points, silver medals worth 2, etc) that it is sorted by
	"""

	counts: dict[str, dict[str, int]] = {medal: {} for medal in Medal._member_names_}
	"""{medal type: {player name: amount of times this medal was won}}"""
	points: dict[str, int] = {}
	"""{player name: total points of all medals}"""

	for player_name, player_medals in medals.items():
		counter = Counter(player_medals)
		for medal, count in counter.items():
			counts[medal.name][player_name] = count
		points[player_name] = sum(player_medals)

	df = pandas.DataFrame(counts)
	df.index.name = 'Player'
	df = df.fillna(0)
	df['Medal Score'] = points
	return df.sort_values('Medal Score', ascending=False)

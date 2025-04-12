import contextlib
from typing import TYPE_CHECKING, Any

from lib.reverse_geocode import reverse_geocode_address_sync

if TYPE_CHECKING:
	import pandas
	import requests


def tpg_score(distances: 'pandas.Series'):
	"""
	Computes the score for a whole round of TPG. Note: Not complete yet, this does not take ties into account.

	Arguments:
		distances: Distances in kilometres for each round."""
	distance_scores = 0.25 * (20_000 - distances)
	distance_ranks = distances.rank(method='min', ascending=True)
	players_beaten = distances.size - distances.rank(method='max', ascending=True)
	players_beaten_scores = 5000 * (players_beaten / (distances.size - 1))
	scores = distance_scores + players_beaten_scores
	bonus = distance_ranks.map({1: 3000, 2: 2000, 3: 1000})
	# TODO: Should actually just pass in the fivek column
	bonus.loc[distance_scores <= 0.1] = 5000
	scores += bonus.fillna(0)
	scores.loc[distances >= 19_995] = 5000  # Antipode 5K
	for _, group in scores.groupby(distance_ranks, sort=False):
		# where distance is tied, all players in that tie receive the average of what the points would be
		scores.loc[group.index] = group.mean()
	return scores.round(2)


def print_round(n: int, row: Any, sesh: 'requests.Session | None' = None):
	loc_address = reverse_geocode_address_sync(row.target_lat, row.target_lng, sesh)
	print(f'{n}: Round {row.round}: {row.target_lat, row.target_lng} {loc_address}')
	sub_address = reverse_geocode_address_sync(row.latitude, row.longitude, sesh)
	print(f'Submission: {row.latitude}, {row.longitude} {sub_address}')
	print(
		f'Distance: {row.distance / 1000:4g}km Place: {row.place}/{row.total_subs} Score: {row.score}'
	)
	with contextlib.suppress(AttributeError):
		print(
			f'Geodesic distance: {row.geod_distance / 1000:.4g}km Heading from photo to loc: {row.heading}°'
		)

	print('-' * 10)

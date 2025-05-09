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
	bonus.loc[distances <= 0.1] = 5000
	scores += bonus.fillna(0)
	scores.loc[distances >= 19_995] = 5000  # Antipode 5K
	for _, group in scores.groupby(distance_ranks, sort=False):
		# where distance is tied, all players in that tie receive the average of what the points would be
		scores.loc[group.index] = group.mean()
	return scores.round(2)


def custom_tpg_score(
	distances: 'pandas.Series',
	world_distance: float = 20_000.0,
	fivek_score: float | None = 7_500.0,
):
	"""
	Computes the score for a whole round of TPG, with a custom world distance constant, for spinoff TPGs that cover a smaller area. Does not factor in ties because I don't care. Rounds to 2 decimal places as normal.

	Arguments:
		distances: Distances in kilometres for each round.
		world_distance: Maximum distance possible in this subset of the world in kilometres, defaults to 20K which is the default constant (not the exact max distance of the earth but close enough) anyway.
		fivek_score: Flat score for 5Ks (100m radius), or None to disable this / consider 5Ks manually.
	"""
	distance_scores = world_distance - distances
	players_beaten = distances.size - distances.rank(method='max', ascending=True)
	players_beaten_scores = 5000 * (players_beaten / (distances.size - 1))
	scores = (distance_scores + players_beaten_scores) / 2
	if fivek_score:
		scores[distances <= 0.1] = fivek_score
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

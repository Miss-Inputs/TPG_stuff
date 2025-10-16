#!/usr/bin/env python3
"""Simulate rounds as though all known players are present and submitting what is known to be their best pic."""

import logging
from argparse import ArgumentParser, BooleanOptionalAction
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING

import geopandas
import shapely
from geopandas.geoseries import GeoSeries
from pandas import Index, RangeIndex
from travelpygame import Round, ScoringOptions, load_points, load_rounds, main_tpg_scoring
from travelpygame.random_points import random_point_in_bbox, random_points_in_poly
from travelpygame.simulation import (
	SimulatedStrategy,
	Simulation,
	get_player_summary,
	get_round_summary,
	simulate_existing_rounds,
)
from travelpygame.tpg_data import get_submissions_per_user_from_path
from travelpygame.util import (
	format_point,
	format_xy,
	output_geodataframe,
	read_geodataframe,
	try_set_index_name_col,
)

if TYPE_CHECKING:
	from geopandas import GeoSeries


def compare_rounds(old_round: Round, new_round: Round, name: str | None):
	if old_round.submissions and old_round.is_scored:
		old_winner = max(old_round.submissions, key=lambda sub: sub.score or float('nan'))
		if old_winner.name != new_round.submissions[0].name:
			# TODO: Output this somewhere
			print(
				f'{old_round.name}: Winner was {old_round.submissions[0].name}, now {new_round.submissions[0].name}'
			)
	if name:
		old_sub = next((sub for sub in old_round.submissions if sub.name == name), None)
		if not old_sub:
			return
		new_sub = next(sub for sub in new_round.submissions if sub.name == name)
		if old_sub.rank is not None and new_sub.rank is not None and old_sub.rank != new_sub.rank:
			print(f'{old_round.name}: Placing changed from {old_sub.rank} to {new_sub.rank}')
		if (old_sub.latitude != new_sub.latitude) or (old_sub.longitude != new_sub.longitude):
			old_desc = old_sub.description or format_xy(old_sub.longitude, old_sub.latitude)
			new_desc = new_sub.description or format_xy(new_sub.longitude, new_sub.latitude)
			print(f'{old_round.name}: Previously submitted {old_desc}, now would submit {new_desc}')


def get_simulation(
	existing_rounds: list[Round] | None,
	pics: Mapping[str, 'GeoSeries | Sequence[shapely.Point]'],
	scoring: ScoringOptions,
	strategy: SimulatedStrategy,
	targets_path: Path | None,
	num_random_rounds: int | None,
	region_path: Path | None,
	*,
	use_haversine: bool,
) -> Simulation:
	if not targets_path and not num_random_rounds:
		if existing_rounds is None:
			raise RuntimeError('You have nothing to simulate')
		return simulate_existing_rounds(
			existing_rounds, pics, scoring, strategy, use_haversine=use_haversine
		)

	if targets_path:
		targets = load_with_name(targets_path)
		rounds = {
			str(index): point
			for index, point in targets.geometry.items()
			if isinstance(point, shapely.Point)
		}
	elif num_random_rounds:
		if region_path:
			region = read_geodataframe(region_path)
			points = random_points_in_poly(
				region, num_random_rounds, use_tqdm=True, desc='Generating random points'
			)
		else:
			points = [random_point_in_bbox(-180, -90, 180, 90) for _ in range(num_random_rounds)]
		rounds = {format_point(point): point for point in points}
	else:
		raise RuntimeError('Not sure how we got here')
	return Simulation(rounds, None, pics, scoring, strategy, use_haversine=use_haversine)


def output_results(
	new_rounds: list[Round],
	existing_rounds: list[Round] | None,
	name: str | None,
	rounds_output_path: Path | None,
	output_path: Path | None,
	losing_rounds_path: Path | None,
):
	if existing_rounds:
		for result in new_rounds:
			r = next((r for r in existing_rounds if r.name == result.name), None)
			if r:
				compare_rounds(r, result, name)

	round_summary = get_round_summary(new_rounds, name)
	print(round_summary)
	if rounds_output_path:
		round_summary.to_csv(rounds_output_path)

	# TODO: More detailed stats here, like maybe a whole entire leaderboard
	player_summary = get_player_summary(new_rounds)
	print(player_summary)
	if output_path:
		player_summary.to_csv(output_path)

	if losing_rounds_path and name:
		losing_rounds = [
			{'name': r.name or format_point(r.target), 'target': r.target}
			for r in new_rounds
			if r.submissions[-1].name == name
		]
		if losing_rounds:
			output_geodataframe(
				geopandas.GeoDataFrame(losing_rounds, geometry='target', crs='wgs84'),
				losing_rounds_path,
			)


def load_with_name(path: Path | str):
	points = load_points(path)
	points = try_set_index_name_col(points)
	if isinstance(points.index, RangeIndex):
		# Try and get something more descriptive than just the default increasing index
		points.index = Index(
			[
				format_point(geo) if isinstance(geo, shapely.Point) else str(geo)
				for geo in points.geometry
			]
		)
	return points


def get_pics(
	pics_per_user: Mapping[str, 'GeoSeries'],
	name: str | None,
	points_path: Path | None,
	threshold: int | None,
	additional_players: list[list[str]] | None,
) -> dict[str, 'GeoSeries']:
	pics = {
		player: player_pics
		for player, player_pics in pics_per_user.items()
		if threshold is None or player_pics.size >= threshold
	}
	if additional_players:
		for additional_name, path in additional_players:
			points = load_with_name(path)
			pics[additional_name] = points.geometry

	if points_path:
		if not name:
			print(
				'Warning: --points-path does not do anything without --name. You may want to use --add-player if these points are for a different player'
			)
		else:
			# TODO: Probably we want to combine the points rather than replace them (for example, a 5K might be just a submission and not something one keeps track of in the point set)
			pics[name] = load_with_name(points_path).geometry
	if not pics:
		raise RuntimeError('Nobody is able to be simulated')
	return pics


def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	strategy_choices = {s.name.lower(): s for s in SimulatedStrategy}
	argparser.add_argument(
		'submissions_path',
		type=Path,
		help='Path to either a geofile with a "player" column to get submissions per player, or to TPG data to get per-user submissions and to use the rounds as targets if targets is not specified',
	)
	argparser.add_argument(
		'--name',
		help='Optionally keep track of a particular player (likely use case = yourself) and how their submission changes',
	)

	target_args = argparser.add_argument_group(
		'Target arguments', 'Which locations will be the targets of each simulated round'
	).add_mutually_exclusive_group(required=False)
	target_args.add_argument(
		'--targets',
		type=Path,
		help='If this path is specified, load points from this file to be used as each round',
	)
	target_args.add_argument(
		'--random-rounds', type=int, help='If this is specified, generate N random rounds'
	)
	target_args.add_argument(
		'--random-in-region',
		nargs=2,
		metavar=('number_of_points', 'path'),
		help='If this is specified, it must be two arguments: number of points and path of a file containing geometry to generate random points in',
	)

	sim_args = argparser.add_argument_group(
		'Simulation arguments', 'Arguments controlling how the simulation is simulated'
	)
	sim_args.add_argument(
		'--custom-scoring',
		type=float,
		help="If specified, use a scoring method for regional TPGs with this as the world distance in km. If not specified, use main TPG scoring. This is a bit awkward but I could'nt think of anything better right now whoops",
	)
	sim_args.add_argument(
		'--strategy',
		choices=strategy_choices.keys(),
		help='Strategy of simulated players',
		default='closest',
	)
	sim_args.add_argument(
		'--use-haversine',
		action=BooleanOptionalAction,
		help='Use haversine for distances, defaults to true',
		default=True,
	)

	output_args = argparser.add_argument_group(
		'Output arguments', 'Arguments specifying what is output and where'
	)
	output_args.add_argument(
		'--output-path', type=Path, help='Output total scores/etc of simulated players here'
	)
	output_args.add_argument(
		'--rounds-output-path', type=Path, help='Output winners/etc of each round here'
	)
	output_args.add_argument(
		'--losing-rounds-path',
		type=Path,
		help='With --name, output rounds where that player loses here',
	)

	player_args = argparser.add_argument_group(
		'Pics arguments',
		'Arguments to control what simulated players exist and what pics they have',
	)
	player_args.add_argument(
		'--points-path',
		type=Path,
		help="In conjunction with --name, replace your points with those loaded from this file (to use pics that you haven't submitted yet)",
	)
	player_args.add_argument(
		'--threshold',
		type=int,
		help='Only simulate players who have submitted at least this amount of pics. This can help speed up the simulation',
	)
	player_args.add_argument(
		'--add-player',
		'--additional-player',
		dest='add_player',
		action='append',
		nargs=2,
		metavar=('name', 'point_set_path'),
		help='Add a new player with a name and points from a file (pair of arguments in that order, can be specified multiple times)',
	)
	# TODO: Get main TPG data if data_path is not provided (would need to rewrite this as async which isn't necessarily difficult or time consuming but I'm very cbf)
	# TODO: Option to also try with a new_points point set, and see how it compares, and what pics would improve your ranking etc
	args = argparser.parse_args()

	path: Path = args.submissions_path
	name: str | None = args.name
	points_path: Path | None = args.points_path
	targets_path: Path | None = args.targets
	num_random_points: int | None = args.random_rounds
	random_points = args.random_in_region
	if random_points:
		num_random_points = int(random_points[0])
		region_path = Path(random_points[1])
	else:
		region_path = None

	strategy = strategy_choices[args.strategy]
	if args.custom_scoring:
		scoring = ScoringOptions(7500, None, None, None, args.custom_scoring)
	else:
		scoring = main_tpg_scoring

	existing_rounds = load_rounds(path) if path.suffix[1:].lower() == 'json' else None
	pics_per_user = get_submissions_per_user_from_path(path)

	pics = get_pics(pics_per_user, name, points_path, args.threshold, args.add_player)
	simulation = get_simulation(
		existing_rounds,
		pics,
		scoring,
		strategy,
		targets_path,
		num_random_points,
		region_path,
		use_haversine=args.use_haversine,
	)

	if name and name not in simulation.player_pics:
		print(
			f'Warning: {name} was not found in TPG data or otherwise did not have any pics, so does not exist in this context. Setting to None'
		)
		name = None

	new_rounds = simulation.simulate_rounds()
	output_results(
		new_rounds,
		existing_rounds,
		name,
		args.rounds_output_path,
		args.output_path,
		args.losing_rounds_path,
	)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	main()

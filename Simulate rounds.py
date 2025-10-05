#!/usr/bin/env python3
"""Simulate rounds as though all known players are present and submitting what is known to be their best pic."""

import logging
from argparse import ArgumentParser, BooleanOptionalAction
from pathlib import Path

import shapely
from travelpygame import (
	Round,
	ScoringOptions,
	get_submissions_per_user,
	load_points,
	load_rounds,
	main_tpg_scoring,
)
from travelpygame.random_points import random_point_in_bbox
from travelpygame.simulation import (
	SimulatedStrategy,
	Simulation,
	get_player_summary,
	get_round_summary,
	simulate_existing_rounds,
)
from travelpygame.util import format_point, format_xy, try_set_index_name_col


def compare_rounds(old_round: Round, new_round: Round, name: str | None):
	if old_round.is_scored:
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
	existing_rounds: list[Round],
	scoring: ScoringOptions,
	strategy: SimulatedStrategy,
	targets_path: Path | None,
	num_random_rounds: int | None,
	*,
	use_haversine: bool,
):
	if not targets_path and not num_random_rounds:
		return simulate_existing_rounds(
			existing_rounds, scoring, strategy, use_haversine=use_haversine
		), True

	if targets_path:
		targets = try_set_index_name_col(load_points(targets_path))
		rounds = {
			str(index): point
			for index, point in targets.geometry.items()
			if isinstance(point, shapely.Point)
		}
	elif num_random_rounds:
		points = [random_point_in_bbox(-180, -90, 180, 90) for _ in range(num_random_rounds)]
		rounds = {format_point(point): point for point in points}
	else:
		raise RuntimeError('Not sure how we got here')
	pics = {
		player: shapely.points([(lng, lat) for lat, lng in latlngs]).tolist()
		for player, latlngs in get_submissions_per_user(existing_rounds).items()
	}
	return Simulation(rounds, None, pics, scoring, strategy, use_haversine=use_haversine), False


def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	strategy_choices = {s.name.lower(): s for s in SimulatedStrategy}
	argparser.add_argument(
		'data_path',
		type=Path,
		help='Path to TPG data to use (to get submissions from), and if an option such as --targets is not specified, the rounds will be used',
	)
	target_args = argparser.add_mutually_exclusive_group(required=False)
	target_args.add_argument(
		'--targets',
		type=Path,
		help='If this path is specified, load points from this file to be used as each round',
	)
	target_args.add_argument(
		'--random-rounds', type=int, help='If this is specified, generate N random rounds'
	)

	argparser.add_argument(
		'--name',
		help='Optionally keep track of a particular player (likely use case = yourself) and how their submission changes',
	)
	argparser.add_argument(
		'--points-path',
		type=Path,
		help="In conjunction with --name, replace your points with those loaded from this file (to use pics that you haven't submitted yet)",
	)
	argparser.add_argument(
		'--output-path', type=Path, help='Output total scores/etc of simulated players here'
	)
	argparser.add_argument(
		'--rounds-output-path', type=Path, help='Output winners/etc of each round here'
	)
	argparser.add_argument(
		'--custom-scoring',
		type=float,
		help="If specified, use a scoring method for regional TPGs with this as the world distance in km. If not specified, use main TPG scoring. This is a bit awkward but I could'nt think of anything better right now whoops",
	)
	argparser.add_argument(
		'--strategy',
		choices=strategy_choices.keys(),
		help='Strategy of simulated players',
		default='closest',
	)
	argparser.add_argument(
		'--use-haversine',
		action=BooleanOptionalAction,
		help='Use haversine for distances, defaults to true',
		default=True,
	)
	# TODO: Get main TPG data if data_path is not provided (would need to rewrite this as async which isn't necessarily difficult or time consuming but I'm very cbf)
	# TODO: Many more options - generate randomly from regions, etc
	# TODO: Option to add some point sets as fictional players
	# TODO: Option to also try with a new_points point set, and see how it compares, and what pics would improve your ranking etc
	args = argparser.parse_args()

	path: Path = args.data_path
	name: str | None = args.name
	points_path: Path | None = args.points_path
	targets_path: Path | None = args.targets
	strategy = strategy_choices[args.strategy]
	if args.custom_scoring:
		scoring = ScoringOptions(7500, None, None, None, args.custom_scoring)
	else:
		scoring = main_tpg_scoring

	existing_rounds = load_rounds(path)  # Stil need this either way to get the submissions
	simulation, using_existing_rounds = get_simulation(
		existing_rounds,
		scoring,
		strategy,
		targets_path,
		args.random_rounds,
		use_haversine=args.use_haversine,
	)
	if points_path:
		if not name:
			print('Warning: --points-path does not do anything without --name')
		else:
			simulation.player_pics[name] = try_set_index_name_col(load_points(points_path)).geometry

	new_rounds = simulation.simulate_rounds()
	if using_existing_rounds:
		for result in new_rounds:
			r = next((r for r in existing_rounds if r.name == result.name), None)
			if r:
				compare_rounds(r, result, name)

	round_summary = get_round_summary(new_rounds)
	print(round_summary)
	if args.rounds_output_path:
		round_summary.to_csv(args.rounds_output_path)

	# TODO: More detailed stats here, like maybe a whole entire leaderboard
	player_summary = get_player_summary(new_rounds)
	print(player_summary)
	if args.output_path:
		player_summary.to_csv(args.output_path)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	main()

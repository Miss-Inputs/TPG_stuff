#!/usr/bin/env python3
"""Simulate rounds as though all known players are present and submitting what is known to be their best pic."""

import logging
from argparse import ArgumentParser, BooleanOptionalAction
from pathlib import Path

import pandas
from travelpygame import ScoringOptions, load_rounds, main_tpg_scoring
from travelpygame.simulation import SimulatedStrategy, simulate_existing_rounds


def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	strategy_choices = {s.name.lower(): s for s in SimulatedStrategy}
	argparser.add_argument('data_path', type=Path, help='Path to TPG data to use')
	argparser.add_argument(
		'--output-path', type=Path, help='Output total scores of simulated players here'
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
	# TODO: Many more options - load round targets from point set, generate randomly from regions, etc
	# TODO: Option to replace/append a particular user's pics with that of a point set (so you can use pics from your voronoi and not just what you've submitted)
	# TODO: Option to add some point sets as fictional players
	# TODO: Option to also try with a new_points point set, and see how it compares, and what pics would improve your ranking etc
	args = argparser.parse_args()

	path: Path = args.data_path
	strategy = strategy_choices[args.strategy]
	if args.custom_scoring:
		scoring = ScoringOptions(7500, None, None, None, args.custom_scoring)
	else:
		scoring = main_tpg_scoring

	rounds = load_rounds(path)
	totals: dict[str, float] = {}
	for result in simulate_existing_rounds(
		rounds, scoring, strategy, use_haversine=args.use_haversine
	):
		r = next((r for r in rounds if r.name == result.name), None)
		if r and r.is_scored:
			old_winner = max(r.submissions, key=lambda sub: sub.score or float('nan'))
			if old_winner.name != result.submissions[0].name:
				# TODO: Output this somewhere
				print(
					f'{r.name}: Winner was {r.submissions[0].name}, now {result.submissions[0].name}'
				)

		for sub in result.submissions:
			assert sub.score is not None, 'why is sub.score None'
			if sub.name not in totals:
				totals[sub.name] = 0
			totals[sub.name] += sub.score
	total_scores = pandas.Series(totals, name='total').sort_values(ascending=False)
	print(total_scores)
	if args.output_path:
		total_scores.to_csv(args.output_path)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	main()

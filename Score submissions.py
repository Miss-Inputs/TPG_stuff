#!/usr/bin/env python3
"""Scores rounds/submissions, either from an exported submission tracker (kmz/kml), or from JSON."""

from argparse import ArgumentParser, BooleanOptionalAction
from pathlib import Path

import pandas
from travelpygame import (
	Round,
	ScoringOptions,
	convert_submission_tracker,
	load_rounds,
	make_leaderboards,
	rounds_to_json,
	score_round,
)
from travelpygame.util import format_distance


def _round_number_getter(r: Round):
	return r.number


def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument('path', type=Path, help='Path to CSV/KML file', nargs='+')
	argparser.add_argument(
		'--output-path',
		type=Path,
		help='Path to save json of scored rounds to, and base name for leaderboards etc',
	)
	argparser.add_argument(
		'--world-distance',
		type=float,
		help='Max distance in the world (in km), used for calculating scoring, defaults to 5000km',
		default=5_000.0,
	)
	argparser.add_argument(
		'--fivek-score',
		'--5k-score',
		dest='fivek_score',
		type=float,
		help='Score for a 5K, defaults to 7500',
		default=7_500.0,
	)
	argparser.add_argument(
		'--fivek-threshold',
		'--5k-threshold',
		dest='fivek_threshold',
		type=float,
		help='Threshold in metres for a submission being close enough to be considered a 5K, used for calculating scoring, defaults to 100m',
		default=100,
	)
	argparser.add_argument(
		'--fivek-suffix',
		'--5k-suffix',
		dest='fivek_suffix',
		type=str,
		help='If loading from kml/kmz, suffix to add to submissions in the tracker to manually mark them as 5Ks',
		default=' (5K)',
	)
	argparser.add_argument(
		'--use-haversine',
		action=BooleanOptionalAction,
		help='Use haversine instead of WGS geod for scoring (less accurate as it assumes the earth is a sphere, but more consistent with other TPG things), defaults to True',
		default=True,
	)
	argparser.add_argument(
		'--clip-negative',
		action=BooleanOptionalAction,
		help='If True (default), gives a score of 0 for submissions outside of world_distance km, otherwise lets them be negative',
		default=True,
	)
	argparser.add_argument(
		'--reminder-list',
		type=Path,
		help='Path to file containing names of people who want to be reminded if they have not submitted',
	)
	argparser.add_argument(
		'--ongoing-round',
		action=BooleanOptionalAction,
		help='Ignore the last round for the leaderboard and treat it as currently ongoing, defaults to False (ie run this after the round finishes)',
		default=False,
	)
	args = argparser.parse_args()

	paths: list[Path] = args.path
	output_path: Path | None = args.output_path
	reminder_list_path: Path | None = args.reminder_list
	reminder_list = (
		{line for line in reminder_list_path.read_text('utf8').splitlines() if line}
		if reminder_list_path
		else set()
	)

	rounds: list[Round] = []
	last_round_num = 0
	for path in paths:
		if path.suffix[1:].lower() in {'kml', 'kmz'}:
			loaded = convert_submission_tracker(
				path, last_round_num + 1, fivek_suffix=args.fivek_suffix
			)
		else:
			loaded = load_rounds(path)
		last_round_num = max(r.number for r in loaded)
		rounds += loaded
	rounds.sort(key=_round_number_getter)

	# TODO: This should load options from the file actually, though for KMZ we would still need a way to override via command line
	options = ScoringOptions(
		fivek_flat_score=args.fivek_score,
		fivek_bonus=None,
		rank_bonuses=None,
		antipode_5k_flat_score=None,
		world_distance_km=args.world_distance,
		clip_negative=args.clip_negative,
	)

	scored_rounds = [
		r
		if r.is_scored
		else score_round(r, options, args.fivek_threshold, use_haversine=args.use_haversine)
		for r in rounds
	]
	if output_path:
		output_path.write_text(rounds_to_json(scored_rounds))

	points, distance, medals = (
		make_leaderboards(scored_rounds[:-1])
		if args.ongoing_round
		else make_leaderboards(scored_rounds)
	)
	print(points)
	if output_path:
		points.to_csv(output_path.with_name(f'{output_path.stem} - Points Leaderboard.csv'))
	print(distance)
	if output_path:
		distance.to_csv(output_path.with_name(f'{output_path.stem} - Distance Leaderboard.csv'))
	print(medals)
	if output_path:
		medals.to_csv(output_path.with_name(f'{output_path.stem} - Medals Leaderboard.csv'))

	latest_round = scored_rounds[-1]
	df = pandas.DataFrame([s.model_dump() for s in latest_round.submissions])
	df = df.dropna(axis='columns', how='all')
	if output_path:
		round_output_path = output_path.with_name(f'{output_path.stem} - {latest_round.name}.csv')
		df['distance_km'] = df['distance'] / 1_000
		df.drop(columns='distance').to_csv(round_output_path, index=False)

	# Print them all so we know who to sort where in what order, and for top 3 result post etc
	print(latest_round.name, latest_round.latitude, latest_round.longitude)

	df['distance'] = df['distance'].map(format_distance)
	print(
		df.drop(columns=['is_tie', 'latitude', 'longitude'])
		.set_index('name')
		.to_string(max_colwidth=60)
	)

	submitted_names = frozenset(df['name'])
	needs_reminder = reminder_list - submitted_names
	if needs_reminder:
		print('Reminder to submit:', needs_reminder)


if __name__ == '__main__':
	main()

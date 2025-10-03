#!/usr/bin/env python3
"""Converts exported submission trackers to JSON, for use with other scripts"""

from argparse import ArgumentParser
from itertools import groupby
from pathlib import Path

from travelpygame import Round, convert_submission_tracker, rounds_to_json


def _season_sorter(r: Round):
	return -1 if r.season is None else r.season

def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument('path', type=Path, help='Path to KML/KMZ file', nargs='+')
	argparser.add_argument('output_path', type=Path, help='Path to save json')
	season_args = argparser.add_mutually_exclusive_group()
	season_args.add_argument('--start-round', type=int, help='Number to start counting rounds from, defaults to 1', default=1)
	season_args.add_argument('--season', type=int, help='Season number for all these rounds')
	season_args.add_argument(
		'--season-start',
		type=int,
		help='Starting round number for each season, to automatically detect the season',
		nargs='+',
	)
	# Should that be nargs=* instead? Oh well, it seems to work and not accidentally be required
	args = argparser.parse_args()

	paths: list[Path] = args.path
	season: int | None = args.season
	season_starts: list[int] | None = args.season_start
	output_path: Path = args.output_path

	rounds = convert_submission_tracker(paths, args.start_round, season_starts or season)
	if season_starts or season:
		for season, season_rounds in groupby(sorted(rounds, key=_season_sorter), _season_sorter):
			season_rounds = list(season_rounds)
			print(f'Season {season}: {len(season_rounds)} rounds: {[r.name or r.number for r in season_rounds]}')
	j = rounds_to_json(rounds)

	if output_path.suffix[1:].lower() in {'kml', 'kmz'}:
		print(f"output_path is {output_path}, assuming you didn't intend to do that")
		return
	output_path.write_text(j, 'utf8')


if __name__ == '__main__':
	main()

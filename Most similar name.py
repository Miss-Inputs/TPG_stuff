#!/usr/bin/env python3
"""For fun: Most similar name of a point in a point set to some other string (compares the Levenshtein distance of strings).
Perhaps it can help pick some pic to use as a joke submission if you are too far away to get any meaningful points or otherwise want to throw."""
#TODO: Put this in the readme after I've cleaned it up a bit

import asyncio
import logging
from argparse import ArgumentParser, BooleanOptionalAction
from collections.abc import Collection, Sequence
from operator import itemgetter
from pathlib import Path

from pandas import DataFrame, Series
from travelpygame.util.text import levenshtein_dist

from lib.io_utils import load_point_set_from_arg

logger = logging.getLogger(__name__)


def get_levvy_dists(names: Collection[str], target: str, *, ignore_case: bool):
	if ignore_case:
		target = target.casefold()
		dists = {name: levenshtein_dist(name.casefold(), target) for name in names}
	else:
		dists = {name: levenshtein_dist(name, target) for name in names}
	return Series(dists, name='dist').sort_values(ascending=True)


def _read_all_lines(path: Path):
	lines = path.read_text('utf8').splitlines()
	return [line for line in lines if line and not line.isspace()]


def get_closest_to_all(names: Collection[str], targets: Collection[str], *, ignore_case: bool):
	closest_names = {}
	closest_dists = {}

	for target in targets:
		if ignore_case:
			target_casefold = target.casefold()
			dists = {name: levenshtein_dist(name.casefold(), target_casefold) for name in names}
		else:
			dists = {name: levenshtein_dist(name, target) for name in names}
		min_name, min_dist = min(dists.items(), key=itemgetter(1))
		closest_names[target] = min_name
		closest_dists[target] = min_dist
	df = DataFrame({'closest_name': closest_names, 'dist': closest_dists})
	df.index.name = 'target'
	if isinstance(targets, Sequence):
		return df.loc[list(targets)]
	
	return df.sort_index(axis='index')


def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument(
		'point_set',
		help="Path to file containing points (.csv, .ods, .xls, .xlsx, pickled DataFrame, GeoJSON, etc) or potentially a <player:> or <username:> arg, but each point must have names, or it won't work.",
	)

	load_args = argparser.add_argument_group(
		'Loading arguments', 'Arguments to control how point_set is loaded'
	)

	load_args.add_argument(
		'--lat-column',
		'--latitude-column',
		dest='lat_col',
		help='Force a specific column label for latitude, defaults to autodetected',
	)
	load_args.add_argument(
		'--lng-column',
		'--longitude-column',
		dest='lng_col',
		help='Force a specific column label for longitude, defaults to autodetected',
	)
	load_args.add_argument(
		'--unheadered',
		action='store_true',
		help='Explicitly treat csv/Excel as not having a header, otherwise autodetect (and default to yes header if unknown)',
	)
	load_args.add_argument(
		'--crs',
		default='wgs84',
		help='Coordinate reference system to use if point_set is .csv/.ods/etc, defaults to WGS84',
	)
	load_args.add_argument(
		'--name-col',
		help='Force a specific column label for the name of each point, otherwise autodetect',
	)

	target_args = argparser.add_mutually_exclusive_group(required=True)
	target_args.add_argument(
		'--string', '--target', help='String to compare names in your point set to'
	)
	target_args.add_argument(
		'--file', type=Path, help='A file containing names on each line to compare names to'
	)

	argparser.add_argument(
		'--ignore-case',
		'--casefold',
		action=BooleanOptionalAction,
		help='Ignore case when comparing strings, defaults to false',
	)

	args = argparser.parse_args()
	ignore_case: bool = args.ignore_case

	point_set = asyncio.run(
		load_point_set_from_arg(
			args.point_set,
			args.lat_col,
			args.lng_col,
			args.crs,
			args.name_col,
			force_unheadered=args.unheadered,
		)
	)
	names = {index for index, _ in point_set.items() if isinstance(index, str)}  # noqa: PERF102 #Whoops it's not a dict, sorry Ruff
	# TODO: Have point sets be just one option for 'source', which might also be a newline-delimited file or spreadsheet or similar, will have to think harder about argument names
	# TODO: This would benefit greatly from reverse geocoding, esp as the names for each point don't actually have to be unique

	if args.string:
		dists = get_levvy_dists(names, args.string, ignore_case=ignore_case)
		print(dists)
	if args.file:
		targets = _read_all_lines(args.file)
		closest = get_closest_to_all(names, targets, ignore_case=ignore_case)
		print(closest)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	main()

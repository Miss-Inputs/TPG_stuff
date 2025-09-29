#!/usr/bin/env python3
"""Simply parse the exported submission tracker and convert it to GeoJSON."""

from argparse import ArgumentParser, BooleanOptionalAction
from pathlib import Path

from geopandas import GeoDataFrame
from travelpygame.util import parse_submission_kml


def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument('path', type=Path, help='Path to CSV/KML file', nargs='+')
	argparser.add_argument('output_path', type=Path, help='Path to output GeoJSON to')
	argparser.add_argument(
		'--mode',
		choices=('rounds', 'submissions', 'both'),
		default='both',
		help='Whether to export the rounds or submissions or both (default)',
	)
	argparser.add_argument(
		'--drop-duplicates',
		action=BooleanOptionalAction,
		default=True,
		help='Whether to avoid outputting duplicate points (default true)',
	)
	argparser.add_argument(
		'--name', help='Optionally only output submissions from a user with this name'
	)
	args = argparser.parse_args()

	tracker = parse_submission_kml(args.path)

	points = []
	if args.mode in {'rounds', 'both'}:
		points += [{'name': r.name, 'point': r.target} for r in tracker.rounds]
	if args.mode in {'submissions', 'both'}:
		for r in tracker.rounds:
			if args.name:
				points += [
					{
						'name': sub.description,
						'style': sub.style,
						'round': r.name,
						'point': sub.point,
					}
					for sub in r.submissions
					if sub.name == args.name
				]
			else:
				points += [
					{
						'name': sub.name,
						'desc': sub.description,
						'style': sub.style,
						'round': r.name,
						'point': sub.point,
					}
					for sub in r.submissions
				]
	gdf = GeoDataFrame(points, geometry='point', crs='wgs84')
	if args.drop_duplicates:
		gdf = gdf.drop_duplicates(subset='point', keep='first')
	gdf.to_file(args.output_path)


if __name__ == '__main__':
	main()

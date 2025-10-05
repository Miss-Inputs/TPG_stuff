#!/usr/bin/env python3
"""Gets the most frequently used locations of every player (who has submitted a location more than once, or some other configurable amount.)."""

import asyncio
import logging
from argparse import ArgumentParser, BooleanOptionalAction
from collections import Counter, defaultdict
from operator import itemgetter
from pathlib import Path

import geopandas
from shapely import Point
from tqdm.auto import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from travelpygame import get_main_tpg_rounds_with_path, load_rounds_async
from travelpygame.util import output_geodataframe

from lib.settings import Settings


async def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument(
		'--output-path',
		type=Path,
		help='Where to save submissions, can be .csv or .geojson/.gpkg/etc',
	)
	argparser.add_argument(
		'--data-path', type=Path, help='TPG data path, if not specified, will get main TPG data.'
	)
	argparser.add_argument(
		'--include-ties',
		action=BooleanOptionalAction,
		default=True,
		help="Include all pics that are a user's most submitted, as there can be ties. Defaults to true",
	)
	argparser.add_argument(
		'--threshold',
		type=int,
		default=2,
		help='Only include pics from players who have submitted a pic at least this amount of times. Defaults to 2, setting it to 1 will include all kinds of one-off random pics and setting it to 0 or lower accomplishes nothing more than that',
	)
	args = argparser.parse_args()
	output_path: Path | None = args.output_path

	if args.data_path:
		rounds = await load_rounds_async(args.data_path)
	else:
		settings = Settings()
		rounds = await get_main_tpg_rounds_with_path(settings.main_tpg_data_path)

	location_counts: defaultdict[str, list[Point]] = defaultdict(list)

	for r in tqdm(rounds, 'Getting all submissions from all rounds', unit='round'):
		for sub in r.submissions:
			point = Point(sub.longitude, sub.latitude)
			location_counts[sub.name].append(point)

	rows = []
	for name, subs in location_counts.items():
		counts = Counter(subs)
		most_common, most_common_count = max(counts.items(), key=itemgetter(1))
		if most_common_count < args.threshold:
			continue
		rows.append(
			{
				'name': name,
				'most_common': most_common,
				'count': most_common_count,
				'num_unique_locations': len(counts),
			}
		)
		other_most_common = [
			loc
			for loc, count in counts.items()
			if loc != most_common and count == most_common_count
		]
		if other_most_common:
			if args.include_ties:
				rows += [
					{
						'name': f'{name} {i}',
						'most_common': loc,
						'count': most_common_count,
						'num_unique_locations': len(counts),
					}
					for i, loc in enumerate(other_most_common, 2)
				]
			else:
				print(
					f'{name} has ties for locations submitted {most_common_count} times (with {len(counts)} unique locations): {other_most_common}'
				)

	gdf = geopandas.GeoDataFrame(rows, geometry='most_common', crs='wgs84')
	gdf = gdf.sort_values('count', ascending=False)
	print(gdf)
	if output_path:
		await asyncio.to_thread(output_geodataframe, gdf, output_path, index=False)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	with logging_redirect_tqdm():
		asyncio.run(main())

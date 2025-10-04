#!/usr/bin/env python3
"""Gets all the points submitted by a particular user from TPG data."""

import asyncio
import logging
from argparse import ArgumentParser
from collections import defaultdict
from pathlib import Path

from geopandas import GeoDataFrame
from shapely import Point
from travelpygame import get_main_tpg_rounds_with_path, load_rounds_async
from travelpygame.util import geodataframe_to_csv

from lib.settings import Settings


async def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument('name', help='Name of player to get submissions for')
	argparser.add_argument(
		'--output-path',
		type=Path,
		help='Where to save submissions, can be .csv or .geojson/.gpkg/etc',
	)
	argparser.add_argument(
		'--data-path', type=Path, help='TPG data path, if not specified, will get main TPG data.'
	)
	args = argparser.parse_args()
	output_path: Path | None = args.output_path

	if args.data_path:
		rounds = await load_rounds_async(args.data_path)
	else:
		settings = Settings()
		rounds = await get_main_tpg_rounds_with_path(settings.main_tpg_data_path)

	usages: defaultdict[Point, list[str]] = defaultdict(list)
	for r in rounds:
		for sub in r.submissions:
			if sub.name != args.name:
				continue
			point = Point(sub.longitude, sub.latitude)
			usages[point].append(r.name or f'Round {r.number}')
	if not usages:
		print(f'Did not find any submissions by {args.name}')
		return
	gdf = GeoDataFrame(
		[{'geometry': point, 'usages': usage} for point, usage in usages.items()],
		geometry='geometry',
		crs='wgs84',
	)
	print(gdf)
	if output_path:
		if output_path.suffix[1:].lower() == 'csv':
			await asyncio.to_thread(geodataframe_to_csv, gdf, output_path, index=False)
		else:
			gdf.to_file(args.output_path)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	asyncio.run(main())

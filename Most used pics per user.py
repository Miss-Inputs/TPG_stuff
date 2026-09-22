#!/usr/bin/env python3
"""Gets the most frequently used locations of every player (who has submitted a location more than once, or some other configurable amount.)."""

import asyncio
import logging
from argparse import ArgumentParser, BooleanOptionalAction
from operator import itemgetter
from pathlib import Path

import geopandas
from tqdm.contrib.logging import logging_redirect_tqdm
from travelpygame.util import output_geodataframe

from lib.io_utils import load_or_fetch_submission_summary
from lib.settings import Settings


async def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument(
		'--output-path',
		type=Path,
		help='Where to save list of submissions, can be .csv or .geojson/.gpkg/etc',
	)
	argparser.add_argument(
		'--ties',
		action=BooleanOptionalAction,
		default=True,
		help="Include all pics that are a user's most submitted, as there can be ties. Defaults to true",
	)
	argparser.add_argument(
		'--threshold',
		type=int,
		default=2,
		help='Only include players who have submitted at least this amount of pics. Defaults to 2, setting it to 0 or lower is effectively disabling it',
	)
	argparser.add_argument(
		'--usage-threshold',
		type=int,
		default=2,
		help='Only include pics that have been submitted at least this number of times. Defaults to 2',
	)
	args = argparser.parse_args()
	output_path: Path | None = args.output_path

	settings = Settings()
	subs_path = settings.submission_summary_path
	tpg_export_path = settings.tpg_export_path
	aliases_path = settings.aliases_path

	sub_data = await load_or_fetch_submission_summary(
		subs_path, tpg_export_path, aliases_path=aliases_path, forbid_extra=True
	)

	rows = []
	for name, group in sub_data.per_player.items():
		n_pics = len(group)
		if n_pics < args.threshold:
			continue
		if args.ties:
			max_count = max(sub.count for sub in group)
			most_common_pics = [sub for sub in group if sub.count == max_count]
			# Ideally, we want to add any other info that might be in the submission summary
			rows.extend(
				{
					'player': name,
					'num_pics': n_pics,
					'usage': max_count,
					'first_used': most_common.earliest_known,
					'last_used': most_common.latest_known,
					'game_names': most_common.game_names,
					'geometry': most_common.point,
				}
				for most_common in most_common_pics
			)
		else:
			most_common, count = max(((sub, sub.count) for sub in group), key=itemgetter(1))
			rows.append(
				{
					'player': name,
					'num_pics': n_pics,
					'usage': count,
					'first_used': most_common.earliest_known,
					'last_used': most_common.latest_known,
					'game_names': most_common.game_names,
					'geometry': most_common.point,
				}
			)

	gdf = geopandas.GeoDataFrame(rows, crs='wgs84')
	gdf = gdf.sort_values(['usage', 'player'], ascending=[False, True])
	gdf = gdf[gdf['usage'] >= args.usage_threshold]
	print(gdf)
	if output_path:
		await asyncio.to_thread(output_geodataframe, gdf, output_path, index=False)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	with logging_redirect_tqdm():
		asyncio.run(main())

#!/usr/bin/env python3
"""Gets the most frequently used locations of every player (who has submitted a location more than once, or some other configurable amount.)."""

import asyncio
import logging
from argparse import ArgumentParser, BooleanOptionalAction
from pathlib import Path

import geopandas
from pandas import Series
from tqdm.contrib.logging import logging_redirect_tqdm
from travelpygame import load_or_fetch_submission_summary
from travelpygame.util import output_geodataframe

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
	subs_path = Settings().subs_per_player_path

	subs = await load_or_fetch_submission_summary(subs_path)

	rows = []
	for name, group in subs.groupby('username'):
		n_pics = group.index.size
		if n_pics < args.threshold:
			continue
		if args.ties:
			max_count = group['count'].max()
			max_pics = group[group['count'] == max_count]
			# Ideally, we want to add any other info that might be in the submission summary
			for _, row in max_pics.iterrows():
				rows.append(
					{
						'player': row['player_name'],
						'username': name,
						'num_pics': n_pics,
						'usage': max_count,
						'geometry': row.geometry,
					}
				)
		else:
			idxmax = group['count'].idxmax()
			most_common = group.loc[idxmax]
			assert isinstance(most_common, Series), f'most_common is {type(most_common)}'
			rows.append(
				{
					'player': group['player_name'].iloc[0],
					'username': name,
					'num_pics': n_pics,
					'usage': most_common['count'],
					'geometry': most_common.geometry,
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

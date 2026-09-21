#!/usr/bin/env python3
"""Finds pics that are far away from pics by other users (pics that are close to another pic by the same user are not considered.)

This takes a while to run, maybe about 2 hours or so."""

import asyncio
import logging
from argparse import ArgumentParser
from pathlib import Path
from typing import TYPE_CHECKING

import geopandas
from shapely import Point
from tqdm.auto import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from travelpygame.util.distance import cartesian_product_distances
from travelpygame.util.io_utils import output_geodataframe

from lib.io_utils import load_or_fetch_submission_summary
from lib.settings import Settings

if TYPE_CHECKING:
	from pandas import Series
	from travelpygame.submission_data import SubmissionSummary


def get_unique_pics(subs: 'SubmissionSummary', threshold: int) -> geopandas.GeoDataFrame:
	gdf = geopandas.GeoDataFrame(subs.submissions, geometry='point', crs='wgs84')
	# TODO: Option to filter for active players only (last submitted something after a certain date)

	rows = []
	groupie = gdf.groupby('player', sort=False)
	with tqdm(groupie, total=groupie.ngroups, unit='player') as t:
		for name, group in t:
			assert isinstance(group, geopandas.GeoDataFrame), (
				f'group is {type(group)}, not GeoDataFrame'
			)
			t.set_postfix(name=name)
			n_pics = group.index.size
			if n_pics < threshold:
				continue

			others = gdf.drop(index=group.index)
			other_distances = cartesian_product_distances(group.geometry, others.geometry)

			for index, row in tqdm(group.iterrows(), total=n_pics, leave=False, unit='row'):
				point = row['point']
				row_distances: Series = other_distances.loc[index]  # ty:ignore[invalid-argument-type] #.loc should work with Hashable
				closest_index = row_distances.idxmin()
				distance = row_distances.loc[closest_index]  # ty: ignore[invalid-argument-type]
				closest_row: Series = gdf.loc[closest_index]  # ty: ignore[invalid-argument-type]
				closest = closest_row['point']
				assert isinstance(closest, Point), f'closest is {type(closest)}, expected Point'

				median = row_distances.median()

				rows.append(
					{
						'point': point,
						'player': name,
						'mean_dist_to_other': row_distances.mean(),
						'median_dist_to_other': median,
						'closest_distance': distance,
						'closest_lat': closest.y,
						'closest_lng': closest.x,
						'closest_user': closest_row['player'],
					}
				)
			del others, other_distances
	unique_pics = geopandas.GeoDataFrame(rows, geometry='point', crs='wgs84')
	return unique_pics.sort_values('closest_distance', ascending=False)


async def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument(
		'--output-path',
		type=Path,
		help='Where to save list of submissions, can be .csv or .geojson/.gpkg/etc',
	)
	argparser.add_argument(
		'--threshold',
		type=int,
		default=2,
		help='Only include pics from players who have submitted at least this amount of unique pics. Defaults to 2, setting it to 0 or lower is effectively disabling it',
	)
	args = argparser.parse_args()
	output_path: Path | None = args.output_path

	subs_path = Settings().submission_summary_path
	subs = await load_or_fetch_submission_summary(subs_path)
	# Do NOT even think about trying to use self_cartesian_product_distances(subs.geometry) to just get vectorized distances all at once. You will accomplish nothing except rendering your computer inoperable for 20 minutes while it runs out of memory and thrashes. Do it whatever the other way is.

	gdf = get_unique_pics(subs, args.threshold)
	gdf = gdf.sort_values('closest_distance', ascending=False)
	print(gdf)
	if output_path:
		# TODO: Also just have something that gets max(closest_distance) per player
		await asyncio.to_thread(output_geodataframe, gdf, output_path, index=False)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	with logging_redirect_tqdm():
		asyncio.run(main())

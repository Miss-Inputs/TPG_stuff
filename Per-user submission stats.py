#!/usr/bin/env python3
"""Gets stats for all players. This is stil a bit of a mess and this docstring isn't even very good. For now, the paths it outputs to are hardcoded and dump files into /tmp."""

import asyncio
import logging
from argparse import ArgumentParser, BooleanOptionalAction
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import geopandas
import pandas
import shapely
from tqdm.auto import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from travelpygame import PointSet, find_furthest_point, output_geodataframe
from travelpygame.tpg_api import get_session
from travelpygame.tpg_data import get_player_display_names
from travelpygame.util import (
	circular_mean_points,
	format_dataframe,
	format_point,
	geod_distance,
	get_geometry_antipode,
	wgs84_geod,
)

from lib.io_utils import load_or_fetch_point_sets


@dataclass
class HullInfo:
	hull: shapely.Polygon | None
	area: float
	perimeter: float


def get_concave_hull_info(point_set: PointSet):
	if point_set.count == 1:
		return HullInfo(None, 0, 0)
	if point_set.count == 2:
		point_1, point_2 = point_set.point_array
		assert isinstance(point_1, shapely.Point), type(point_1)
		assert isinstance(point_2, shapely.Point), type(point_1)
		distance = geod_distance(point_1, point_2)
		return HullInfo(None, distance, distance)
	hull = point_set.concave_hull
	area, perimeter = wgs84_geod.geometry_area_perimeter(hull)
	area = abs(area)
	if not isinstance(hull, shapely.Polygon):
		tqdm.write(f'Huh? Concave hull for {point_set.name} is a {type(hull)}, expected Polygon')
		hull = None

	return HullInfo(hull, area, perimeter)


def get_stats(
	point_sets: Collection[PointSet],
	player_names: Mapping[str, str],
	*,
	get_concave_hulls: bool,
	find_furthest: bool,
) -> pandas.DataFrame:
	data = {}
	with tqdm(point_sets, 'Calculating stats', unit='player') as t:
		for point_set in t:
			name = player_names.get(point_set.name, point_set.name)
			t.set_postfix(name=name)
			# TODO: These should all be parameters whether to calculate each particular stat or not
			# Using .estimate_utm_crs() seems like a good idea, but it causes infinite coordinates for some people who have travelled too much, so that's no good

			anticentroid = get_geometry_antipode(point_set.centroid)
			row: dict[str, Any] = {
				'count': point_set.count,
				'average_point': circular_mean_points(point_set.point_array),
				'centroid': point_set.centroid,
				'anticentroid': anticentroid,
			}
			if get_concave_hulls:
				concave_hull = get_concave_hull_info(point_set)
				row['concave_hull'] = concave_hull.hull
				row['concave_hull_area'] = concave_hull.area
				row['concave_hull_perimeter'] = concave_hull.perimeter
			if find_furthest:
				furthest_point, furthest_distance = find_furthest_point(
					point_set.point_array, anticentroid, max_iter=1000, use_tqdm=False
				)
				row['antipoint'] = furthest_point
				row['furthest_distance'] = furthest_distance

			data[name] = row
	df = pandas.DataFrame.from_dict(data, 'index')
	df = df.reset_index(names='name')

	sort_cols = (('furthest_distance', True), ('concave_hull_area', False), ('count', False))
	for sort_col, sort_ascending in sort_cols:
		if sort_col in df.columns:
			df = df.sort_values(sort_col, ascending=sort_ascending, ignore_index=True)
			break
	return df


async def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument(
		'path',
		nargs='?',
		type=Path,
		help='Path to load submissions from, if this is not specified will try the ALL_SUBS_PATH environment variable if set.',
	)
	argparser.add_argument(
		'--threshold',
		type=int,
		help='Only take into account players who have submitted at least this amount of pics.',
	)
	argparser.add_argument(
		'--find-furthest-points',
		action=BooleanOptionalAction,
		default=False,
		help='Find the furthest possible point on the planet for each player. Defaults to false.',
	)
	argparser.add_argument(
		'--find-concave-hulls',
		action=BooleanOptionalAction,
		default=True,
		help='Find the concave hulls and their area for each player. Defaults to true.',
	)

	argparser.add_argument(
		'--concave-hull-output-path', type=Path, help='Path to save concave hulls'
	)

	args = argparser.parse_args()
	path: Path | None = args.path

	threshold: int | None = args.threshold

	async with get_session() as sesh:
		# Maybe should use aliases, I dunno
		all_point_sets = await load_or_fetch_point_sets(path)
		player_names = await get_player_display_names(sesh)

	point_sets = [ps for ps in all_point_sets if threshold is None or ps.count >= threshold]

	stats = get_stats(
		point_sets,
		player_names,
		get_concave_hulls=args.find_concave_hulls,
		find_furthest=args.find_furthest_points,
	)

	print(
		format_dataframe(
			stats,
			distance_cols=('concave_hull_perimeter', 'furthest_distance'),
			point_cols=('centroid', 'anticentroid', 'average_point', 'antipoint'),
			area_cols='concave_hull_area',
		)
	)

	# I should make these paths configurable but I didn't and haven't, and should
	stats.to_csv('/tmp/stats.csv', index=False)

	if 'antipoint' in stats.columns:
		antipoint_stats = stats[['name', 'antipoint', 'furthest_distance', 'count']].sort_values(
			'furthest_distance'
		)
		antipoint_stats['antipoint'] = antipoint_stats['antipoint'].map(format_point)
		await asyncio.to_thread(antipoint_stats.to_csv, '/tmp/antipoint_stats.csv', index=False)
		antipoints = geopandas.GeoDataFrame(
			stats[['name', 'antipoint']], geometry='antipoint', crs='wgs84'
		)
		await asyncio.to_thread(antipoints.to_file, '/tmp/antipoints.geojson')

	geopandas.GeoDataFrame(
		stats[['name', 'average_point']], geometry='average_point', crs='wgs84'
	).to_file('/tmp/average_points.geojson')
	geopandas.GeoDataFrame(stats[['name', 'centroid']], geometry='centroid', crs='wgs84').to_file(
		'/tmp/centroids.geojson'
	)

	concave_hull_path: Path | None = args.concave_hull_output_path
	if concave_hull_path and args.find_concave_hulls:
		concave_hulls = geopandas.GeoDataFrame(
			stats[['name', 'concave_hull', 'concave_hull_area', 'concave_hull_perimeter']],
			geometry='concave_hull',
			crs='wgs84',
		)
		concave_hulls = concave_hulls.dropna()
		await asyncio.to_thread(output_geodataframe, concave_hulls, concave_hull_path)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	with logging_redirect_tqdm():
		asyncio.run(main())

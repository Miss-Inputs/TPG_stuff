#!/usr/bin/env python3
"""Gets stats for all players. This is stil a bit of a mess and this docstring isn't even very good. For now, the paths it outputs to are hardcoded and dump files into /tmp."""

import asyncio
import logging
from argparse import ArgumentParser, BooleanOptionalAction
from collections.abc import Collection, Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import geopandas
import pandas
import shapely
from tqdm.auto import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from travelpygame.point_set_stats import get_point_set_stats
from travelpygame.tpg_api import get_session
from travelpygame.tpg_data import PlayerName, get_player_display_names
from travelpygame.util import format_dataframe, format_point, geod_distance, wgs84_geod

from lib.io_utils import load_or_fetch_point_sets

if TYPE_CHECKING:
	from travelpygame.point_set import PointSet


@dataclass
class HullInfo:
	hull: shapely.Polygon | None
	area: float
	perimeter: float


def get_concave_hull_info(point_set: 'PointSet'):
	# TODO: Not used, figure out if we need to use it or if we get rid of it
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
	point_sets: Collection['PointSet'], player_names: Mapping[str, str], *, find_furthest: bool
) -> pandas.DataFrame:
	data: dict[PlayerName, dict[str, Any]] = {}
	with tqdm(point_sets, 'Calculating stats', unit='player') as t:
		for point_set in t:
			name = player_names.get(point_set.name, point_set.name)
			t.set_postfix(name=name)
			# TODO: A lot more parameters should be optional
			# Using .estimate_utm_crs() seems like a good idea, but it causes infinite coordinates for some people who have travelled too much, so that's no good
			# TODO: Some things are not in PointSetStats yet: anti-centroid (antipode of centroid), concave hull perimeter; if you care that much

			stats = get_point_set_stats(
				point_set,
				find_geomedian=False,
				find_antipoint=find_furthest,
				get_projected_centroid=False,
			)
			row: dict[str, Any] = {'count': point_set.count, **asdict(stats)}

			data[name] = row
	df = pandas.DataFrame.from_dict(data, 'index')
	df = df.reset_index(names='name')
	# These columns contain index labels, which are generic in this case so we don't want to look at that
	df = df.drop(columns=['antipoint_closest', 'closest_to_bbox_label'], errors='ignore')
	df = df.dropna(how='all', axis='columns')

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

	args = argparser.parse_args()
	path: Path | None = args.path

	threshold: int | None = args.threshold

	async with get_session() as sesh:
		# Maybe should use aliases, I dunno
		all_point_sets = await load_or_fetch_point_sets(path)
		player_names = await get_player_display_names(sesh)

	point_sets = [ps for ps in all_point_sets if threshold is None or ps.count >= threshold]

	stats = get_stats(point_sets, player_names, find_furthest=args.find_furthest_points)
	# I should make these paths configurable but I didn't and haven't, and should
	stats.to_csv('/tmp/stats.csv', index=False)
	# TODO: Yeah nah westmost/etc nw_most/etc need to be split up

	point_cols = (
		'circular_mean',
		'arithmetic_mean',
		'arithmetic_median',
		'closest_to_bbox',
		'raw_centroid',
		'centroid',
		'centre_of_extremes',
		'antipoint',
	)

	print(
		format_dataframe(
			stats,
			distance_cols=(
				'diagonal_dist',
				'total_distance_from_centroid',
				'max_dist_from_centroid',
				'antipoint_dist',
				'closest_to_bbox_dist',
			),
			point_cols=point_cols,
			area_cols=('bbox_area', 'convex_hull_area', 'concave_hull_area'),
		)
	)

	if 'antipoint' in stats.columns:
		antipoint_stats = stats[['name', 'antipoint', 'antipoint_dist', 'count']].sort_values(
			'antipoint_dist'
		)
		antipoint_stats['antipoint'] = antipoint_stats['antipoint'].map(format_point)
		await asyncio.to_thread(antipoint_stats.to_csv, '/tmp/antipoint_stats.csv', index=False)
		antipoints = geopandas.GeoDataFrame(
			stats[['name', 'antipoint']], geometry='antipoint', crs='wgs84'
		)
		await asyncio.to_thread(antipoints.to_file, '/tmp/antipoints.geojson')

	geopandas.GeoDataFrame(
		stats[['name', 'circular_mean']], geometry='circular_mean', crs='wgs84'
	).to_file('/tmp/average_points.geojson')
	if 'centroid' in stats.columns:
		geopandas.GeoDataFrame(
			stats[['name', 'centroid']], geometry='centroid', crs='wgs84'
		).to_file('/tmp/centroids.geojson')


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	with logging_redirect_tqdm():
		asyncio.run(main())

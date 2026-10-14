#!/usr/bin/env python3
"""Gets stats for all players. This is stil a bit of a mess and this docstring isn't even very good. For now, the paths it outputs to are hardcoded and dump files into /tmp."""

import asyncio
import logging
from argparse import ArgumentParser, BooleanOptionalAction
from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from geopandas import GeoDataFrame
from pandas import DataFrame, Series
from shapely import Point, Polygon
from shapely.geometry.base import BaseGeometry
from tqdm.auto import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from travelpygame.point_set import PointSet, get_visited_regions
from travelpygame.point_set_stats import get_point_set_stats
from travelpygame.util import (
	format_dataframe,
	format_points,
	geod_distance,
	maybe_set_index_name_col,
	output_dataframe,
	read_geodataframe,
	wgs84_geod,
)

from lib.io_utils import load_or_fetch_point_sets

if TYPE_CHECKING:
	from travelpygame.tpg_data import PlayerName


@dataclass
class HullInfo:
	hull: Polygon | None
	area: float
	perimeter: float


def get_concave_hull_info(point_set: 'PointSet') -> HullInfo:
	# TODO: Not used, figure out if we need to use it or if we get rid of it
	if point_set.count == 1:
		return HullInfo(None, 0, 0)
	if point_set.count == 2:
		point_1, point_2 = point_set.point_array
		assert isinstance(point_1, Point), type(point_1)
		assert isinstance(point_2, Point), type(point_1)
		distance = geod_distance(point_1, point_2)
		return HullInfo(None, distance, distance)
	hull = point_set.concave_hull
	area, perimeter = wgs84_geod.geometry_area_perimeter(hull)
	area = abs(area)
	if not isinstance(hull, Polygon):
		tqdm.write(f'Huh? Concave hull for {point_set.name} is a {type(hull)}, expected Polygon')
		hull = None

	return HullInfo(hull, area, perimeter)


def get_stats(
	point_sets: Collection['PointSet'], regions: GeoDataFrame | None, *, find_antipoints: bool
) -> DataFrame:
	data: dict[PlayerName, dict[str, Any]] = {}
	with tqdm(point_sets, 'Calculating stats', unit='player') as t:
		for point_set in t:
			t.set_postfix(name=point_set.name)

			most_submitted_index = point_set.gdf['count'].idxmax()
			most_submitted = point_set[most_submitted_index]
			most_submitted_count = point_set.gdf.loc[most_submitted_index, 'count']  # ty: ignore[invalid-argument-type]
			earliest_known = point_set.gdf['earliest_known'].min()
			latest_known = point_set.gdf['latest_known'].max()
			game_names = set(point_set.gdf['game_names'].explode())
			# TODO: A lot more parameters should be optional
			# Using .estimate_utm_crs() seems like a good idea, but it causes infinite coordinates for some people who have travelled too much, so that's no good
			# TODO: Some things are not in PointSetStats yet: anti-centroid (antipode of centroid), concave hull perimeter; if you care that much

			stats = get_point_set_stats(
				point_set,
				find_geomedian=False,
				find_antipoint=find_antipoints,
				get_projected_centroid=False,
			)
			# TODO: Use antipode of centroid instead if find_antipoints is False
			row: dict[str, Any] = {
				'# unique pics': point_set.count,
				'Most submitted': most_submitted,
				'Most submitted count': most_submitted_count,
				'Earliest known': earliest_known,
				'Latest known': latest_known,
				'# graticules': stats.num_graticles,
			}
			if regions is not None:
				visited_regions_counts = get_visited_regions(point_set, regions)
				visited_regions = {
					region for region, count in visited_regions_counts.items() if count
				}
				row['Region count'] = len(visited_regions)
			# other travel extent measurements
			row.update(stats.distance_extents)
			row.update(stats.area_extents)
			row['Latitude range'] = stats.latitude_range
			row['Longitude range'] = stats.longitude_range
			row['Distance to antipoint'] = stats.antipoint_dist
			# row['Distance of closest point to bbox corners'] = stats.closest_to_bbox_dist #nah
			# extremities
			# Remember that we have not given anyone any labels so we can only really use the points themselves
			# And if we are displaying said points, we might as well just not display the westmost/etc coordinate itself since we're alreadyh showing it as part of those points
			row['Max west'] = point_set[stats.westmost[0]]
			row['Max east'] = point_set[stats.eastmost[0]]
			row['Max north'] = point_set[stats.northmost[0]]
			row['Max south'] = point_set[stats.southmost[0]]
			row['Max northwest'] = point_set[stats.northwestmost]
			row['Max northeast'] = point_set[stats.northeastmost]
			row['Max southwest'] = point_set[stats.southwestmost]
			row['Max southeast'] = point_set[stats.southeastmost]
			# Do we want distance for max nw/ne/etc here?

			if stats.antipoint_closest is not None:
				row['Closest to antipoint'] = point_set[stats.antipoint_closest]
			# centres
			row.update(stats.centres)
			row.update(stats.antipoints)
			row['Main TPG?'] = 'World TPG' in game_names
			row['Known games'] = ', '.join(sorted(game_names))
			data[point_set.name] = row
	df = DataFrame.from_dict(data, 'index')
	df = df.reset_index(names='name')
	# These columns contain index labels, which are generic in this case so we don't want to look at that
	return df.dropna(how='all', axis='columns')


def _is_geom(col: Series) -> bool:
	return not col.empty and all(isinstance(item, BaseGeometry) for item in col)


def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	filter_args = argparser.add_argument_group('Filter args', 'Arguments to filter players')
	output_args = argparser.add_argument_group('Output args')
	argparser.add_argument(
		'path',
		nargs='?',
		type=Path,
		help='Path to load submissions from, if this is not specified will try the ALL_SUBS_PATH environment variable if set.',
	)
	filter_args.add_argument(
		'--threshold',
		type=int,
		help='Only take into account players who have submitted at least this amount of pics.',
	)
	filter_args.add_argument(
		'--min-date',
		'--date-threshold',
		type=datetime.fromisoformat,
		help='Only include players who have known submissions at least after this date (YYYY-MM-DD format only, no being weird allowed).',
	)
	argparser.add_argument(
		'--find-antipoints',
		'--find-furthest-points',
		action=BooleanOptionalAction,
		default=False,
		help='Find the furthest possible point on the planet for each player. Defaults to false due to computational complexity.',
	)
	argparser.add_argument(
		'--regions',
		'--region-path',
		type=Path,
		help='Optional file to load regions from (e.g. countries or subdivisions) as polygons, and count how many regions each player has been to',
	)
	output_args.add_argument('--output-path', type=Path, help='Path to output stats to (csv, etc)')
	output_args.add_argument(
		'--points-path',
		'--points-output-path',
		type=Path,
		help='Path to a folder where point related stats are output as GeoJSON. Will be created if it does not exist, but the files will be overwritten',
	)

	args = argparser.parse_args()
	path: Path | None = args.path
	threshold: int | None = args.threshold
	min_date: datetime | None = args.min_date
	regions_path: Path | None = args.regions
	find_antipoints: bool = args.find_antipoints
	output_path: Path | None = args.output_path
	points_path: Path | None = args.points_path

	if regions_path:
		regions = read_geodataframe(regions_path)
		regions, auto_regions_name_col = maybe_set_index_name_col(
			regions, log_context=regions_path.name
		)
		if auto_regions_name_col:
			print(f'Set {regions_path.name} index to {auto_regions_name_col}')
	else:
		regions = None

	point_sets = asyncio.run(load_or_fetch_point_sets(path, min_date, threshold))

	df = get_stats(point_sets, regions, find_antipoints=find_antipoints)
	geom_cols = [
		col_name for col_name, col in df.select_dtypes('object', 'str').items() if _is_geom(col)
	]
	dubious_cols = ['Diagonal distance of bounding box']
	"""Don't care for this being in the stats because it's a bit silly"""
	stats = df.drop(columns=dubious_cols)
	for geom_col in geom_cols:
		if geom_col in stats:
			stats[geom_col] = format_points(stats[geom_col])
	if output_path:
		output_dataframe(stats, output_path, index=False)

	print(
		format_dataframe(
			stats,
			distance_cols=(
				'Diagonal distance of bounding box',
				'Total distance from centroid',
				'Maximum distance from centroid',
				'Distance to antipoint',
			),
			area_cols=('Bounding box area', 'Convex hull area', 'Concave hull area'),
		)
	)

	if points_path:
		points_path.mkdir(exist_ok=True, parents=True)
		for geom_col in tqdm(geom_cols, desc='Outputting geometries', unit='column'):
			gdf = GeoDataFrame(df[['name', geom_col]], geometry=geom_col, crs='wgs84')
			# maybe we could include associated columns? Like "Most submitted" has "Most submitted count", "Antipoint" has "Distance to antipoint", etc
			gdf.to_file(points_path / f'{geom_col}.geojson')


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	with logging_redirect_tqdm():
		main()

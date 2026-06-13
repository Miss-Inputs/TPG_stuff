#!/usr/bin/env python3
"""Find the best pic for each point in a given set of polygons (e.g. a geofile containing countries) and the best and worst point of each polygon for your points, so you can use it to predict how well you might do in a particular TPG, for example."""
#TODO: WIP, put this in the readme when done

import asyncio
import logging
from argparse import ArgumentParser, BooleanOptionalAction
from collections.abc import Hashable
from operator import itemgetter
from pathlib import Path
from typing import TYPE_CHECKING

import pandas
from geopandas import GeoDataFrame
from shapely import MultiPolygon, Point, Polygon, prepare
from tqdm.auto import tqdm
from travelpygame.util import (
	format_dataframe,
	format_distance,
	maybe_set_index_name_col,
	output_dataframe,
	read_geodataframe,
)
from travelpygame.util.distance import get_point_to_polygon_distance

from lib.io_utils import load_point_set_from_path

if TYPE_CHECKING:
	from shapely.geometry.base import BaseGeometry
	from travelpygame.point_set import PointSet


def _get_best_pic_dest_point(dest: Point, dest_name: str, pics: 'PointSet', *, use_haversine: bool):
	closest_index, distance = pics.get_closest_index(dest, use_haversine=use_haversine)
	return {
		'name': dest_name,
		'closest': closest_index,
		'best_case': dest,
		'best_case_dist': distance,
	}


def _get_row(dest: 'BaseGeometry', dest_name: str, pics: 'PointSet', *, use_haversine: bool):
	if isinstance(dest, Point):
		return _get_best_pic_dest_point(dest, dest_name, pics, use_haversine=use_haversine)
	if not isinstance(dest, (Polygon, MultiPolygon)):
		raise TypeError(f'{dest_name} had unsupported geometry type {type(dest)}')
	prepare(dest)

	best_points: list[tuple[Hashable, Point, float]] = []
	worst_points: list[tuple[Point, float]] = []

	for index, point in pics.items():
		(best_point, best_dist), (worst_point, worst_dist) = get_point_to_polygon_distance(
			point, dest, None, use_haversine=use_haversine
		)
		best_points.append((index, best_point, best_dist))
		worst_points.append((worst_point, worst_dist))
		# TODO: You may need to do a very slow check of all distances to everywhere in worst_points for this to be completely accurate but I dunno

	closest_point, closest_dest, closest_dist = min(best_points, key=itemgetter(2))
	worst_point = max(worst_points, key=itemgetter(1))[0]
	closest_worst, worst_dist = pics.get_closest_index(worst_point, use_haversine=use_haversine)

	return {
		'name': dest_name,
		'closest': closest_point,
		'best_case': closest_dest,
		'best_case_dist': closest_dist,
		'worst_case_closest': closest_worst,
		'worst_case': worst_point,
		'worst_case_dist': worst_dist,
	}


def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument(
		'points', type=Path, help='Path of file with existing points to find the best ones out of'
	)
	argparser.add_argument(
		'targets',
		type=Path,
		help='Path of file containing geometries to find the best distances to',
	)
	argparser.add_argument(
		'out_path', type=Path, nargs='?', help='Output distances/best pics to a file, optionally'
	)
	# TODO: All the lat_col/lng_col arguments, for now just don't be weird, and have a normal lat and lng col
	argparser.add_argument(
		'--source-name-col',
		help='Column label in points with the name of each point, or autodetect (it will be okay to not have any such column)',
	)
	argparser.add_argument(
		'--target-name-col',
		help='Column label in targets with the name of each point, or autodetect (it will be okay to not have any such column)',
	)
	argparser.add_argument(
		'--threshold',
		type=float,
		help='Report on how often each pic is better than this distance (in km)',
	)
	argparser.add_argument(
		'--use-haversine',
		action=BooleanOptionalAction,
		help='Use haversine for distances, defaults to false',
		default=True,
	)
	args = argparser.parse_args()

	point_set_path: Path = args.points
	points_name_col = args.source_name_col
	point_set = asyncio.run(
		load_point_set_from_path(point_set_path, point_name_col=points_name_col)
	)

	target_name_col: str | None = args.target_name_col
	target_path: Path = args.targets
	dests = read_geodataframe(target_path)
	dests, _auto_dest_name_col = maybe_set_index_name_col(dests, target_name_col, target_path.name)
	if not dests.active_geometry_name:
		raise ValueError('no geometry in dests?')

	rows = []
	with tqdm(dests.iterrows(), 'Finding best pics', dests.index.size, unit='target') as t:
		for index, dest_row in t:
			name = str(index)
			t.set_postfix(target=name)
			# TODO: Fall back to a better name from like a representative point or something if auto_dest_name_col was not set
			dest = dest_row[dests.active_geometry_name]
			row = _get_row(dest, name, point_set, use_haversine=args.use_haversine)
			rows.append(row)

	df = pandas.DataFrame(rows)
	df = df.sort_values('best_case_dist')

	print(format_dataframe(df, ('best_case_dist', 'worst_case_dist'), ('best_case', 'worst_case')))

	combined = pandas.concat(
		(
			df[['name', 'closest', 'best_case', 'best_case_dist']].rename(
				columns={'best_case': 'target', 'best_case_dist': 'dist'}
			),
			df[['name', 'worst_case_closest', 'worst_case', 'worst_case_dist']]
			.rename(
				columns={
					'worst_case_closest': 'closest',
					'worst_case': 'target',
					'worst_case_dist': 'dist',
				}
			)
			.assign(name=df['name'] + '_worst_case'),
		)
	)
	counts = combined['closest'].value_counts()
	print('Number of times each pic was the closest:')
	print(counts.to_string(header=False, name=False))
	print('Average best case distance:', format_distance(df['best_case_dist'].mean()))
	print('Average worst case distance:', format_distance(df['worst_case_dist'].mean()))
	print('Average distance to either best or worst:', format_distance(combined['dist'].mean()))
	if args.threshold:
		threshold: float = args.threshold * 1_000
		counts = combined[combined['dist'] < threshold]['closest'].value_counts()
		print(f'Number of times each pic was below {format_distance(threshold)}:', counts)

	gdf = GeoDataFrame(combined, geometry='target', crs='wgs84')
	gdf.to_file('/tmp/fjzdfzxf.geojson')

	# TODO: Combine the worst and best cases, get the count of closest pics and average distance from that
	# TODO: Maybe could output some lines from pic to best

	if args.out_path:
		output_dataframe(df, args.out_path)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	main()

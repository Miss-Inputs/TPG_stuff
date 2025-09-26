#!/usr/bin/env python3
"""Given an existing set of points (photos that one has for submitting to TPG, etc), and a set of new points to be added (e.g. new destinations to consider travelling to), finds out what is the most optimal."""

import logging
from argparse import ZERO_OR_MORE, ArgumentParser
from pathlib import Path

import geopandas
from pandas import Index, RangeIndex
from shapely import Point
from tqdm.auto import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from travelpygame.new_pic_eval import find_if_new_pics_better, load_points_or_rounds
from travelpygame.util import (
	format_distance,
	format_point,
	geodataframe_to_csv,
	get_closest_point_index,
	load_points,
	try_set_index_name_col,
)


def get_distances(points: geopandas.GeoDataFrame, new_points: geopandas.GeoDataFrame):
	points_geom = points.geometry.to_numpy()

	rows = []
	with tqdm(new_points.geometry.items(), total=new_points.index.size) as t:
		for index, new_point in t:
			if not isinstance(new_point, Point):
				raise TypeError(
					f'new points contained {type(new_point)} at {index} instead of Point'
				)
			closest_index, distance = get_closest_point_index(new_point, points_geom)
			closest = points.index[closest_index]
			rows.append(
				{
					'new_point': index,
					'closest': closest,
					'distance': distance,
					'geometry': new_points.geometry.loc[index],  # pyright: ignore[reportArgumentType, reportCallIssue]]
				}
			)
	return geopandas.GeoDataFrame(rows, geometry='geometry', crs='wgs84').sort_values(
		'distance', ascending=False
	)


def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument(
		'existing_points', type=Path, help='Your existing set of points, as .csv/.ods/.geojson/etc'
	)
	argparser.add_argument(
		'new_points', type=Path, help='New points to evaluate, as .csv/.ods/.geojson/etc'
	)
	argparser.add_argument(
		'--threshold',
		type=float,
		help='Ignore new points that are <= threshold metres away from existing points',
	)
	argparser.add_argument(
		'--targets',
		nargs=ZERO_OR_MORE,
		type=Path,
		help='Detect which pics become your best pic from locations or rounds loaded from this path (geojson/csv/ods/etc or submission tracker kml/kmz)',
	)
	argparser.add_argument('--output-path', type=Path)
	# TODO: Option for new_points to just be one point
	# TODO: lat/lng/blah column name options
	args = argparser.parse_args()

	points = load_points(args.existing_points)
	new_points = load_points(args.new_points)

	points = try_set_index_name_col(points)
	new_points = try_set_index_name_col(new_points)

	distances = get_distances(points, new_points)
	if args.threshold:
		distances = distances[distances['distance'] > args.threshold]
	distances['distance'] = distances['distance'].map(format_distance)
	distances['point'] = distances['geometry'].map(format_point)
	print('Distances from existing points:')
	print(distances.drop(columns='geometry'))

	if args.output_path:
		geodataframe_to_csv(distances, args.output_path, index=False)
	if args.targets:
		test_points = load_points_or_rounds(args.targets)
		test_points = try_set_index_name_col(test_points)
		if isinstance(test_points.index, RangeIndex):
			test_points.index = Index(
				[
					format_point(geo) if isinstance(geo, Point) else str(geo)
					for geo in test_points.geometry
				]
			)
		results = find_if_new_pics_better(points, new_points, test_points, use_haversine=True)
		better = results[results['is_new_better']].copy()
		better['diff'] = better['current_distance'] - better['new_distance']
		better = better.sort_values('diff', ascending=False)
		# TODO: This should be saved to csv, it's just like, do I have a new output_path argument or what
		for index, row in better.iterrows():
			diff = row['current_distance'] - row['new_distance']
			new_pic = row['new_best']
			old_pic = row['current_best']
			print(
				f'{index} would be improved by {new_pic}, beating {old_pic} by {format_distance(diff)}'
			)

	# TODO: Rather than necessarily the best pic among the new pics, what we probably want to do instead is take each new pic at a time and find how often it becomes the better pic, and sum the distance diffs (the total distance saved by adding this new pic)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	with logging_redirect_tqdm():
		main()

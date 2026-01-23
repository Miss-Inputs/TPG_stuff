#!/usr/bin/env python3
"""Displays and outputs stats for each round, like the average centroid, etc."""

import asyncio
import logging
from argparse import ArgumentParser
from itertools import chain
from pathlib import Path

import numpy
import pandas
import pyproj
from aiohttp import ClientSession
from shapely import MultiPoint, Point
from travelpygame import load_rounds_async
from travelpygame.util import get_centroid, get_projected_crs, get_total_bounds

from lib.format_utils import describe_point

logger = logging.getLogger(Path(__file__).stem)


async def main() -> None:
	argparser = ArgumentParser(__doc__)
	argparser.add_argument(
		'path',
		type=Path,
		help='JSON file with rounds/submissions, expected to already have scores and distances',
	)
	argparser.add_argument(
		'--world-distance',
		type=float,
		help='Maximum distance in km to count a submission as not being an outlier, default 5000km',
		default=5000.0,
	)
	argparser.add_argument(
		'--projected-crs',
		'--metric-crs',
		help='Name of CRS to use for calculating the centroid, or autodetect',
	)
	args = argparser.parse_args()

	rounds = await load_rounds_async(args.path)
	world_distance: float = args.world_distance * 1_000
	all_rounds = [r.target for r in rounds]
	total_bounds = get_total_bounds(all_rounds)
	print(f'Total bounds of rounds: {total_bounds}')
	all_submissions = list(chain.from_iterable(r.submissions for r in rounds))
	all_submission_points = [s.point for s in all_submissions]
	print(f'Total bounds of submissions: {get_total_bounds(all_submission_points)}')

	if args.projected_crs:
		projected_crs = pyproj.CRS.from_user_input(args.projected_crs)
	else:
		projected_crs = get_projected_crs(total_bounds)
		if projected_crs:
			logger.info(
				'Projected CRS autodetected as %s %s', projected_crs.name, projected_crs.srs
			)
		else:
			logger.info(
				'Could not detect a projected CRS, falling back to World Equidistant Cylindrical'
			)
			projected_crs = pyproj.CRS.from_epsg(4087)

	rows = []
	async with ClientSession() as sesh:
		for r in rounds:
			if any(sub.distance is None for sub in r.submissions):
				raise ValueError('This has not been scored yet')

			distances = numpy.asarray([sub.distance for sub in r.submissions])
			avg_distance = numpy.mean(distances[distances <= world_distance])
			avg_distance_raw = numpy.mean(distances)

			points_raw = numpy.asarray(
				[Point(sub.longitude, sub.latitude) for sub in r.submissions]
			)
			points = points_raw[distances <= world_distance]

			centroid_raw = get_centroid(MultiPoint(points_raw), projected_crs)
			centroid = get_centroid(MultiPoint(points), projected_crs)

			rows.append(
				{
					'Round': r.name or str(r.number),
					'Number of submissions': len(r.submissions),
					'Number of submissions within threshold': (distances <= world_distance).sum(),
					'Average distance': avg_distance / 1_000,
					'Raw average distance': avg_distance_raw / 1_000,
					'Submission centroid lat': centroid.y,
					'Submission centroid lng': centroid.x,
					'Raw centroid lat': centroid_raw.y,
					'Raw centroid lng': centroid_raw.x,
					'Submission centroid': await describe_point(centroid, sesh),
				}
			)

	df = pandas.DataFrame(rows).set_index('Round')
	print(df)
	out_path = args.path.with_name(f'{args.path.stem} - Stats.csv')
	await asyncio.to_thread(df.to_csv, out_path)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	asyncio.run(main())

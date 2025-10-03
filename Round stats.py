#!/usr/bin/env python3
"""Displays and outputs stats for each round, like the average centroid, etc."""

import asyncio
from argparse import ArgumentParser
from pathlib import Path

import numpy
import pandas
from aiohttp import ClientSession
from shapely import MultiPoint, Point
from travelpygame import load_rounds_async
from travelpygame.util import get_centroid

from lib.format_utils import describe_point


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
	args = argparser.parse_args()

	rounds = await load_rounds_async(args.path)
	world_distance: float = args.world_distance

	rows = []
	async with ClientSession() as sesh:
		for r in rounds:
			if any(sub.distance is None for sub in r.submissions):
				raise ValueError('This has not been scored yet')

			distances = numpy.asarray([sub.distance for sub in r.submissions])
			distances /= 1_000
			avg_distance = numpy.mean(distances[distances <= world_distance])
			avg_distance_raw = numpy.mean(distances)

			points_raw = numpy.asarray(
				[Point(sub.longitude, sub.latitude) for sub in r.submissions]
			)
			points = points_raw[distances <= world_distance]

			centroid_raw = get_centroid(MultiPoint(points_raw))
			centroid = get_centroid(MultiPoint(points))

			rows.append(
				{
					'Round': r.name or str(r.number),
					'Number of submissions': len(r.submissions),
					'Number of submissions within threshold': (distances <= world_distance).sum(),
					'Average distance': avg_distance,
					'Raw average distance': avg_distance_raw,
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
	asyncio.run(main())

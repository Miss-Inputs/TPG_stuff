#!/usr/bin/env python3
"""Find the best pic for each point in a given set of points, so you can use it to predict how well you might do in a particular TPG"""

import asyncio
from argparse import ArgumentParser
from collections.abc import Hashable
from pathlib import Path
from typing import TYPE_CHECKING

import numpy
from tqdm.auto import tqdm

from lib.geo_utils import geod_distance_and_bearing, haversine_distance
from lib.io_utils import find_first_matching_column, geodataframe_to_csv, load_points_async

if TYPE_CHECKING:
	import geopandas
	import pandas


def get_best_pic(
	dest_row: 'pandas.Series',
	sources: 'geopandas.GeoDataFrame',
	source_name_col: Hashable = 'name',
	*,
	use_haversine: bool = True,
):
	n = sources.index.size
	source_lat = sources.geometry.y.to_numpy()
	source_lng = sources.geometry.x.to_numpy()
	dest_lat = numpy.repeat(dest_row.geometry.y, n)
	dest_lng = numpy.repeat(dest_row.geometry.x, n)
	distances = (
		haversine_distance(source_lat, source_lng, dest_lat, dest_lng)
		if use_haversine
		else geod_distance_and_bearing(source_lat, source_lng, dest_lat, dest_lng)[0]
	)
	shortest = numpy.argmin(distances)
	shortest_dist = distances[shortest]
	return sources[source_name_col].iloc[shortest], shortest_dist


async def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument('path1', type=Path)
	argparser.add_argument('path2', type=Path)
	argparser.add_argument('out_path', type=Path, nargs='?')
	# TODO: All the lat_col/lng_col arguments, for now just don't be weird, and have a normal lat and lng col
	# TODO: Also name col arguments would be useful here, for now we assume "name" is the one we want
	args = argparser.parse_args()

	sources = await load_points_async(args.path1)
	dests = await load_points_async(args.path2)
	source_name_col = find_first_matching_column(sources, ('name', 'desc', 'description'))

	best_pics = {}
	distances = {}
	for index, dest_row in tqdm(dests.iterrows(), total=dests.index.size):
		best_pics[index], distances[index] = get_best_pic(dest_row, sources, source_name_col)

	dests['best'] = best_pics
	dests['distance'] = distances
	dests = dests.sort_values('distance')

	print(dests)
	if args.out_path:
		if args.out_path.suffix[1:].lower() == 'csv':
			await asyncio.to_thread(geodataframe_to_csv, dests, args.out_path, index=False)
		else:
			await asyncio.to_thread(dests.to_file, args.out_path)


if __name__ == '__main__':
	asyncio.run(main())

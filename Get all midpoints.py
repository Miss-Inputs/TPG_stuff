#!/usr/bin/env python3
"""Find all the combinations of midpoints for you and your teammate in TPG"""

import asyncio
import itertools
from argparse import ArgumentParser
from pathlib import Path

import geopandas
import pandas
from tqdm.auto import tqdm

from lib.format_utils import format_point
from lib.geo_utils import get_midpoint
from lib.io_utils import load_points_async


def get_row_midpoint(row_1: pandas.Series, row_2: pandas.Series):
	point_1 = row_1.geometry
	point_2 = row_2.geometry
	midpoint = get_midpoint(point_1, point_2)

	desc_1 = row_1.get('desc', row_1.get('name', None))
	desc_2 = row_2.get('desc', row_2.get('name', None))
	desc = f'{format_point(point_1) if pandas.isna(desc_1) else desc_1} + {format_point(point_2) if pandas.isna(desc_2) else desc_2}'
	return {'geometry': midpoint, 'name': desc}


async def main() -> None:
	argparser = ArgumentParser()
	argparser.add_argument('path1', type=Path)
	argparser.add_argument('path2', type=Path)
	argparser.add_argument('out_path', type=Path, nargs='?')
	# TODO: All the lat_col/lng_col arguments, for now just don't be weird, and have a normal lat and lng col
	args = argparser.parse_args()

	gdf_1 = await load_points_async(args.path1)
	gdf_2 = await load_points_async(args.path2)

	data = []
	total = gdf_1.index.size * gdf_2.index.size
	for (_index1, row1), (_index2, row2) in tqdm(
		itertools.product(gdf_1.iterrows(), gdf_2.iterrows()), total=total
	):
		data.append(get_row_midpoint(row1, row2))

	gdf = geopandas.GeoDataFrame(data, crs='wgs84')
	print(gdf)
	if args.out_path:
		gdf.to_file(args.out_path)


if __name__ == '__main__':
	asyncio.run(main())

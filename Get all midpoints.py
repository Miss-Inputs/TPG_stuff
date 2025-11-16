#!/usr/bin/env python3
"""Find all the combinations of midpoints for you and your teammate in TPG"""

import asyncio
import itertools
from argparse import ArgumentParser
from contextlib import nullcontext
from pathlib import Path

import geopandas
import pandas
from aiohttp import ClientSession
from tqdm.auto import tqdm
from travelpygame.util import format_point, get_midpoint, load_points_async, output_geodataframe

from lib.format_utils import describe_point


async def get_row_midpoint(
	row_1: pandas.Series, row_2: pandas.Series, session: ClientSession | None
):
	point_1 = row_1.geometry
	point_2 = row_2.geometry
	midpoint = get_midpoint(point_1, point_2)

	desc_1 = row_1.get('desc', row_1.get('name', row_1.get('location', None)))
	desc_2 = row_2.get('desc', row_2.get('name', row_2.get('location', None)))
	if session:
		desc_1 = await describe_point(point_1, session) if pandas.isna(desc_1) else desc_1
		desc_2 = await describe_point(point_2, session) if pandas.isna(desc_2) else desc_2
	else:
		desc_1 = format_point(point_1) if pandas.isna(desc_1) else desc_1
		desc_2 = format_point(point_2) if pandas.isna(desc_2) else desc_2
	desc = f'{desc_1} + {desc_2}'
	return {'geometry': midpoint, 'name': desc}


async def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument('path1', type=Path)
	argparser.add_argument(
		'path2',
		nargs='?',
		type=Path,
		help='If not specified, get midpoints of points in path1 with other points',
	)
	argparser.add_argument('--out-path', type=Path)
	argparser.add_argument(
		'--reverse-geocode',
		action='store_true',
		help='Use reverse geocoding for the name of unnamed points',
	)
	# TODO: All the lat_col/lng_col arguments, for now just don't be weird, and have a normal lat and lng col, and use "desc" or "name" as the name column
	args = argparser.parse_args()

	gdf_1 = await load_points_async(args.path1)
	gdf_1 = gdf_1.drop_duplicates('geometry')
	if args.path2:
		gdf_2 = await load_points_async(args.path2)
		gdf_2 = gdf_2.drop_duplicates('geometry')
		total = gdf_1.index.size * gdf_2.index.size
		combinations = itertools.product(gdf_1.iterrows(), gdf_2.iterrows())
	else:
		combinations = tuple(itertools.combinations(gdf_1.iterrows(), 2))
		total = len(combinations)

	data = []
	async with ClientSession() if args.reverse_geocode else nullcontext() as session:
		for (_index1, row1), (_index2, row2) in tqdm(combinations, total=total):
			data.append(await get_row_midpoint(row1, row2, session))

	gdf = geopandas.GeoDataFrame(data, crs='wgs84')
	print(gdf)
	if args.out_path:
		await asyncio.to_thread(output_geodataframe, gdf, args.out_path, index=False)


if __name__ == '__main__':
	asyncio.run(main())

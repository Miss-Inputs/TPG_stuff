#!/usr/bin/env python3
"""Checks which regions you have visited and which you have remaining (or have not listed), out of a file with separate regions (countries, states, etc)."""

import asyncio
import logging
from argparse import ArgumentParser, BooleanOptionalAction
from collections import Counter
from pathlib import Path

from pandas import Series
from tqdm.contrib.logging import logging_redirect_tqdm
from travelpygame.point_set import get_visited_regions
from travelpygame.util import maybe_set_index_name_col, read_geodataframe_async

from lib.io_utils import load_point_set_from_arg


def print_counts(counter: Counter, *, print_most_visited: bool):
	counts = Series(counter)

	is_visited = counts > 0
	if not is_visited.any():
		print('You have not visited any of these regions!')
		return
	if print_most_visited:
		visited = counts[is_visited].sort_values(ascending=False)
		print('Most visited regions:')
		print(visited.head(10))

	if is_visited.all():
		print('You have visited every region! 🎉🎉🎉')
		return

	completion = is_visited.mean()
	print(f'Completion progress: {is_visited.sum()}/{counts.size} ({completion:%})')
	unvisited = counts.index[counts == 0]
	print('Not yet visited:')
	for region in unvisited.sort_values():
		print(region)


async def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	point_set_args = argparser.add_argument_group(
		'Point set arguments', 'Arguments to control how point_set is loaded'
	)
	region_args = argparser.add_argument_group(
		'Region arguments', 'Arguments for the list of regions'
	)

	point_set_args.add_argument(
		'point_set',
		help='Path to file (.csv, .ods, .xls, .xlsx, pickled DataFrame, GeoJSON, etc), or player:<player display name> or username:<player username>, which will load all the submissions for a particular player.',
	)
	region_args.add_argument(
		'regions',
		type=Path,
		help='Path to a file (GeoJSON, GeoPackage, etc) containing rows of polygons to check off.',
	)

	point_set_args.add_argument(
		'--lat-column',
		'--latitude-column',
		dest='lat_col',
		help='Force a specific column label for latitude, defaults to autodetected',
	)
	point_set_args.add_argument(
		'--lng-column',
		'--longitude-column',
		dest='lng_col',
		help='Force a specific column label for longitude, defaults to autodetected',
	)
	point_set_args.add_argument(
		'--unheadered',
		action='store_true',
		help='Explicitly treat csv/Excel as not having a header, otherwise autodetect (and default to yes header if unknown)',
	)
	point_set_args.add_argument(
		'--crs',
		default='wgs84',
		help='Coordinate reference system to use if point_set is .csv/.ods/etc, defaults to WGS84',
	)
	point_set_args.add_argument(
		'--name-col',
		help='Force a specific column label for the name of each point, otherwise autodetect',
	)
	point_set_args.add_argument(
		'--projected-crs',
		'--metric-crs',
		'--metres-crs',
		help='Projected coordinate reference system to use for some operations, autodetect (when needed) if not specified',
	)

	region_args.add_argument(
		'--region-name-col',
		help='Force a specific column label in regions to be used as the name column which identifies each region, if not specified then try and autodetect it. If autodetection fails, it will still work but each region will have a generic name instead.',
	)

	argparser.add_argument(
		'--print-most-visited',
		action=BooleanOptionalAction,
		default=True,
		help='Print your most visited regions (which regions appear most often in your point set). Defaults to true.',
	)

	args = argparser.parse_args()
	point_set = await load_point_set_from_arg(
		args.point_set,
		args.lat_col,
		args.lng_col,
		args.crs,
		args.name_col,
		args.projected_crs,
		force_unheadered=args.unheadered,
	)

	regions_path: Path = args.regions
	regions = await read_geodataframe_async(regions_path)
	region_name_col: str | None = args.region_name_col
	regions, auto_regions_name_col = maybe_set_index_name_col(
		regions, region_name_col, regions_path.name
	)
	if auto_regions_name_col:
		print(f'Set {regions_path.name} index to {auto_regions_name_col}')
	dest_crs = point_set.gdf.crs
	if regions.crs != dest_crs:
		if dest_crs:
			print(f'Reprojecting regions from {regions.crs} to point set CRS ({dest_crs})')
			regions = regions.set_crs(dest_crs)
		else:
			print('Point set had no CRS somehow, assuming WGS84 and reprojecting regions to that')
			regions = regions.set_crs('wgs84')

	region_counter = get_visited_regions(point_set, regions)
	print_counts(region_counter, print_most_visited=args.print_most_visited)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	with logging_redirect_tqdm():
		asyncio.run(main())

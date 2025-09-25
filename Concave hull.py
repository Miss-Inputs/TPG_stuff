#!/usr/bin/env python3
"""Simple script to get a convex hull, for visualizing the general area that your pics have"""

import asyncio
from argparse import ArgumentParser
from pathlib import Path

import pyproj
import shapely
from geopandas import GeoSeries
from travelpygame.util import format_area, format_distance, load_points_async


async def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument(
		'path',
		type=Path,
		help='Path to file (.csv, .ods, .xls, .xlsx, pickled DataFrame, GeoJSON, etc)',
	)
	argparser.add_argument('output_path', type=Path, help='Path to write a file with the results')

	argparser.add_argument(
		'--lat-column',
		'--latitude-column',
		dest='lat_col',
		help='Force a specific column label for latitude, defaults to autodetected',
	)
	argparser.add_argument(
		'--lng-column',
		'--longitude-column',
		dest='lng_col',
		help='Force a specific column label for latitude, defaults to autodetected',
	)
	argparser.add_argument(
		'--unheadered',
		action='store_true',
		help='Explicitly treat csv/Excel as not having a header, otherwise autodetect (and default to yes header if unknown)',
	)
	argparser.add_argument(
		'--crs', default='wgs84', help='Coordinate reference system to use, defaults to WGS84'
	)
	argparser.add_argument(
		'--convex',
		action='store_true',
		help='Create a convex hull instead (simpler computation, probably less useful/interesting)',
	)

	args = argparser.parse_args()
	gdf = await load_points_async(
		args.path,
		args.lat_col,
		args.lng_col,
		crs=args.crs,
		has_header=False if args.unheadered else None,
	)
	union = gdf.union_all()
	hull = shapely.convex_hull(union) if args.convex else shapely.concave_hull(union)
	s = GeoSeries([hull], crs=gdf.crs)
	await asyncio.to_thread(s.to_file, args.output_path)

	if s.crs and not s.crs.equals('wgs84'):
		s = s.to_crs('wgs84')
		hull = s.geometry[0]
	geod = pyproj.Geod(ellps='WGS84')
	area, perimeter = geod.geometry_area_perimeter(hull)
	print(f'Area: {format_area(abs(area))}')
	print(f'Perimeter: {format_distance(perimeter)}')


if __name__ == '__main__':
	asyncio.run(main())

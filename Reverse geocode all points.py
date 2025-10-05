#!/usr/bin/env python3

import asyncio
from argparse import ArgumentParser
from collections.abc import Hashable
from pathlib import Path
from typing import TYPE_CHECKING

from aiohttp import ClientSession
from shapely import Point
from tqdm.auto import tqdm
from travelpygame import load_points_async, output_geodataframe

from lib.reverse_geocode import reverse_geocode_address

if TYPE_CHECKING:
	import geopandas
	from shapely.geometry.base import BaseGeometry


async def _reverse_geocode_row(session: ClientSession, index: Hashable, geometry: 'BaseGeometry'):
	if not isinstance(geometry, Point):
		# Feels like it'd be a bit annoying to raise an error here so I'll just pretend it was a point all along
		geometry = geometry.representative_point()
	return index, await reverse_geocode_address(geometry.y, geometry.x, session)


async def reverse_geocode_all(gdf: 'geopandas.GeoDataFrame', column_name: Hashable = 'address'):
	async with ClientSession() as sesh:
		tasks = [
			asyncio.create_task(
				_reverse_geocode_row(sesh, index, geometry), name=f'reverse_geocode {index}'
			)
			for index, geometry in gdf.geometry.items()
		]
		results = dict(
			[
				await result
				for result in tqdm.as_completed(tasks, desc='Reverse geocoding', unit='row')
			]
		)
	gdf[column_name] = results
	return gdf


async def main() -> None:
	argparser = ArgumentParser()
	argparser.add_argument(
		'path',
		type=Path,
		help='Path to file (.csv, .ods, .xls, .xlsx, pickled DataFrame, GeoJSON, etc)',
	)
	argparser.add_argument(
		'--output-path', type=Path, help='Optional path to write a .csv file with the results'
	)
	argparser.add_argument(
		'--column-name',
		help='Name of column with the results, defaults to "address"',
		default='address',
	)

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

	args = argparser.parse_args()
	gdf = await load_points_async(
		args.path,
		args.lat_col,
		args.lng_col,
		crs=args.crs,
		has_header=False if args.unheadered else None,
	)
	await reverse_geocode_all(gdf, args.column_name)
	print(gdf)
	output_path: Path | None = args.output_path
	if output_path:
		await asyncio.to_thread(output_geodataframe, gdf, output_path, index=False)


if __name__ == '__main__':
	asyncio.run(main())

#!/usr/bin/env python3
"""Get the address of all points in a point set from Nominatim."""

import asyncio
from argparse import ArgumentParser, BooleanOptionalAction
from collections.abc import Hashable
from pathlib import Path
from typing import TYPE_CHECKING

from aiohttp import ClientSession
from shapely import Point
from tqdm.auto import tqdm
from travelpygame import output_geodataframe
from travelpygame.reverse_geocode import DEFAULT_ENDPOINT, reverse_geocode_address

from lib.io_utils import load_point_set_from_arg

if TYPE_CHECKING:
	import geopandas
	from shapely.geometry.base import BaseGeometry


async def _reverse_geocode_row(
	session: ClientSession,
	index: Hashable,
	geometry: 'BaseGeometry',
	endpoint: str | None = DEFAULT_ENDPOINT,
	language: str | None = 'en',
):
	if not isinstance(geometry, Point):
		# Feels like it'd be a bit annoying to raise an error here so I'll just pretend it was a point all along
		tqdm.write(
			f'Note: Index {index} was {type(geometry)}, not Point, converting with representative_point()'
		)
		geometry = geometry.representative_point()
	return index, await reverse_geocode_address(geometry.y, geometry.x, session, language, endpoint)


async def reverse_geocode_all(
	gdf: 'geopandas.GeoDataFrame',
	column_name: Hashable,
	endpoint: str | None = DEFAULT_ENDPOINT,
	language: str | None = 'en',
	*,
	parallel: bool,
):
	async with ClientSession(
		headers={'User-Agent': 'https://github.com/Miss-Inputs/TPG_stuff'}
	) as sesh:
		# TODO: Parallel handling should be improved (and also technically the name is wrong but swagever), have the number of connections manually specified instead. Although maybe we should just remove it
		if parallel:
			tasks = [
				asyncio.create_task(
					_reverse_geocode_row(sesh, index, geometry, endpoint, language),
					name=f'reverse_geocode {index}',
				)
				for index, geometry in gdf.geometry.items()
			]
			results = dict(
				[
					await result
					for result in tqdm.as_completed(tasks, desc='Reverse geocoding', unit='row')
				]
			)
		else:
			results = dict(
				[
					await _reverse_geocode_row(sesh, index, geometry, endpoint, language)
					for index, geometry in gdf.geometry.items()
				]
			)
	gdf[column_name] = results
	return gdf


async def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	point_set_args = argparser.add_argument_group('Point set arguments')
	output_args = argparser.add_argument_group('Output arguments')
	web_args = argparser.add_argument_group('Web service arguments')

	point_set_args.add_argument(
		'point_set',
		help='Path to file (.csv, .ods, .xls, .xlsx, pickled DataFrame, GeoJSON, etc) or player:<name>/username:<username>.',
	)
	output_args.add_argument(
		'--output-path',
		type=Path,
		help='Optional path to write a file (.csv, .xlsx, etc) with the results',
	)
	output_args.add_argument(
		'--column-name',
		help='Name of column with the results, defaults to "address"',
		default='address',
	)

	web_args.add_argument('--endpoint', help='Use a different endpoint')
	web_args.add_argument(
		'--parallel',
		action=BooleanOptionalAction,
		default=False,
		help='Use asyncio as_completed to kinda sorta do it in parallel, defaults to false. Do not do this with the default endpoint!!!',
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
		help='Force a specific column label for latitude, defaults to autodetected',
	)
	point_set_args.add_argument(
		'--unheadered',
		action='store_true',
		help='Explicitly treat csv/Excel as not having a header, otherwise autodetect (and default to yes header if unknown)',
	)
	point_set_args.add_argument(
		'--crs', default='wgs84', help='Coordinate reference system to use, defaults to WGS84'
	)

	args = argparser.parse_args()
	output_path: Path | None = args.output_path

	point_set = await load_point_set_from_arg(
		args.point_set, args.lat_col, args.lng_col, args.crs, force_unheadered=args.unheadered
	)
	gdf = point_set.gdf
	await reverse_geocode_all(gdf, args.column_name, args.endpoint, parallel=args.parallel)
	print(gdf)
	if output_path:
		await asyncio.to_thread(output_geodataframe, gdf, output_path, index=False)


if __name__ == '__main__':
	asyncio.run(main())

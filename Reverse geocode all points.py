#!/usr/bin/env python3
"""Get the address of all points in a point set from Nominatim."""

import asyncio
from argparse import ArgumentParser
from collections.abc import Hashable
from pathlib import Path
from typing import TYPE_CHECKING

from aiohttp import ClientSession
from pandas import Series
from shapely import Point
from tqdm.auto import tqdm
from travelpygame import PointSet, output_geodataframe
from travelpygame.reverse_geocode import get_address_nominatim

from lib.io_utils import load_point_set_from_arg

if TYPE_CHECKING:
	from shapely.geometry.base import BaseGeometry


async def _get_row_address(
	session: ClientSession,
	index: Hashable,
	geometry: 'BaseGeometry',
	endpoint: str | None,
	language: str | None,
):
	if not isinstance(geometry, Point):
		# Feels like it'd be a bit annoying to raise an error here so I'll just pretend it was a point all along
		tqdm.write(
			f'Note: Index {index} was {type(geometry)}, not Point, converting with representative_point()'
		)
		geometry = geometry.representative_point()
	return index, await get_address_nominatim(geometry.y, geometry.x, session, language, endpoint)


async def get_addresses(
	point_set: PointSet, endpoint: str | None, language: str | None, *, parallel: bool
):
	user_agent = 'https://github.com/Miss-Inputs/TPG_stuff'
	if endpoint:
		user_agent = f'{user_agent} (user specified endpoint)'
	async with ClientSession(headers={'User-Agent': user_agent}) as sesh:
		# TODO: Parallel handling should be improved (and also technically the name is wrong because asyncio is concurrent instead but swagever), have the number of connections manually specified and use a semaphore etc instead. Although maybe we should just remove it, I've taken the option away from the argument parser for now
		if parallel:
			tasks = [
				asyncio.create_task(
					_get_row_address(sesh, index, geometry, endpoint, language),
					name=f'reverse_geocode {index}',
				)
				for index, geometry in point_set.points.items()
			]
			results = dict(
				[
					await result
					for result in tqdm.as_completed(tasks, desc='Reverse geocoding', unit='row')
				]
			)
		else:
			results = {}
			with tqdm(
				point_set.points.items(), 'Reverse geocoding', point_set.count, unit='row'
			) as t:
				for index, geometry in t:
					t.set_postfix(index=index)
					_index, address = await _get_row_address(
						sesh, index, geometry, endpoint, language
					)
					results[index] = address

	return Series(results)


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
		'--language',
		help='Set the language of the response, as a lowercase two-letter language code (defaults to en)',
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

	# TODO: We want an option here to reverse geocode locally from a geofile, using GADM or whatever else
	# TODO: Also an option to get the components

	args = argparser.parse_args()
	output_path: Path | None = args.output_path

	point_set = await load_point_set_from_arg(
		args.point_set, args.lat_col, args.lng_col, args.crs, force_unheadered=args.unheadered
	)
	addresses = await get_addresses(point_set, args.endpoint, args.language, parallel=False)
	print(addresses)
	gdf = point_set.gdf.copy()
	gdf[args.column_name] = addresses
	if output_path:
		await asyncio.to_thread(output_geodataframe, gdf, output_path, index=False)


if __name__ == '__main__':
	asyncio.run(main())

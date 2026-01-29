#!/usr/bin/env python3
"""Use a raster file containing DEM (digital elevation model) data to get the elevation for each point in a point set, and output that as a new file with a new column."""

import asyncio
import logging
from argparse import ArgumentParser, BooleanOptionalAction
from pathlib import Path

import pyproj
import rasterio
from numpy.ma import masked
from pandas import RangeIndex
from tqdm.auto import tqdm
from travelpygame import output_geodataframe

from lib.io_utils import load_point_set_from_arg


def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	point_set_args = argparser.add_argument_group('Point set arguments')
	dem_args = argparser.add_argument_group('Point set arguments')
	output_args = argparser.add_argument_group('Output arguments')

	point_set_args.add_argument(
		'point_set',
		help='Path to file (.csv, .ods, .xls, .xlsx, pickled DataFrame, GeoJSON, etc), or player:<player name>/username:<Discord username> to load known submissions for a certain player',
	)
	dem_args.add_argument(
		'dem_path', type=Path, help='Path to raster file (.tiff, etc) containing elevation data'
	)
	output_args.add_argument(
		'output_path', type=Path, nargs='?', help='Path to write a file with the results'
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

	dem_args.add_argument(
		'--band',
		'--raster-band',
		type=int,
		default=1,
		help='Which band of the raster to use, defaults to 1',
	)

	output_args.add_argument(
		'--dropna',
		'--skip-no-elevation',
		action=BooleanOptionalAction,
		default=False,
		help="Don't add rows to the output where elevation could not be found. Defaults to false",
	)
	output_args.add_argument(
		'--elevation-col-name',
		'--elevation-column-name',
		default='elevation',
		help='Name of the column in the output for the elevation. Defaults to "elevation".',
	)

	args = argparser.parse_args()
	point_set = asyncio.run(
		load_point_set_from_arg(
			args.point_set, args.lat_col, args.lng_col, args.crs, force_unheadered=args.unheadered
		)
	)
	gdf = point_set.gdf.copy()
	data = {}
	with rasterio.open(args.dem_path) as dem:
		print('Bands:', dem.count)
		dem_crs = pyproj.CRS(dem.crs)
		print('CRS:', dem_crs)
		print('Upper left:', dem.transform * (0, 0))
		print('Lower right:', dem.transform * (dem.width, dem.height))

		coords = point_set.coord_array
		data = [
			None if values[0] is masked else values[0]
			for values in dem.sample(
				tqdm(coords, 'Sampling DEM for coordinates', unit='point'), args.band, masked=True
			)
		]
	col_name: str = args.elevation_col_name
	gdf[col_name] = data
	if args.dropna:
		gdf = gdf.dropna(subset=col_name)
	if args.output_path:
		output_geodataframe(gdf, args.output_path, index=not isinstance(gdf.index, RangeIndex))
	print(gdf)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	main()

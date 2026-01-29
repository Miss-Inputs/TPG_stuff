#!/usr/bin/env python3
"""Use a raster file containing DEM (digital elevation model) data to get the elevation for each point in a point set, and output that as a new file with a new column."""

import logging
from argparse import ArgumentParser
from pathlib import Path

import pyproj
import rasterio
import shapely
from numpy.ma import masked
from tqdm.auto import tqdm
from travelpygame import load_points, output_geodataframe


def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument(
		'path',
		type=Path,
		help='Path to file (.csv, .ods, .xls, .xlsx, pickled DataFrame, GeoJSON, etc)',
	)
	argparser.add_argument('dem_path', type=Path, help='Path to raster file (.tiff, etc)')
	argparser.add_argument(
		'output_path', type=Path, nargs='?', help='Path to write a file with the results'
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
	# TODO: Arguments for which band of the raster to use, and what to name the elevation column, with defaults of 1 and "elevation" respectively

	args = argparser.parse_args()
	gdf = load_points(
		args.path,
		args.lat_col,
		args.lng_col,
		crs=args.crs,
		has_header=False if args.unheadered else None,
	)
	data = {}
	with rasterio.open(args.dem_path) as dem:
		print('Bands:', dem.count)
		dem_crs = pyproj.CRS(dem.crs)
		print('CRS:', dem_crs)
		print('Upper left:', dem.transform * (0, 0))
		print('Lower right:', dem.transform * (dem.width, dem.height))

		coords = shapely.get_coordinates(gdf.geometry)
		data = [
			None if values[0] is masked else values[0]
			for values in dem.sample(tqdm(coords), masked=True)
		]
	gdf['elevation'] = data
	if args.output_path:
		output_geodataframe(gdf, args.output_path, index=False)
	print(gdf)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	main()

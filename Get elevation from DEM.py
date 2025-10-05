#!/usr/bin/env python3
import logging
from argparse import ArgumentParser
from pathlib import Path

import rasterio
import shapely
from numpy.ma import masked
from tqdm.auto import tqdm
from travelpygame import load_points, output_geodataframe


def main() -> None:
	argparser = ArgumentParser()
	argparser.add_argument(
		'path',
		type=Path,
		help='Path to file (.csv, .ods, .xls, .xlsx, pickled DataFrame, GeoJSON, etc)',
	)
	argparser.add_argument('dem_path', type=Path, help='Path to raster file (.tiff, etc)')
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
		coords = {}
		for index, geom in gdf.geometry.items():
			if not isinstance(geom, shapely.Point):
				# TODO: Handle more gracefully
				raise TypeError(type(geom))
			coords[index] = (geom.x, geom.y)
		data = {
			index: None if values[0] is masked else values[0]
			for index, values in zip(
				coords.keys(), dem.sample(tqdm(coords.values()), masked=True), strict=True
			)
		}
	gdf['elevation'] = data
	output_geodataframe(gdf, args.output_path, index=False)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	main()

#!/usr/bin/env python3
"""Generate some stats for all the locations from a file. May need a better name.

Note that this is extremely under construction."""

import asyncio
from argparse import ArgumentParser
from pathlib import Path

import geopandas
import shapely

from lib.format_utils import format_distance, format_point
from lib.geo_utils import find_furthest_point_via_optimization, get_antipodes, get_centroid
from lib.io_utils import load_points_async


async def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument(
		'path',
		type=Path,
		help='Path to file (.csv, .ods, .xls, .xlsx, pickled DataFrame, GeoJSON, etc)',
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

	geo = gdf.geometry
	# For now this is just trying to find the furthest possible point. If you are reading this, I got impatient and committed and pushed this too early
	points = [g for g in geo if isinstance(g, shapely.Point)]

	x = geo.x.to_numpy()
	y = geo.y.to_numpy()

	antipode_lat, antipode_lng = get_antipodes(y, x)
	antipoints = shapely.points(antipode_lng, antipode_lat)
	assert not isinstance(antipoints, shapely.Point)
	antipoints_mp = shapely.MultiPoint(antipoints)
	antihull = shapely.concave_hull(antipoints_mp)
	assert isinstance(antihull, shapely.Polygon)
	geopandas.GeoSeries([antihull], crs='wgs84').to_file('/tmp/antihull.geojson')

	point, dist = find_furthest_point_via_optimization(points, get_centroid(antihull))
	print(format_point(point), format_distance(dist))


if __name__ == '__main__':
	asyncio.run(main())

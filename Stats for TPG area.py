#!/usr/bin/env python3
import asyncio
from argparse import ArgumentParser
from pathlib import Path

import pyproj
import shapely
from shapely import MultiPolygon, Polygon

from lib.format_utils import format_area, format_distance, format_point
from lib.io_utils import read_geodataframe_async
from lib.stats import get_longest_distance


async def main() -> None:
	argparser = ArgumentParser()
	argparser.add_argument('path', type=Path, help='Path to geojson/gpkg/etc file')
	argparser.add_argument(
		'--category',
		nargs='*',
		help='Column name containing stats to print a breakdown per area, otherwise attempts to autodetect',
	)
	argparser.add_argument(
		'--metres-crs',
		help='Override CRS used for area computations, should be something with metres as the unit',
	)
	args = argparser.parse_args()

	path = args.path
	gdf = await read_geodataframe_async(path)
	crs = pyproj.CRS(args.metres_crs) if args.metres_crs else gdf.estimate_utm_crs()
	cat_cols: list[str] = args.category

	if not cat_cols:
		nunique = gdf.drop(columns='geometry').nunique()
		maybe_cats = nunique[nunique < (gdf.index.size // 2)]
		cat_cols = maybe_cats.index.to_list()

	centroid = gdf.union_all().centroid
	print('Centroid:', format_point(centroid))

	metres = gdf.to_crs(crs)
	metres['area'] = metres.area
	total_area = metres['area'].sum()
	print('Total area:', format_area(total_area))

	for cat_col in cat_cols:
		areas = (
			metres.groupby(cat_col, sort=False)['area']
			.sum()
			.sort_values(ascending=False)
			.to_frame()
		)
		areas['percent'] = (areas / total_area).map('{:%}'.format)
		areas['area'] = areas['area'].map(format_area)
		print(areas)

	coverage_valid = metres.is_valid_coverage()
	if not coverage_valid:
		print(
			'Does not form a valid coverage, which might be fine but might have unexpected results'
		)
		# To be honest, is_valid_coverage is very strict, not a lot actually does have a complete lack of gaps apparently

	union = metres.union_all('coverage' if coverage_valid else 'unary')
	if not isinstance(union, (Polygon, MultiPolygon)):
		raise TypeError(type(union))
	bounds = union.envelope
	print('Bounding box size:', format_area(bounds.area))
	min_x, min_y, max_x, max_y = union.bounds
	# almost certainly a better way to do that by reusing bounds but whatever
	print(
		'Longest distance inside bounding box:',
		format_distance(shapely.distance(shapely.Point(min_x, min_y), shapely.Point(max_x, max_y))),
	)
	convex_hull = union.convex_hull
	assert isinstance(convex_hull, (Polygon, MultiPolygon)), type(convex_hull)
	print('Convex hull size:', format_area(convex_hull.area))
	max_convex_dist = get_longest_distance(convex_hull)
	print('Longest distance inside convex hull:', format_distance(max_convex_dist))



if __name__ == '__main__':
	asyncio.run(main(), debug=False)

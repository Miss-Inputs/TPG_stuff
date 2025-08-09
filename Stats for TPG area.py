#!/usr/bin/env python3
import asyncio
from argparse import ArgumentParser
from pathlib import Path

import pyproj
import shapely
from shapely import MultiPolygon, Polygon, ops

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
	crs_to_wgs84 = pyproj.Transformer.from_crs(crs, 'wgs84', always_xy=True)
	cat_cols: list[str] = args.category

	if not cat_cols:
		nunique = gdf.drop(columns='geometry').nunique()
		maybe_cats = nunique[nunique < (gdf.index.size // 2)]
		cat_cols = maybe_cats.index.to_list()

	invalid_reasons = gdf.is_valid_reason()
	invalid_reasons = invalid_reasons[invalid_reasons != 'Valid Geometry']
	if not invalid_reasons.empty:
		print('Invalid geometries:')
		print(invalid_reasons)

	metres = gdf.to_crs(crs)
	metres['area'] = metres.area
	total_area = metres['area'].sum()
	print('Total area:', format_area(total_area))
	invalid_reasons = metres.is_valid_reason()
	invalid_reasons = invalid_reasons[invalid_reasons != 'Valid Geometry']
	if not invalid_reasons.empty:
		print('Invalid geometries after converting to metres CRS:')
		print(invalid_reasons)
		# metres = metres.set_geometry(metres.make_valid())
		metres = metres.drop(index=invalid_reasons.index)

	for cat_col in cat_cols:
		grouper = metres.groupby(cat_col, sort=False)['area']
		areas = grouper.sum().sort_values(ascending=False).to_frame()
		areas['percent'] = (areas / total_area).map('{:%}'.format)
		areas['area'] = areas['area'].map(format_area)
		areas['count'] = grouper.size()
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
	centroid = union.centroid
	print('Centroid:', format_point(ops.transform(crs_to_wgs84.transform, centroid)))
	print(
		'Centroid snapped to area:',
		format_point(ops.transform(crs_to_wgs84.transform, ops.nearest_points(centroid, union)[1])),
	)
	rep_point = union.representative_point()
	print('Representative point:', format_point(ops.transform(crs_to_wgs84.transform, rep_point)))
	print(
		'Representative point snapped to area:',
		format_point(
			ops.transform(crs_to_wgs84.transform, ops.nearest_points(rep_point, union)[1])
		),
	)
	max_inscribed_circle = shapely.maximum_inscribed_circle(union)
	pole_of_inaccessibility = shapely.get_point(max_inscribed_circle, 0)
	print(
		'Pole of inaccessibility, probably wrong/meaningless if multipolygon:',
		format_point(ops.transform(crs_to_wgs84.transform, pole_of_inaccessibility)),
	)
	print(
		'Pole of inaccessibility snapped to area:',
		format_point(
			ops.transform(
				crs_to_wgs84.transform, ops.nearest_points(pole_of_inaccessibility, union)[1]
			)
		),
	)
	min_bounding_circle = shapely.minimum_bounding_circle(union)
	min_circle_centroid = min_bounding_circle.centroid
	print(
		'Minimum bounding circle centroid (minimized maximum distance to random points):',
		format_point(ops.transform(crs_to_wgs84.transform, min_circle_centroid)),
	)
	print(
		'Minimum bounding circle centroid snapped to area:',
		format_point(
			ops.transform(crs_to_wgs84.transform, ops.nearest_points(min_circle_centroid, union)[1])
		),
	)

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
	convex_centroid = convex_hull.centroid
	print(
		'Convex hull centroid:',
		format_point(ops.transform(crs_to_wgs84.transform, convex_centroid)),
	)


if __name__ == '__main__':
	asyncio.run(main(), debug=False)

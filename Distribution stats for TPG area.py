#!/usr/bin/env python3

from argparse import ArgumentParser, BooleanOptionalAction
from datetime import timedelta
from pathlib import Path
from time import perf_counter

from travelpygame.util import format_area, get_area, read_geodataframe


def main() -> None:
	argparser = ArgumentParser()
	argparser.add_argument(
		'path', type=Path, help='Path to GeoJSON/.gpkg/etc file containing the TPG area'
	)
	argparser.add_argument(
		'regions', type=Path, help='Path to GeoJSON/.gpkg/etc file containing regions as polygons'
	)
	argparser.add_argument(
		'--name-col',
		help='Name of column in regions file to use, if not specified then try to autodetect',
	)
	argparser.add_argument(
		'--metres-crs',
		help='Override CRS used for area computations, should be something with metres as the unit',
	)
	argparser.add_argument(
		'--area-from-crs',
		action=BooleanOptionalAction,
		default=False,
		help='Use the metric CRS to calculate the area of polygons instead of the WGS84 geod, defaults to False',
	)

	args = argparser.parse_args()

	gdf = read_geodataframe(args.path)
	regions = read_geodataframe(args.regions)
	regions = regions.dropna(subset='geometry')

	if gdf.crs:
		if regions.crs != gdf.crs:
			regions = regions.to_crs(gdf.crs)
			print(f'Reprojected regions to gdf crs: {gdf.crs}')
	elif regions.crs:
		gdf = gdf.to_crs(regions.crs)
		print(f'Reprojected gdf to regions crs: {regions.crs}')
	else:
		gdf = gdf.set_crs('wgs84')
		regions = regions.set_crs('wgs84')

	name_col: str = args.name_col or regions.drop(columns='geometry').columns[0]
	if name_col in gdf.columns:
		regions = regions.rename(columns={name_col: f'regions_{name_col}'})
		name_col = f'regions_{name_col}'
	regions = regions[[name_col, 'geometry']]
	regions = regions.clip(tuple(gdf.total_bounds), keep_geom_type=True)  # pyright: ignore[reportArgumentType]
	print('Combining gdf with regions, this can take some time')
	time_started = perf_counter()
	gdf = gdf.overlay(regions, 'identity', keep_geom_type=True)
	print(f'Done in {timedelta(seconds=perf_counter() - time_started)}')

	metres_crs = args.metres_crs or gdf.estimate_utm_crs()
	metres = gdf.to_crs(metres_crs)

	gdf['area'] = metres['area'] = (
		metres.area if args.area_from_crs else gdf.geometry.map(get_area, na_action='ignore')
	)
	total_area = metres['area'].sum()

	area_by_region = metres.groupby(name_col, sort=False, dropna=False)['area'].sum()
	area_by_region = area_by_region.sort_values(ascending=False)
	area_by_region = area_by_region.to_frame()
	area_by_region['percent'] = (area_by_region['area'] / total_area).map('{:%}'.format)
	area_by_region['area'] = area_by_region['area'].map(format_area)
	print(area_by_region.to_string(max_colwidth=40))


if __name__ == '__main__':
	main()

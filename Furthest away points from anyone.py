#!/usr/bin/env python3
"""Find points that are as far away as possible from any submission by anyone."""

import asyncio
import logging
from argparse import ArgumentParser
from itertools import chain
from pathlib import Path
from typing import TYPE_CHECKING

import geopandas
import numpy
import pandas
from shapely import MultiPolygon, Polygon
from tqdm.auto import tqdm
from travelpygame import find_furthest_point, get_main_tpg_rounds_with_path, load_rounds
from travelpygame.util import format_distance, format_point, output_geodataframe, read_geodataframe

from lib.settings import Settings

if TYPE_CHECKING:
	import shapely


def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument(
		'path',
		nargs='?',
		type=Path,
		help='Path to load TPG data from, defaults to MAIN_TPG_DATA_PATH environment variable if set. If that is not set and this argument is not given, gets main TPG data.',
	)
	argparser.add_argument(
		'--threshold',
		type=float,
		help='Find points as much as this distance in km away from anyone else, defaults to 1000km',
		default=1_000,
	)
	argparser.add_argument(
		'--within-region',
		type=Path,
		help='Optionally specify that points must be within a polygon/multipolygon/etc loaded from this file.',
	)
	argparser.add_argument(
		'--output-path',
		type=Path,
		help='Where to save points, can be .geojson/.gpkg/etc (or csv etc if you really want)',
	)
	args = argparser.parse_args()
	path: Path | None = args.path
	if path:
		rounds = load_rounds(path)
	else:
		settings = Settings()
		rounds = asyncio.run(get_main_tpg_rounds_with_path(settings.main_tpg_data_path))
	threshold: float = args.threshold * 1_000
	if args.within_region:
		region_gdf = read_geodataframe(args.within_region)
		region_gdf = region_gdf.to_crs('wgs84')
		region = region_gdf.union_all()
		if not isinstance(region, (Polygon, MultiPolygon)):
			raise TypeError(f'Nope, {args.within_region} contained something else')
	else:
		region = None

	df = pandas.DataFrame(
		list(
			chain.from_iterable(
				(
					{'name': sub.name, 'lat': sub.latitude, 'lng': sub.longitude}
					for sub in r.submissions
				)
				for r in rounds
			)
		)
	)
	df = df.drop_duplicates(subset=['lat', 'lng'])
	all_submissions = geopandas.GeoDataFrame(
		df.drop(columns=['lat', 'lng']),
		geometry=geopandas.points_from_xy(df['lng'], df['lat']),
		crs='wgs84',
	)
	print(all_submissions)

	found: list[shapely.Point] = []
	distances: list[float] = []
	all_points = all_submissions.geometry.to_numpy()
	with tqdm(desc='Finding points') as t:
		while True:
			points = numpy.append(all_points, found)  # pyright: ignore[reportArgumentType]
			point, distance = find_furthest_point(
				points, polygon=region, use_tqdm=False, use_haversine=True
			)
			if distance <= threshold:
				break
			t.update(1)
			t.set_postfix(point=format_point(point), distance=format_distance(distance))
			found.append(point)
			distances.append(distance)

	gdf = geopandas.GeoDataFrame(
		{'geometry': pandas.Series(found), 'distance': pandas.Series(distances)},
		geometry='geometry',
		crs='wgs84',
	)
	gdf = gdf.sort_values('distance', ascending=False)
	print(gdf)
	if args.output_path:
		output_geodataframe(gdf, args.output_path)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	main()

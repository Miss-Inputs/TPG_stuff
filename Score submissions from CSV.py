#!/usr/bin/env python3
"""Use export data -> CSV from hamburger menu on folder in submission tracker, assumes the first line is the target location
Expects columns: WKT, name, description
"""

from argparse import ArgumentParser
from pathlib import Path

import geopandas
import numpy
import pandas
from shapely import Point

from lib.geo_utils import geod_distance_and_bearing
from lib.tpg_utils import custom_tpg_score


def calc_scores(target: Point, gdf: geopandas.GeoDataFrame, world_distance: float=5000.0):
	n = gdf.index.size
	distance, bearing = geod_distance_and_bearing(gdf.geometry.y, gdf.geometry.x, numpy.repeat(target.y, n), numpy.repeat(target.x, n))
	gdf['distance'] = distance / 1000
	gdf['bearing'] = bearing
	
	scores = custom_tpg_score(gdf['distance'], world_distance)
	gdf['score'] = scores
	return gdf.sort_values('score', ascending=False)
	
def main() -> None:
	argparser = ArgumentParser()
	argparser.add_argument('csv', type=Path, help='Path to CSV file')
	#TODO: world_distance should be an argument too I guess, meh
	args = argparser.parse_args()

	path: Path = args.csv
	df = pandas.read_csv(path, on_bad_lines='warn')
	gs = geopandas.GeoSeries.from_wkt(df.pop('WKT'), crs='wgs84')
	gdf = geopandas.GeoDataFrame(df, geometry=gs)

	target = gdf.geometry.iloc[0]
	if not isinstance(target, Point):
		raise TypeError(f'uh oh target is {type(target)}')
	gdf = gdf.tail(-1)
	gdf = gdf.set_index('name', verify_integrity=True)
	gdf = calc_scores(target, gdf)

	print(gdf)
	out_path = path.with_stem(f'{path.stem} scores')
	gdf.to_csv(out_path)

if __name__ == '__main__':
	main()

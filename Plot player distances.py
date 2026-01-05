#!/usr/bin/env python3

import asyncio
import logging
from argparse import ArgumentParser, BooleanOptionalAction
from pathlib import Path

import contextily
import geopandas
from matplotlib import pyplot
from shapely import Point
from tqdm.auto import tqdm
from travelpygame import get_best_pic
from travelpygame.util import format_distance
from travelpygame.util.point_construction import get_fixed_grid

from lib.io_utils import load_point_set_from_arg

logger = logging.getLogger(Path(__file__).stem)


def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument(
		'points',
		help='Path to file (.csv, .ods, .xls, .xlsx, pickled DataFrame, GeoJSON, etc), or player:<player display name> or username:<player username>, which will load all the submissions for a particular player.',
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
		help='Force a specific column label for longitude, defaults to autodetected',
	)
	argparser.add_argument(
		'--unheadered',
		action='store_true',
		help='Explicitly treat csv/Excel as not having a header, otherwise autodetect (and default to yes header if unknown)',
	)
	argparser.add_argument(
		'--crs', default='wgs84', help='Coordinate reference system to use, defaults to WGS84'
	)
	argparser.add_argument(
		'--name-col',
		help='Force a specific column label for the name of each point, otherwise autodetect',
	)
	argparser.add_argument(
		'--resolution',
		type=float,
		default=1.0,
		help='Resolution of points to plot distances to, higher resolutions (smaller values) will be more computationally intensive. Defaults to 1 decimal degree.',
	)
	argparser.add_argument(
		'--limit-grid-to-bbox',
		action=BooleanOptionalAction,
		default=False,
		help="Limit the grid to the bounding box of the player's points, defaults to false",
	)
	# TODO: Option to limit grid to certain range
	argparser.add_argument(
		'--output-path',
		'--save',
		'--to-file',
		type=Path,
		dest='output_path',
		help='Location to save map as an image, instead of showing',
	)
	# TODO: Options for colour map, marker size, basemap provider, etc

	args = argparser.parse_args()
	point_set = asyncio.run(
		load_point_set_from_arg(
			args.points, args.lat_col, args.lng_col, args.crs, force_unheadered=args.unheadered
		)
	)

	resolution: float = args.resolution
	if args.limit_grid_to_bbox:
		min_x, min_y, max_x, max_y = point_set.gdf.total_bounds
	else:
		min_x = -179
		min_y = -89
		max_x = 179
		max_y = 89
	point_grid: geopandas.GeoSeries = get_fixed_grid(min_x, min_y, max_x, max_y, resolution)
	point_grid = point_grid.reset_index(drop=True)  # pyright: ignore[reportAssignmentType] #no mum you don't understand, it returns a Series and not a DataFrame
	# TODO: Boxes would be more ideal than points, but also more complicated

	distances = {}
	best_pics = {}
	for index, point in tqdm(
		point_grid.items(), 'Computing distances to points', total=point_grid.size, unit='point'
	):
		assert isinstance(point, Point), f'Why is point {index} a {type(point)} and not a Point'
		best_pic, distance = get_best_pic(point_set.point_array, point)
		distances[index] = distance
		best_pics[index] = best_pic

	gdf = geopandas.GeoDataFrame(
		{'point': point_grid, 'distance': distances, 'best_pic': best_pics},
		geometry='point',
		crs='wgs84',
	)

	# Not important, just felt like throwing in a fun fact
	print('Average distance to points:', format_distance(gdf['distance'].mean()))
	print('Max distance to points:', format_distance(gdf['distance'].max()))

	gdf['distance'] /= 1_000

	fig, ax = pyplot.subplots()
	gdf.plot(
		column='distance',
		legend=False,
		ax=ax,
		markersize=resolution**2,
		alpha=0.3,
		cmap='RdYlGn_r',
		vmin=0,
		vmax=20_037.5,
		legend_kwds={'shrink': 0.4},
	)

	contextily.add_basemap(ax, crs=gdf.crs, attribution=False)

	ax.set_axis_off()
	fig.tight_layout(pad=0)
	if args.output_path:
		fig.savefig(args.output_path, dpi=500, bbox_inches='tight')
	else:
		pyplot.show()


if __name__ == '__main__':
	main()

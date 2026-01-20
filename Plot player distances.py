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
from travelpygame import PointSet, get_best_pic
from travelpygame.util import format_distance
from travelpygame.util.point_construction import get_fixed_box_grid, get_fixed_grid

from lib.io_utils import load_point_set_from_arg


def get_grid(point_set: PointSet, resolution: float, *, use_boxes: bool, limit_to_bbox: bool):
	if limit_to_bbox:
		min_x, min_y, max_x, max_y = point_set.gdf.total_bounds
	else:
		min_x = -179
		min_y = -89
		max_x = 179
		max_y = 89
	crs = point_set.gdf.crs or 'wgs84'
	if use_boxes:
		return get_fixed_box_grid(min_x, min_y, max_x, max_y, resolution, crs)
	return get_fixed_grid(min_x, min_y, max_x, max_y, resolution, crs)


def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument(
		'point_set',
		help='Path to file (.csv, .ods, .xls, .xlsx, pickled DataFrame, GeoJSON, etc), or player:<player display name> or username:<player username>, which will load all the submissions for a particular player.',
	)
	point_set_args_group = argparser.add_argument_group(
		'Point set arguments', 'Arguments to control loading of point_set'
	)
	grid_args_group = argparser.add_argument_group(
		'Grid arguments', 'Arguments to control creation of the grid'
	)
	plot_args_group = argparser.add_argument_group(
		'Plotting arguments', 'Arguments to control how things are plotted'
	)

	point_set_args_group.add_argument(
		'--lat-column',
		'--latitude-column',
		dest='lat_col',
		help='Force a specific column label for latitude, defaults to autodetected',
	)
	point_set_args_group.add_argument(
		'--lng-column',
		'--longitude-column',
		dest='lng_col',
		help='Force a specific column label for longitude, defaults to autodetected',
	)
	point_set_args_group.add_argument(
		'--unheadered',
		action='store_true',
		help='Explicitly treat csv/Excel as not having a header, otherwise autodetect (and default to yes header if unknown)',
	)
	point_set_args_group.add_argument(
		'--crs', default='wgs84', help='Coordinate reference system to use, defaults to WGS84'
	)
	point_set_args_group.add_argument(
		'--name-col',
		help='Force a specific column label for the name of each point, otherwise autodetect',
	)

	grid_args_group.add_argument(
		'--resolution',
		type=float,
		default=1.0,
		help='Resolution of points/boxes to plot distances to, higher resolutions (smaller values) will be more computationally intensive. Defaults to 1 decimal degree.',
	)
	grid_args_group.add_argument(
		'--limit-grid-to-bbox',
		action=BooleanOptionalAction,
		default=False,
		help="Limit the grid to the bounding box of the player's points, defaults to false",
	)
	# TODO: Option to limit grid to certain range
	grid_args_group.add_argument(
		'--use-boxes',
		action=BooleanOptionalAction,
		default=True,
		help='Use boxes instead of points (and compute distances to an arbitrary point on the box which will usually end up being the middle), defaults to true.',
	)

	argparser.add_argument(
		'--use-haversine',
		action=BooleanOptionalAction,
		default=False,
		help='Use haversine distance instead of geodesic distance, defaults to false.',
	)
	plot_args_group.add_argument(
		'--cmap',
		'--colour-map',
		'--color-map',
		default='RdYlGn_r',
		help='Colour map, defaults to RdYlGn_r (smaller distances are green, further away distances are red).',
	)
	plot_args_group.add_argument(
		'--absolute-scale',
		action=BooleanOptionalAction,
		default=True,
		help='If true, set the limits of the colour map to 0km and the largest possible distance on Earth, otherwise let the colours be displayed with the largest distance to anywhere on the grid being the limit',
	)
	# TODO: If the grid is limited, this doesn't really make the most sense
	# TODO: Also the doco sucks kinda
	plot_args_group.add_argument(
		'--legend',
		action=BooleanOptionalAction,
		default=False,
		help='If true, enable a colour bar, defaults to false',
	)
	# TODO: Options for marker size (if not --use-boxes), basemap provider, alpha, etc

	argparser.add_argument(
		'--output-path',
		'--save',
		'--to-file',
		type=Path,
		dest='output_path',
		help='Location to save map as an image, instead of showing',
	)

	args = argparser.parse_args()
	point_set = asyncio.run(
		load_point_set_from_arg(
			args.point_set, args.lat_col, args.lng_col, args.crs, force_unheadered=args.unheadered
		)
	)

	use_boxes: bool = args.use_boxes
	resolution: float = args.resolution
	grid = get_grid(
		point_set, resolution, use_boxes=use_boxes, limit_to_bbox=args.limit_grid_to_bbox
	)

	distances = {}
	best_pics = {}
	with tqdm(
		grid.items(),
		'Computing distances to ' + ('boxes' if use_boxes else 'points'),
		total=grid.size,
		unit='box' if use_boxes else 'point',
	) as t:
		for index, geom in t:
			point = geom if isinstance(geom, Point) else geom.representative_point()
			# This seems wrong, since we already know it's a box and what it is and we should get the exact middle of the box, maybe? But representative_point seems to return that for rectangles already
			t.set_postfix(index=index, point=point)
			best_pic, distance = get_best_pic(point_set, point, use_haversine=args.use_haversine)
			distances[index] = distance
			best_pics[index] = best_pic

	gdf = geopandas.GeoDataFrame(
		{'geometry': grid, 'distance': distances, 'best_pic': best_pics}, crs='wgs84'
	)

	# Not important, just felt like throwing in a fun fact
	print('Average distance to points:', format_distance(gdf['distance'].mean()))
	print('Max distance to points:', format_distance(gdf['distance'].max()))

	# Just easier to work with kilometres here
	gdf['distance'] /= 1_000

	fig, ax = pyplot.subplots()
	vmin = 0
	vmax = 20_037.5 if args.absolute_scale else None
	legend_kwds = {'shrink': 0.4} if args.legend else None

	if use_boxes:
		gdf.plot(
			column='distance',
			legend=args.legend,
			ax=ax,
			alpha=0.3,
			cmap=args.cmap,
			vmin=vmin,
			vmax=vmax,
			legend_kwds=legend_kwds,
		)
	else:
		gdf.plot(
			column='distance',
			legend=args.legend,
			ax=ax,
			markersize=resolution**2,
			alpha=0.3,
			cmap=args.cmap,
			vmin=vmin,
			vmax=vmax,
			legend_kwds=legend_kwds,
		)

	contextily.add_basemap(ax, crs=gdf.crs, attribution=False)

	ax.set_axis_off()
	fig.tight_layout(pad=0)
	if args.output_path:
		fig.savefig(args.output_path, dpi=500, bbox_inches='tight')
	else:
		pyplot.show()


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	main()

#!/usr/bin/env python3
"""Plots a map that shows where one player is closer vs where the other is closer.
Note: This is probably not the correct algorithm to do this sort of thing, oh well.

"""

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
from travelpygame.util.point_construction import get_fixed_grid

from lib.io_utils import load_point_set_from_arg

logger = logging.getLogger(Path(__file__).stem)


async def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	left_player_opts = argparser.add_argument_group(
		'Left player options', 'Options for the "left" player.'
	)
	right_player_opts = argparser.add_argument_group(
		'Right player options', 'Options for the "right" player.'
	)
	left_player_opts.add_argument(
		'left_player',
		help='Path to file (.csv, .ods, .xls, .xlsx, pickled DataFrame, GeoJSON, etc), or player:<player display name> or username:<player username>, which will load all the submissions for a particular player.',
	)
	left_player_opts.add_argument(
		'--left-name-col',
		help='Force a specific column label for the name of each point, otherwise autodetect',
	)
	left_player_opts.add_argument(
		'--left-colour',
		help='Colour to plot where the left player is closer, defaults to red for no particular reason.',
		default='red',
	)
	right_player_opts.add_argument(
		'right_player',
		help='Path to file (.csv, .ods, .xls, .xlsx, pickled DataFrame, GeoJSON, etc), or or player:<player display name> or username:<player username>, which will load all the submissions for a particular player.',
	)
	right_player_opts.add_argument(
		'--right-name-col',
		help='Force a specific column label for the name of each point, otherwise autodetect',
	)
	right_player_opts.add_argument(
		'--right-colour',
		help='Colour to plot where the left player is closer, defaults to blue for no particular reason.',
		default='blue',
	)

	# TODO: --lat-column/--lng-column/--unheadered/--crs options for each player but I can't be arsed making them for both

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
	argparser.add_argument(
		'--marker-size',
		type=float,
		default=1.0,
		help='Size of dots on map, defaults to 1.0, you may want to fiddle with this so they overlap enough to not look like dots',
	)
	# TODO: Options for colour map, basemap provider, etc

	args = argparser.parse_args()

	left_player = await load_point_set_from_arg(args.left_player, name_col=args.left_name_col)
	right_player = await load_point_set_from_arg(args.right_player, name_col=args.right_name_col)

	resolution: float = args.resolution
	if args.limit_grid_to_bbox:
		left_minx, left_miny, left_maxx, left_maxy = left_player.gdf.total_bounds
		right_minx, right_miny, right_maxx, right_maxy = right_player.gdf.total_bounds
		min_x = min(left_minx, right_minx)
		min_y = min(left_miny, right_miny)
		max_x = max(left_maxx, right_maxx)
		max_y = max(left_maxy, right_maxy)
	else:
		# Having points at the exact edges gets screwy
		min_x = -179
		min_y = -89
		max_x = 179
		max_y = 89
	point_grid: geopandas.GeoSeries = get_fixed_grid(min_x, min_y, max_x, max_y, resolution)
	point_grid = point_grid.reset_index(drop=True)  # pyright: ignore[reportAssignmentType] #no mum you don't understand, it returns a Series and not a DataFrame, because of the drop=True
	# TODO: Boxes would be more ideal than points, but also more complicated

	left_best_pics = {}
	right_best_pics = {}
	colours = {}
	for index, point in tqdm(
		point_grid.items(), 'Computing distances to points', total=point_grid.size, unit='point'
	):
		assert isinstance(point, Point), f'Why is point {index} a {type(point)} and not a Point'
		left_best_pic, left_distance = get_best_pic(left_player.point_array, point)
		right_best_pic, right_distance = get_best_pic(right_player.point_array, point)
		# TODO: Handle ties (or at least log them) instead of giving right player the win, though for now that is very unlikely to happen
		colours[index] = args.left_colour if left_distance < right_distance else args.right_colour
		left_best_pics[index] = left_best_pic
		right_best_pics[index] = right_best_pic

	gdf = geopandas.GeoDataFrame(
		{
			'point': point_grid,
			'colour': colours,
			'left_best': left_best_pics,
			'right_best': right_best_pics,
		},
		geometry='point',
		crs='wgs84',
	)
	print(gdf)

	fig, ax = pyplot.subplots()
	gdf.plot(
		color=gdf['colour'],
		legend=False,
		ax=ax,
		markersize=args.marker_size,
		alpha=0.3,
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
	asyncio.run(main())

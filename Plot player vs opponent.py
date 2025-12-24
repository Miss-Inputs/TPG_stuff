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
import pandas
from matplotlib import pyplot
from shapely import Point
from tqdm.auto import tqdm
from travelpygame import get_best_pic, load_or_fetch_per_player_submissions, load_points
from travelpygame.util import format_point, try_set_index_name_col
from travelpygame.util.point_construction import get_fixed_grid

from lib.settings import Settings

logger = logging.getLogger(Path(__file__).stem)


async def load_player(path_or_name: str, name_col: str | None):
	if path_or_name.startswith('username:'):
		settings = Settings()
		all_subs = await load_or_fetch_per_player_submissions(
			settings.subs_per_player_path, settings.main_tpg_data_path
		)
		player_points = all_subs[path_or_name.removeprefix('username:')]
	else:
		player_points = load_points(path_or_name)
	assert player_points.crs, 'player_points had no crs, which should never happen'
	if not player_points.crs.is_geographic:
		logger.warning('player_points had non-geographic CRS %s, converting to WGS84')
		player_points = player_points.to_crs('wgs84')

	player_points = (
		player_points.set_index(name_col) if name_col else try_set_index_name_col(player_points)
	)
	if isinstance(player_points.index, pandas.RangeIndex):
		player_points.index = pandas.Index(player_points.geometry.map(format_point))
	return player_points


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
		help='Path to file (.csv, .ods, .xls, .xlsx, pickled DataFrame, GeoJSON, etc), or username:<player username>, which will load all the submissions for a particular player.',
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
		help='Path to file (.csv, .ods, .xls, .xlsx, pickled DataFrame, GeoJSON, etc), or username:<player username>, which will load all the submissions for a particular player.',
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

	left_player = await load_player(args.left_player, args.left_name_col)
	right_player = await load_player(args.right_player, args.right_name_col)

	resolution: float = args.resolution
	if args.limit_grid_to_bbox:
		left_minx, left_miny, left_maxx, left_maxy = left_player.total_bounds
		right_minx, right_miny, right_maxx, right_maxy = right_player.total_bounds
		min_x = min(left_minx, right_minx)
		min_y = min(left_miny, right_miny)
		max_x = max(left_maxx, right_maxx)
		max_y = max(left_maxy, right_maxy)
	else:
		min_x = -179
		min_y = -89
		max_x = 179
		max_y = 89
	point_grid: geopandas.GeoSeries = get_fixed_grid(min_x, min_y, max_x, max_y, resolution)
	point_grid = point_grid.reset_index(drop=True)  # pyright: ignore[reportAssignmentType] #no mum you don't understand, it returns a Series and not a DataFrame
	# TODO: Boxes would be more ideal than points, but also more complicated

	left_best_pics = {}
	right_best_pics = {}
	colours = {}
	for index, point in tqdm(
		point_grid.items(), 'Computing distances to points', total=point_grid.size, unit='point'
	):
		assert isinstance(point, Point), f'Why is point {index} a {type(point)} and not a Point'
		left_best_pic, left_distance = get_best_pic(left_player, point)
		right_best_pic, right_distance = get_best_pic(right_player, point)
		# TODO: Handle ties (or at least log them) instead of giving right player the win
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

#!/usr/bin/env python3
"""Plots a map that shows where one player is closer vs where the other is closer.
Note: This is probably not the correct algorithm to do this sort of thing, oh well.

"""

import asyncio
import logging
from argparse import ArgumentParser, BooleanOptionalAction
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import contextily
import geopandas
from matplotlib import pyplot
from shapely import Point, prepare
from tqdm.auto import tqdm
from travelpygame import PointSet, get_best_pic
from travelpygame.util import format_dataframe, output_dataframe
from travelpygame.util.point_construction import get_fixed_box_grid, get_fixed_grid

from lib.io_utils import load_point_set_from_arg

if TYPE_CHECKING:
	from pandas import Series
	from shapely.geometry.base import BaseGeometry

BboxType = tuple[float, float, float, float] | list[float] | Literal['max', 'min'] | None


def get_grid(
	left_player: PointSet,
	right_player: PointSet,
	resolution: float,
	limit_bbox: BboxType,
	*,
	use_boxes: bool,
):
	if isinstance(limit_bbox, (tuple, list)):
		min_x, min_y, max_x, max_y = limit_bbox
	elif limit_bbox is None:
		# Having points at the exact edges might be screwy
		min_x = -179
		min_y = -89
		max_x = 179
		max_y = 89
	else:
		left_minx, left_miny, left_maxx, left_maxy = left_player.gdf.total_bounds
		right_minx, right_miny, right_maxx, right_maxy = right_player.gdf.total_bounds
		min_x = min(left_minx, right_minx) if limit_bbox == 'max' else max(left_minx, right_minx)
		min_y = min(left_miny, right_miny) if limit_bbox == 'max' else max(left_miny, right_miny)
		max_x = max(left_maxx, right_maxx) if limit_bbox == 'max' else min(left_maxx, right_maxx)
		max_y = max(left_maxy, right_maxy) if limit_bbox == 'max' else min(left_maxy, right_maxy)
	# Assume crs is wgs84 for now
	return (
		get_fixed_box_grid(min_x, min_y, max_x, max_y, resolution)
		if use_boxes
		else get_fixed_grid(min_x, min_y, max_x, max_y, resolution)
	)


def get_first_index_inside(geom: 'BaseGeometry', point_set: PointSet):
	is_within = point_set.points.within(geom)
	if not is_within.any():
		return None
	within = point_set.points[is_within]
	return within.index[0]


def get_winner(
	geom: 'BaseGeometry', left_player: PointSet, right_player: PointSet
) -> tuple[Literal['left', 'right', 'tie'], Any, Any]:
	if not isinstance(geom, Point):
		prepare(geom)
		point = geom.representative_point()
		left_in_box = get_first_index_inside(geom, left_player)
		right_in_box = get_first_index_inside(geom, right_player)
		if left_in_box:
			if right_in_box:
				return 'tie', left_in_box, right_in_box
			return 'left', left_in_box, get_best_pic(right_player, point)
		if right_in_box:
			# and not left_in_box
			return 'right', right_in_box, get_best_pic(left_player, point)
	else:
		point = geom
	# TODO: Handle boxes where neither player has a point inside but they could win depending on what point of the box the target was (would need to think about that)
	left_best_pic, left_distance = get_best_pic(left_player, point)
	right_best_pic, right_distance = get_best_pic(right_player, point)
	if left_distance == right_distance:
		# Unlikely but might as well handle this case
		result = 'tie'
	elif left_distance < right_distance:
		result = 'left'
	else:
		result = 'right'
	return result, left_best_pic, right_best_pic


def print_win_stats(winner_col: 'Series', left_name: str, right_name: str):
	left_wins = (winner_col == left_name).sum()
	right_wins = (winner_col == right_name).sum()
	ties = winner_col.isna().sum()
	total = winner_col.size

	print(f'{left_name} wins: {left_wins} of {total} ({left_wins / total:%})')
	print(f'{right_name} wins: {right_wins} of {total} ({right_wins / total:%})')
	print(f'Ties: {ties} ({ties / total:%})')
	print(
		f'{left_name} wins or ties: {left_wins + ties} of {total} ({(left_wins + ties) / total:%})'
	)
	print(
		f'{right_name} wins or ties: {right_wins + ties} of {total} ({(right_wins + ties) / total:%})'
	)


async def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	left_player_arg_group = argparser.add_argument_group(
		'Left player options', 'Options for the "left" player.'
	)
	right_player_arg_group = argparser.add_argument_group(
		'Right player options', 'Options for the "right" player.'
	)
	grid_args_group = argparser.add_argument_group(
		'Grid arguments', 'Arguments to control creation of the grid'
	)
	plot_args_group = argparser.add_argument_group(
		'Plotting arguments', 'Arguments to control how things are plotted'
	)

	left_player_arg_group.add_argument(
		'left_player',
		help='Path to file (.csv, .ods, .xls, .xlsx, pickled DataFrame, GeoJSON, etc), or player:<player display name> or username:<player username>, which will load all the submissions for a particular player.',
	)
	left_player_arg_group.add_argument(
		'--left-name-col',
		help='Force a specific column label for the name of each point, otherwise autodetect',
	)
	left_player_arg_group.add_argument(
		'--left-colour',
		help='Colour to plot where the left player is closer, defaults to red for no particular reason.',
		default='red',
	)
	right_player_arg_group.add_argument(
		'right_player',
		help='Path to file (.csv, .ods, .xls, .xlsx, pickled DataFrame, GeoJSON, etc), or or player:<player display name> or username:<player username>, which will load all the submissions for a particular player.',
	)
	right_player_arg_group.add_argument(
		'--right-name-col',
		help='Force a specific column label for the name of each point, otherwise autodetect',
	)
	right_player_arg_group.add_argument(
		'--right-colour',
		help='Colour to plot where the left player is closer, defaults to blue for no particular reason.',
		default='blue',
	)

	# TODO: --lat-column/--lng-column/--unheadered/--crs options for each player but I can't be arsed making them for both, and also both players need to be the same crs or that would just be screwy

	grid_args_group.add_argument(
		'--use-boxes',
		action=BooleanOptionalAction,
		default=True,
		help='Use boxes instead of points (and compute distances to an arbitrary point on the box which will usually end up being the middle), defaults to true. If players have points inside a box, distances will not be computed, and whichever player has a point in the box will win that box (or if both they will tie).',
	)
	grid_args_group.add_argument(
		'--resolution',
		type=float,
		default=1.0,
		help='Resolution of points to plot distances to, higher resolutions (smaller values) will be more computationally intensive. Defaults to 1 decimal degree.',
	)
	grid_args_group.add_argument(
		'--bbox',
		help='If specified, limit the grid to a bounding box. Can be "max" for the combined bounds of both player\'s points (area that either player has), "min" for the shared bounds (area that both players have), or custom boundaries as a comma-separated list of floats (minx/miny/maxx/maxy). "min" may result in unexpected behaviour if both point sets are nowhere near each other',
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
		'--details-output-path', type=Path, help='Save details of who wins each round to this file'
	)
	plot_args_group.add_argument(
		'--marker-size',
		type=float,
		default=1.0,
		help='Size of dots on map, defaults to 1.0, you may want to fiddle with this so they overlap enough to not look like dots',
	)
	plot_args_group.add_argument(
		'--tie-colour',
		'--tie-color',
		default='yellow',
		help='Colour to use for boxes that both players have a point in, defaults to yellow.',
	)
	# TODO: Options for alpha, basemap provider, etc

	args = argparser.parse_args()

	left_player = await load_point_set_from_arg(args.left_player, name_col=args.left_name_col)
	right_player = await load_point_set_from_arg(args.right_player, name_col=args.right_name_col)

	use_boxes: bool = args.use_boxes
	bbox_arg: str | None = args.bbox
	bbox: BboxType = None
	if bbox_arg:
		bbox = (
			bbox_arg
			if bbox_arg in {'max', 'min'}
			else [float(part.strip()) for part in bbox_arg.split(',')]
		)  # pyright: ignore[reportAssignmentType] #why.

	grid = get_grid(left_player, right_player, args.resolution, bbox, use_boxes=use_boxes)

	left_best_pics = {}
	right_best_pics = {}
	colours = {}
	winners = {}

	with tqdm(
		grid.items(),
		'Computing distances to ' + ('boxes' if use_boxes else 'points'),
		total=grid.size,
		unit='box' if use_boxes else 'point',
	) as t:
		for index, geom in t:
			t.set_postfix(index=index, point=geom.representative_point())
			result, left_best_pic, right_best_pic = get_winner(geom, left_player, right_player)
			if result == 'left':
				colour = args.left_colour
				winner = left_player.name
			elif result == 'right':
				colour = args.right_colour
				winner = right_player.name
			else:
				colour = args.tie_colour
				winner = None
			colours[index] = colour
			winners[index] = winner
			left_best_pics[index] = left_best_pic
			right_best_pics[index] = right_best_pic

	gdf = geopandas.GeoDataFrame(
		{
			'geometry': grid,
			'colour': colours,
			'winner': winners,
			'left_best': left_best_pics,
			'right_best': right_best_pics,
		},
		crs='wgs84',
	)
	print_win_stats(gdf['winner'], left_player.name, right_player.name)

	details = gdf.drop(columns=['colour']).set_geometry(gdf.representative_point())
	details = format_dataframe(details, point_cols='geometry')
	print(details)
	if args.details_output_path:
		output_dataframe(details, args.details_output_path, index=False)

	fig, ax = pyplot.subplots()
	gdf.plot(
		color=gdf['colour'],
		legend=False,
		ax=ax,
		markersize=None if use_boxes else args.marker_size,
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
	logging.basicConfig(level=logging.INFO)
	asyncio.run(main())

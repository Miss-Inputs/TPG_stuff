"""Common reused arguments and handlers for them"""

import re
from argparse import ArgumentParser
from argparse import _ArgumentGroup as ArgumentGroup
from collections.abc import Sequence
from typing import TYPE_CHECKING

from shapely import Point
from travelpygame.point_set import PointSet

if TYPE_CHECKING:
	from geopandas import GeoDataFrame


def filter_gdf(
	gdf: 'GeoDataFrame',
	include_args: Sequence[Sequence[str]] | None = None,
	exclude_args: Sequence[Sequence[str]] | None = None,
) -> 'GeoDataFrame':
	"""Filters `gdf` to only rows matching certain pairs of field/values, or rows not matching.
	`gdf` does not necessarily need to contain points. The inner sequences in include_args and exclude_args should be length 2.
	"""
	if include_args:
		for field, value in include_args:
			gdf = gdf[gdf[field] == value]
	if exclude_args:
		for field, value in exclude_args:
			gdf = gdf[gdf[field] != value]
	return gdf


def filter_point_set(
	point_set: PointSet,
	include_args: Sequence[Sequence[str]] | None = None,
	exclude_args: Sequence[Sequence[str]] | None = None,
) -> PointSet:
	"""Filters `point_set` to only rows matching certain pairs of field/values, or rows not matching. Returns the original point_set unmodified if no filters are provided, or returns a new one that is filtered.
	The inner sequences in include_args and exclude_args should be length 2.
	"""
	if include_args or exclude_args:
		gdf = filter_gdf(point_set.gdf, include_args, exclude_args)
		point_set = PointSet(gdf, point_set.name, point_set.projected_crs)
	return point_set


def add_filter_args(argparser: ArgumentParser | ArgumentGroup, name: str | None = None):
	"""Adds two arguments --filter and --exclude to an argument parser or group.
	Arguments:
		name: What the thing you are filtering is called, if None, it will be "the input".
	"""
	# TODO: There might be a way to have custom ArgumentParser actions that result in this producing a list of 2-length tuples instead of a list of lists, which is then nicer to type hint and prevents mistakes better
	name = name or 'the input'
	argparser.add_argument(
		'--filter',
		action='append',
		nargs=2,
		metavar=('field', 'value'),
		help=f'Filters {name} to only rows matching a certain value, as (field name) == (value). Multiple pairs of field/value can be specified multiple times to restrict to rows matching _all_ filters',
	)
	argparser.add_argument(
		'--exclude',
		action='append',
		nargs=2,
		metavar=('field', 'value'),
		help=f'Filters {name} to only rows _not_ matching a certain value, as (field name) != (value). Multiple pairs of field/value can be specified multiple times to exclude rows matching any filter',
	)

comma_splitter = re.compile(r'[,\s/;]\s*')

def parse_coords(s: str) -> Point:
	lat_s, lng_s = comma_splitter.split( s, maxsplit=1)
	lat = float(lat_s)
	lng = float(lng_s)
	return Point(lng, lat)


def parse_box(s: str) -> tuple[float, float, float, float]:
	minx, miny, maxx, maxy = comma_splitter.split( s, maxsplit=3)
	return (float(minx), float(miny), float(maxx), float(maxy))

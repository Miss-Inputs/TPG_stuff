#!/usr/bin/env python3
"""Creates a map from a file containing regions showing where you have points in those regions."""

import asyncio
import logging
from argparse import ArgumentParser
from enum import Enum, auto
from pathlib import Path
from typing import TYPE_CHECKING

import contextily
import xyzservices
from matplotlib import pyplot
from tqdm.contrib.logging import logging_redirect_tqdm
from travelpygame.util import maybe_set_index_name_col, read_geodataframe

from lib.arg_utils import add_filter_args, filter_point_set
from lib.io_utils import load_point_set_from_arg
from lib.plotting import GoogleBasemap, get_google_map_provider

if TYPE_CHECKING:
	from geopandas import GeoDataFrame
	from matplotlib.figure import Figure
	from travelpygame.point_set import PointSet
	from xyzservices.lib import TileProvider


class CropStyle(Enum):
	"""Controls the way the output is cropped to only visited regions"""

	Crop = auto()
	WideCrop = auto()
	"""Hmm I didn't think of a better wording for this"""


def get_region_statuses(
	point_set: 'PointSet', regions: 'GeoDataFrame', transit_arg: list[list[str]]
) -> 'GeoDataFrame':
	points = point_set.gdf.copy()
	points['is_transit'] = False
	for transit_col, transit_val in transit_arg:
		points['is_transit'] |= points[transit_col] == transit_val

	joined = points.sjoin(regions[['geometry']], 'left', 'within')
	status = {}
	for index, group in joined.groupby('name_right', sort=False)['is_transit']:
		not_transit_count = (~group).sum()
		status[index] = 'Visited' if not_transit_count else 'Transited'

	regions['status'] = status
	# this leaves unvisited as None

	return regions.reset_index()[['name', 'status', 'geometry']]


def get_bbox(regions: 'GeoDataFrame', crop: CropStyle | None) -> tuple[float, float, float, float]:
	total_minx, total_miny, total_maxx, total_maxy = regions.total_bounds
	if not crop:
		return total_minx, total_miny, total_maxx, total_maxy
	visited = regions.dropna(subset='status')
	minx, miny, maxx, maxy = visited.total_bounds
	if crop == CropStyle.WideCrop:
		# TODO: This might bork on the antimeridian whoops, but I'll figure that out when it happens. If it just doesn't crop anything when the antimeridian is involved, then that's fine in my book
		visited_width = abs(maxx - minx)
		visited_height = abs(maxy - miny)
		minx -= visited_width / 2
		miny -= visited_height / 2
		maxx += visited_width / 2
		maxy += visited_height / 2

	return (
		max(total_minx, minx),
		max(total_miny, miny),
		min(total_maxx, maxx),
		min(total_maxy, maxy),
	)


def plot_map(
	regions: 'GeoDataFrame', crop: CropStyle | None, basemap: 'TileProvider | None'
) -> 'Figure':
	fig, ax = pyplot.subplots(figsize=(15, 15))
	if not basemap:
		fig.set_facecolor('lightblue')
	regions.plot(
		'status',
		ax=ax,
		legend=True,
		autolim=crop is None,
		cmap='Pastel1',
		alpha=0.9,
		# TODO: All this stuff should be options, but eh, I'll fiddle with that later
		# We also probably want to set cmap to a dict manually (falling back to a cmap for custom statuses, or maybe that's also an argument)
		edgecolor='black',  # ty: ignore[invalid-argument-type]
		linewidth=0.1,
		missing_kwds={'facecolor': 'none' if basemap else 'white', 'label': 'Unvisited'},
	)
	if crop:
		minx, miny, maxx, maxy = get_bbox(regions, crop)
		ax.set_xlim(minx, maxx)
		ax.set_ylim(miny, maxy)
	if basemap:
		# Not using the tiles= argument to plot() here so we can fiddle with the xlim/ylim first (in case of cropping), which makes it download at a different reso
		contextily.add_basemap(ax, 'auto', basemap, crs=regions.crs, attribution=False)

	ax.set_axis_off()
	fig.tight_layout(pad=0)
	return fig


def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	point_set_args = argparser.add_argument_group(
		'Point set arguments', 'Arguments to control how point_set is loaded'
	)
	region_args = argparser.add_argument_group(
		'Region arguments', 'Arguments for the list of regions'
	)
	plot_args = argparser.add_argument_group('Plot arguments')
	output_args = argparser.add_argument_group('Output args')

	point_set_args.add_argument(
		'point_set',
		help='Path to file (.csv, .ods, .xls, .xlsx, pickled DataFrame, GeoJSON, etc), or player:<player display name> or username:<player username>, which will load all the submissions for a particular player.',
	)
	region_args.add_argument(
		'regions',
		type=Path,
		help='Path to a file (GeoJSON, GeoPackage, etc) containing regions to make a map from.',
	)

	point_set_args.add_argument(
		'--lat-column',
		'--latitude-column',
		dest='lat_col',
		help='Force a specific column label for latitude, defaults to autodetected',
	)
	point_set_args.add_argument(
		'--lng-column',
		'--longitude-column',
		dest='lng_col',
		help='Force a specific column label for longitude, defaults to autodetected',
	)
	point_set_args.add_argument(
		'--unheadered',
		action='store_true',
		help='Explicitly treat csv/Excel as not having a header, otherwise autodetect (and default to yes header if unknown)',
	)
	point_set_args.add_argument(
		'--crs',
		default='wgs84',
		help='Coordinate reference system to use if point_set is .csv/.ods/etc, defaults to WGS84',
	)
	add_filter_args(point_set_args, 'the point set')
	point_set_args.add_argument(
		'--transited',
		action='append',
		nargs=2,
		help='Specify that a combination of column and value in the point set indicates that a photo is taken during transit, e.g. --transited transit yes or --transited type transit. Can be specified more than once. This causes regions where you have only transited through to be automatically marked as such',
	)

	region_args.add_argument(
		'--name-col',
		help='Force a specific column label in regions to be used as the name column which identifies each region, if not specified then try and autodetect it. If autodetection fails, it will still work but each region will have a generic name instead. This would only be needed for manually marking regions',
	)

	plot_args.add_argument(
		'--crop',
		action='store_const',
		dest='crop',
		const=CropStyle.Crop,
		help='Crop the map to only the bounding box of what is actually visited/otherwise marked',
	)
	plot_args.add_argument(
		'--wide-crop',
		action='store_const',
		dest='crop',
		const=CropStyle.WideCrop,
		help='Crop the map to the bounding box of what is actually visited/otherwise marked plus some padding',
	)
	plot_args.add_argument(
		'--basemap',
		default='Esri.WorldTopoMap',
		help='Use a particular tile provider for the basemap (as recognized by xzyservices, or "google" to use Google Maps), or "none" to not use a basemap. Defaults to Esri.WorldTopoMap',
	)

	output_args.add_argument(
		'--output-path',
		type=Path,
		help='Path to output the map to (optional). If this ends in .html it will output an interactive Leaflet map (requires folium and mapclassify to be installed in the environment)',
	)

	args = argparser.parse_args()
	point_set = asyncio.run(
		load_point_set_from_arg(
			args.point_set, args.lat_col, args.lng_col, args.crs, force_unheadered=args.unheadered
		)
	)
	point_set = filter_point_set(point_set, args.filter, args.exclude)

	regions_path: Path = args.regions
	regions = read_geodataframe(regions_path)
	region_name_col: str | None = args.name_col
	regions, auto_regions_name_col = maybe_set_index_name_col(
		regions, region_name_col, regions_path.name
	)
	if auto_regions_name_col:
		print(f'Set {regions_path.name} index to {auto_regions_name_col}')
	regions = regions.rename_axis(index='name')

	dest_crs = point_set.gdf.crs
	# TODO: Maybe it should just be set to WGS84 regardless
	if regions.crs != dest_crs:
		if dest_crs:
			print(f'Reprojecting regions from {regions.crs} to point set CRS ({dest_crs})')
			regions = regions.to_crs(dest_crs)
		else:
			print('Point set had no CRS somehow, assuming WGS84 and reprojecting regions to that')
			regions = regions.set_crs('wgs84')

	regions = get_region_statuses(point_set, regions, args.transited or [])
	print(regions)

	# TODO: Argument to manually mark some regions as having a specific status ("visited but no pics" or whatever)
	# TODO: Argument to specify the exact names of the status (for internationalization I guess, but also maybe someone wants to say some other word than "Visited")
	if args.basemap == 'none':
		basemap = None
	elif args.basemap == 'google':
		basemap = get_google_map_provider(GoogleBasemap.Standard)
	else:
		basemap = xyzservices.providers.query_name(args.basemap)

	output_path: Path | None = args.output_path
	if output_path and output_path.suffix[1:].lower() in {'html', 'htm'}:
		# Note that mapclassify is still required and imported even if we don't use it by providing our own colours instead…
		# The crop options wouldn't really be applicable here, I think, is that true
		# We can't actually set the opacity or anything like that in missing_kwds, so there's not really any way to make it look like we want, unless we add multiple layers but that's messy
		folium_map = regions.explore(
			'status', tiles=basemap, missing_kwds={'label': 'Unvisited', 'color': 'gray'}
		)
		folium_map.save(output_path)
	else:
		fig = plot_map(regions, args.crop, basemap)

		if output_path:
			pyplot.rcParams['svg.fonttype'] = 'none'
			fig.savefig(output_path, bbox_inches='tight')
		else:
			pyplot.show()


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	with logging_redirect_tqdm():
		main()

#!/usr/bin/env python3

from argparse import ArgumentParser
from pathlib import Path
from typing import TYPE_CHECKING

import contextily
import geopandas
from matplotlib import pyplot
from shapely import LineString, Point

from lib.kml import parse_submission_kml
from lib.plotting import GoogleBasemap, get_google_map_provider

if TYPE_CHECKING:
	from xyzservices import TileProvider


def get_points(path: Path, name: str | None):
	submission_tracker = parse_submission_kml(path)
	targets: dict[str, Point] = {}
	submissions: dict[str, Point] = {}

	for r in submission_tracker.rounds:
		targets[r.name] = r.target
		for sub in r.submissions:
			if sub.name == name:
				submissions[r.name] = sub.point

	return targets, submissions


def get_submission_data(path: Path, name: str | None):
	targets, submissions = get_points(path, name)
	if name is None:
		return geopandas.GeoSeries(targets, crs='wgs84')
	lines: dict[str, LineString] = {}
	for round_name, target in targets.items():
		lines[round_name] = LineString([target, submissions[round_name]])
	return geopandas.GeoDataFrame(
		{'target': targets, 'submission': submissions, 'line': lines}, geometry='line', crs='wgs84'
	)


def plot_submissions(
	path: Path,
	name: str | None = None,
	output_path: Path | None = None,
	provider: 'str | TileProvider | None' = None,
	marker_size: float = 5,
):
	fig, ax = pyplot.subplots()
	if provider is None:
		# OSM is being annoying today and being too slow, so I won't use it
		provider = get_google_map_provider(GoogleBasemap.Standard)

	gdf = get_submission_data(path, name)
	print(gdf)
	if isinstance(gdf, geopandas.GeoDataFrame):
		for _, row in gdf.iterrows():
			tx = row['target'].x
			ty = row['target'].y
			sx = row['submission'].x
			sy = row['submission'].y
			# TODO: Style options, for now this is just AusTPG styled
			ax.scatter(tx, ty, marker_size, 'green')
			ax.scatter(sx, sy, marker_size, 'gold')
			ax.annotate('', (tx, ty), (sx, sy), arrowprops={'arrowstyle': '->', 'linewidth': 1})
	else:
		gdf.plot(color='green', markersize=marker_size, ax=ax)

	contextily.add_basemap(
		ax, source='http://mt.google.com/vt/lyrs=m&x={x}&y={y}&z={z}', crs=gdf.crs
	)
	ax.set_axis_off()
	fig.tight_layout()

	if output_path:
		fig.savefig(output_path, dpi=200)
	else:
		pyplot.show()


def main() -> None:
	argparser = ArgumentParser()
	argparser.add_argument('path', type=Path, help='Path to KML file containing each round')
	argparser.add_argument(
		'name', nargs='?', help='Name of user to plot, if not specified will just plot rounds'
	)
	argparser.add_argument(
		'--output-path',
		'--save',
		'--to-file',
		type=Path,
		dest='output_path',
		help='Location to save map as an image, instead of showing',
	)
	argparser.add_argument(
		'--provider',
		nargs='?',
		help='Map provider (URL, name of xysservices provider, etc), defaults to Google Maps',
	)

	args = argparser.parse_args()

	plot_submissions(args.path, args.name, args.output_path, args.provider)


if __name__ == '__main__':
	main()

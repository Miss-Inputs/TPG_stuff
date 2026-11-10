#!/usr/bin/env python3
"""Creates a "travel map" of sorts automatically from submissions, filling in regions from how many people have been there"""

from argparse import ArgumentParser
from collections.abc import Sequence
from pathlib import Path

import geopandas
import pandas
from matplotlib import pyplot
from travelpygame.util import parse_submission_kml, read_geodataframe


def get_submissions(paths: Sequence[Path]):
	submission_tracker = parse_submission_kml(paths)
	submissions = []

	for r in submission_tracker.rounds:
		submissions.extend({'name': sub.name, 'geometry': sub.point} for sub in r.submissions)

	return geopandas.GeoDataFrame(submissions, geometry='geometry', crs='wgs84')


def _unique(group: 'pandas.DataFrame'):
	unique_visitors = group['name'].dropna().unique()
	return pandas.Series(
		{
			'count': unique_visitors.size,
			'visitors': ', '.join(unique_visitors),
			'geometry': group.geometry.iloc[0],
		}
	)


def get_num_visitors_by_region(
	submissions: geopandas.GeoDataFrame, regions: geopandas.GeoDataFrame, name_col: str
):
	regions = regions[[name_col, 'geometry']]
	submissions = submissions.sjoin(regions, how='right')
	return submissions.groupby(name_col, sort=False)[['name', 'geometry']].apply(_unique)


def plot_visitors(visitors: geopandas.GeoDataFrame, output_path: Path | None):
	ax = visitors.plot('count')

	# contextily.add_basemap(ax, source=provider, crs=gdf.crs)
	pyplot.tight_layout(pad=0)
	ax.set_axis_off()

	if output_path:
		pyplot.savefig(output_path, dpi=200)
	else:
		pyplot.show()


def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument(
		'path', type=Path, help='Path to KML/KMZ file from the submission tracker', nargs='+'
	)
	argparser.add_argument(
		'regions', type=Path, help='Path to GeoJSON/.gpkg/etc file containing regions as polygons'
	)
	argparser.add_argument(
		'--name-col',
		help='Name of column in regions file to use, if not specified then try to autodetect',
	)
	argparser.add_argument(
		'--output-path',
		'--save',
		'--to-file',
		type=Path,
		dest='output_path',
		help='Location to save map as an image, instead of showing',
	)

	args = argparser.parse_args()
	paths: list[Path] = args.path

	if len(paths) == 1 and paths[0].suffix[1:].lower() == 'geojson':
		submissions = read_geodataframe(paths[0])
	else:
		submissions = get_submissions(paths)
	regions = read_geodataframe(args.regions)
	assert submissions.crs, 'What did you do to my CRS'
	regions = regions.to_crs(submissions.crs)

	name_col: str = args.name_col or regions.drop(columns='geometry').columns[0]

	visitors_by_region = get_num_visitors_by_region(submissions, regions, name_col)
	print(
		visitors_by_region[['count', 'visitors']]
		.sort_values('count', ascending=False)
		.to_string(max_colwidth=40)
	)
	if not isinstance(visitors_by_region, geopandas.GeoDataFrame):
		raise TypeError(f'Uh oh visitors_by_region is {type(visitors_by_region)}')
	plot_visitors(visitors_by_region, args.output_path)


if __name__ == '__main__':
	main()

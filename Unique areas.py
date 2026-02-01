#!/usr/bin/env python3
"""Makes a map of areas that are exclusively submitted by one user, for some kind of hypothetical TPG-like strategy game where the goal is to "capture" regions."""

import asyncio
from argparse import ArgumentParser, BooleanOptionalAction
from collections import Counter, defaultdict
from collections.abc import Hashable
from pathlib import Path

import contextily
import geopandas
import pandas
from matplotlib import pyplot
from travelpygame.subs_per_player import load_or_fetch_per_player_submissions
from travelpygame.util import (
	first_unique_column_label,
	output_dataframe,
	output_geodataframe,
	read_geodataframe,
	wgs84_geod,
)

from lib.settings import Settings


def get_players_by_region(
	regions: geopandas.GeoDataFrame, subs: geopandas.GeoDataFrame, name_col: Hashable | None
):
	indices = regions.sindex.query(subs.geometry, 'intersects', output_format='indices')
	players_by_region: defaultdict[int, Counter[str]] = defaultdict(Counter)
	# Firstly ensure we have every region…
	for i in range(regions.index.size):
		players_by_region[i] = Counter()
	for point_index, region_index in indices.T:
		player = subs['player'].iloc[point_index]
		players_by_region[region_index.item()][player] += 1

	rows = []
	for region_index, players in players_by_region.items():
		if players:
			most_common_count = max(players.values())
			most_common_player = '/'.join(k for k, v in players.items() if v == most_common_count)
		else:
			most_common_player = None
			most_common_count = 0
		geometry = regions.geometry.iloc[region_index]
		rows.append(
			{
				'name': regions[name_col].iloc[region_index]
				if name_col
				else regions.index[region_index],
				'players': sorted(players.keys()),
				'count': len(players),
				'most_common_player': most_common_player,
				'strength': most_common_count,
				'area': abs(wgs84_geod.geometry_area_perimeter(geometry)[0]),
				'geometry': geometry,
			}
		)

	gdf = geopandas.GeoDataFrame(rows, geometry='geometry', crs=regions.crs)
	gdf['status'] = gdf['count'].map({0: 'Unclaimed', 1: 'Controlled'}).fillna('Contested')
	return gdf


def plot_regions(gdf: geopandas.GeoDataFrame, output_path: Path | None, *, use_contextily: bool):
	fig, ax = pyplot.subplots()

	gdf = gdf.copy()
	gdf.loc[gdf['status'] == 'Unclaimed', 'status'] = None
	gdf.plot(
		'status',
		legend=True,
		ax=ax,
		linewidth=0.1,
		edgecolor='black',
		missing_kwds=None
		if use_contextily
		else {'color': 'lightgray', 'linewidth': 0, 'label': 'Unclaimed'},
	)

	if use_contextily:
		# This likes to not work when gdf includes the whole world… hrm
		contextily.add_basemap(ax, crs='EPSG:4326')

	ax.set_axis_off()
	fig.tight_layout()
	if output_path:
		fig.savefig(output_path, dpi=600)
	else:
		fig.show()


def load_regions(regions_path: Path):
	regions = read_geodataframe(regions_path)
	if not regions.crs:
		print('regions had no CRS, assuming WGS84')
		regions = regions.set_crs('WGS84')
	elif not regions.crs.equals('WGS84'):
		print(f'regions CRS was {regions.crs.name} {regions.crs.srs}, converting to WGS84')
		regions = regions.to_crs('WGS84')
	return regions


def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument(
		'regions_path',
		type=Path,
		help='Path to GeoJSON/.gpkg/etc file containing regions as polygons',
	)
	argparser.add_argument(
		'--name-col',
		help='Name of column in regions file to use, if not specified then try to autodetect',
	)
	argparser.add_argument(
		'--submissions-path', type=Path, help='Path to geofile with submissions per user'
	)
	argparser.add_argument('--map-output-path', type=Path, help='Path to output map image')
	argparser.add_argument(
		'--regions-output-path', type=Path, help='Path to output regions with info'
	)
	argparser.add_argument(
		'--output-path',
		type=Path,
		help='Path to output list of players with list/count of "controlled" regions',
	)
	argparser.add_argument(
		'--use-contextily',
		action=BooleanOptionalAction,
		help='Try using contextily to add a basemap to the map, defaults false because it seems to not be working',
	)

	# Does it make more sense to just use reverse geocoding and get the address components? Hm, maybe

	args = argparser.parse_args()
	regions = load_regions(args.regions_path)
	name_col = args.name_col
	if not name_col:
		name_col = 'name' if 'name' in regions.columns else first_unique_column_label(regions)
		if name_col:
			print(f'Autodetected name column as {name_col}')

	subs_path: Path | None = args.submissions_path
	if not subs_path:
		subs_path = Settings().subs_per_player_path
	if not subs_path:
		raise RuntimeError(
			'--submissions-path was not set in the environment or provided explicitly'
		)

	subs_per_user = asyncio.run(load_or_fetch_per_player_submissions(subs_path))
	# Awkwardly convert it back to a single DataFrame
	subs = pandas.concat(
		(points.assign(player=player) for player, points in subs_per_user.items()),
		ignore_index=True,
	)
	assert isinstance(subs, geopandas.GeoDataFrame), f'subs was {type(subs)}'

	gdf = get_players_by_region(regions, subs, name_col)
	if args.regions_output_path:
		output_geodataframe(gdf, args.regions_output_path, index=False)

	contested = gdf[gdf['status'] == 'Contested']
	print('Contested:')
	print(contested.drop(columns='status').sort_values('count', ascending=False))

	controlled = gdf[gdf['status'] == 'Controlled'].rename(columns={'most_common_player': 'player'})
	print('Controlled:')
	print(controlled[['name', 'player', 'strength']].sort_values('strength', ascending=False))

	unclaimed = gdf[gdf['status'] == 'Unclaimed']
	print('Unclaimed:')
	print(unclaimed[['name', 'area']].sort_values('name'))

	regions_by_player = {
		player: {
			'regions': sorted(regions['name']),
			'count': regions.index.size,
			'total_area': regions['area'].sum(),
		}
		for player, regions in controlled.groupby('player')
	}
	df = pandas.DataFrame.from_dict(regions_by_player, 'index')
	df = df.sort_values('count', ascending=False)
	print(df)
	if args.output_path:
		output_dataframe(df, args.output_path)

	plot_regions(gdf, args.map_output_path, use_contextily=args.use_contextily)


if __name__ == '__main__':
	main()

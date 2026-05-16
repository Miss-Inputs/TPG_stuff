#!/usr/bin/env python3
"""Finds clusters of points in all submissions ever submitted.

Uses SciPy's hierarchical clustering because I'm still a bit ehhh about requiring sklearn."""

import asyncio
import logging
from argparse import ArgumentParser
from collections.abc import Hashable
from operator import itemgetter
from pathlib import Path
from typing import TYPE_CHECKING

from geopandas import GeoDataFrame
from tqdm.contrib.logging import logging_redirect_tqdm
from travelpygame.point_set_stats import find_clusters
from travelpygame.submission_data import load_or_fetch_submission_summary
from travelpygame.util import format_distance
from travelpygame.util.distance import self_cartesian_product_distances
from travelpygame.util.formatting import format_point
from travelpygame.util.io_utils import output_geodataframe

from lib.io_utils import load_point_set_from_arg
from lib.settings import Settings

if TYPE_CHECKING:
	from pandas import DataFrame


def add_cluster_info(gdf: GeoDataFrame, threshold: float):
	gdf['cluster_id'] = find_clusters(gdf.geometry, threshold)
	cluster_sizes = gdf['cluster_id'].value_counts().to_dict()
	gdf['cluster_size'] = gdf['cluster_id'].map(cluster_sizes)

	single_clusters = {cluster_id for cluster_id, size in cluster_sizes.items() if size == 1}
	gdf.loc[gdf['cluster_id'].isin(single_clusters), 'cluster_id'] = None


def _cluster_groupby_sort_key(kv: tuple[Hashable, 'DataFrame']):
	cluster_size = kv[1].iloc[0]['cluster_size']
	if not isinstance(cluster_size, (float, int)):
		cluster_size = cluster_size.item()
	return cluster_size


def _self_cartesian_key(kv: tuple[Hashable, dict[Hashable, float]]):
	return sum(kv[1].values())


def get_cluster_info(gdf: GeoDataFrame, *, print_stuff: bool = True) -> GeoDataFrame:
	groupby = gdf.groupby('cluster_id', dropna=True)
	rows = []
	for cluster_id, cluster in sorted(groupby, key=_cluster_groupby_sort_key):
		assert isinstance(cluster, GeoDataFrame), (
			f'Somehow cluster group was {type(cluster)} and not GeoDataFrame'
		)
		n = cluster.iloc[0]['cluster_size']
		distances = self_cartesian_product_distances(cluster.geometry)
		centre_index = min(distances.items(), key=_self_cartesian_key)[0]
		centre = cluster.loc[centre_index, 'geometry']  # ty:ignore[invalid-argument-type]
		furthest_index, furthest_dist = max(distances[centre_index].items(), key=itemgetter(1))
		furthest_point = cluster.loc[furthest_index, 'geometry']  # ty:ignore[invalid-argument-type]

		# Could have option to use closest_to_corners to get centre, but it's not that important and just for informational purposes
		player_names = cluster['player_name'].dropna().unique()
		n_players = player_names.size
		rows.append(
			{
				'id': cluster_id,
				'centre': centre,
				'size': n,
				'player': player_names[0] if n_players == 1 else None,
				'players': player_names,
				'num_players': n_players,
				'radius': furthest_dist,
			}
		)

		if print_stuff:
			print(f'Cluster {cluster_id}: {n} items')
			for _, row in cluster.iterrows():
				print(row['player_name'], '@', format_point(row['geometry']))
			print('Centre:', format_point(centre))
			print(
				'Furthest from centre:',
				format_point(furthest_point),
				format_distance(furthest_dist),
			)
			print('-' * 10)
	return GeoDataFrame(rows, geometry='centre', crs='wgs84').set_index('id')


def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	argparser.add_argument(
		'--points',
		help="Optional path to load points to cluster, or player:<player name>/username:<username> to use an individual player's submissions, instead of using all TPG submissions",
	)
	argparser.add_argument(
		'--threshold',
		type=float,
		default=100,
		help='Threshold for clustering in metres, defaults to 100m.',
	)
	argparser.add_argument(
		'--output-path',
		type=Path,
		help='Where to save centres of clusters, can be .csv or .geojson/.gpkg/etc',
	)
	argparser.add_argument(
		'--outlier-output-path',
		type=Path,
		help='Optional path to save points that are not part of any cluster, can be .csv or .geojson/.gpkg/etc',
	)
	argparser.add_argument(
		'--complete-output-path',
		type=Path,
		help='Optional path to save all points alongside cluster ID and size, can be .csv or .geojson/.gpkg/etc',
	)
	args = argparser.parse_args()

	# TODO: This could have some other different parameters to get submissions for a certain round etc

	settings = Settings()
	points_path: Path | None = args.points
	if points_path:
		point_set = asyncio.run(
			load_point_set_from_arg(args.points_path, settings_or_path=settings)
		)
		gdf = point_set.gdf
	else:
		subs_path = settings.all_subs_path
		gdf = asyncio.run(load_or_fetch_submission_summary(subs_path))

	threshold: float = args.threshold
	add_cluster_info(gdf, threshold)

	outlier_output_path: Path | None = args.outlier_output_path
	if outlier_output_path:
		outliers = gdf[gdf['cluster_id'].isna()].drop(columns=['cluster_id', 'cluster_size'])
		output_geodataframe(outliers, outlier_output_path)

	all_output_path: Path | None = args.complete_output_path
	if all_output_path:
		output_geodataframe(gdf, all_output_path)

	clusters = get_cluster_info(gdf)
	clusters = clusters.sort_values('radius')
	print(clusters)
	output_path: Path | None = args.output_path
	if output_path:
		output_geodataframe(clusters, output_path)

	# TODO: Could save clusters as multipoints, if that is ever a useful/interesting thing to do


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	with logging_redirect_tqdm():
		main()

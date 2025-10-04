import logging
from collections.abc import Hashable, Sequence

import geopandas
import numpy
import pandas
import shapely
from tqdm.auto import tqdm
from travelpygame.util import geod_distance_and_bearing, haversine_distance

logger = logging.getLogger(__name__)


def get_point_uniqueness(
	point: shapely.Point,
	others: geopandas.GeoSeries | shapely.GeometryCollection | Sequence[shapely.Point],
	*,
	use_geod: bool = True,
):
	"""Finds how far away a point is from any other point.

	Arguments:
		point: A point to be compared to others.
		others: The other points, convertible to GeoSeries. Assumed to be in the same CRS as point, and to not already include point.

	Returns:
		(distance in metres, index in others of closest point)
	"""
	# I dunno about this type hint for others but eh, I'm just putting stuff in there just in case
	if not isinstance(others, geopandas.GeoSeries):
		others = geopandas.GeoSeries(others)
	n = others.size
	x = [point.x] * n
	y = [point.y] * n
	distances_array = (
		geod_distance_and_bearing(y, x, others.y, others.x)[0]
		if use_geod
		else haversine_distance(
			numpy.asarray(y), numpy.asarray(x), others.y.to_numpy(), others.x.to_numpy()
		)
	)
	distances = pandas.Series(distances_array, index=others.index)
	min_dist, min_index = distances.agg(['min', 'idxmin'])
	return min_dist, min_index


def get_points_uniqueness(points: geopandas.GeoSeries):
	"""Gets the minimum distance to other points and index of closest point for each point in points.

	Raises:
		TypeError: If points does not contain points.

	Returns:
		(distances in metres, closest indexes)
	"""
	distances: dict[Hashable, float] = {}
	closest_indexes: dict[Hashable, Hashable] = {}
	for index, point in tqdm(points.items(), 'Finding uniqueness', points.size):
		if not isinstance(point, shapely.Point):
			raise TypeError(type(point))
		others = points.drop(index=index)
		distances[index], closest_indexes[index] = get_point_uniqueness(point, others)
	return pandas.Series(distances), pandas.Series(closest_indexes)


def get_points_uniqueness_in_row(points: geopandas.GeoDataFrame, unique_row: Hashable):
	"""Gets the minimum distance to other points and index of closest point for each point in points, only comparing to rows where a value in a certain column is different.

	Raises:
		TypeError: If points does not contain points.

	Returns:
		(distances in metres, closest indexes)
	"""
	distances: dict[Hashable, float] = {}
	closest_indexes: dict[Hashable, Hashable] = {}
	for index, row in tqdm(points.iterrows(), 'Finding uniqueness', points.index.size):
		point = row.geometry
		if not isinstance(point, shapely.Point):
			raise TypeError(type(point))
		others = points[points[unique_row] != points.at[index, unique_row]]  # pyright: ignore[reportArgumentType]
		distances[index], closest_indexes[index] = get_point_uniqueness(point, others.geometry)
	return pandas.Series(distances), pandas.Series(closest_indexes)

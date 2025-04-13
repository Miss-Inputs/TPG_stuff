from collections import defaultdict
from collections.abc import Hashable, Iterable, Sequence
from functools import partial
from itertools import combinations
from typing import Any, overload

import geopandas
import numpy
import pandas
import pyproj
import shapely
from tqdm.auto import tqdm
from tqdm.contrib.concurrent import process_map

geod = pyproj.Geod(ellps='WGS84')


@overload
def geod_distance_and_bearing(
	lat1: float, lng1: float, lat2: float, lng2: float, *, radians: bool = False
) -> tuple[float, float]: ...


FloatListlike = Sequence[float] | numpy.ndarray | pandas.Series
"""Accepted input types to pyproj.Geod.inv"""


@overload
def geod_distance_and_bearing(
	lat1: FloatListlike,
	lng1: FloatListlike,
	lat2: FloatListlike,
	lng2: FloatListlike,
	*,
	radians: bool = False,
) -> tuple[numpy.ndarray, numpy.ndarray]: ...


def geod_distance_and_bearing(
	lat1: float | FloatListlike,
	lng1: float | FloatListlike,
	lat2: float | FloatListlike,
	lng2: float | FloatListlike,
	*,
	radians: bool = False,
) -> tuple[float | numpy.ndarray, float | numpy.ndarray]:
	"""
	Calculates the WGS84 geodesic distance and heading from one point to another. lat1/lng1/lat2/lng2 can either all be floats, or all arrays.

	Arguments:
		lat1: Latitude of point A, or list/ndarray/etc
		lng1: Longitude of point A, or list/ndarray/etc
		lat2: Latitude of point A, or list/ndarray/etc
		lng2: Longitude of point A, or list/ndarray/etc
		radians: If true, treats the arguments as being in radians, otherwise they are degrees (as normal people use for coordinates)

	Returns:
		(Distance in metres, heading/direction/bearing/whatever you call it from lat1,lng1 to lat2,lng2 in degrees/radians) between point A and point B. If input is an array, it will return an array for each pair of coordinates.
	"""
	_, bearing, dist = geod.inv(lng1, lat1, lng2, lat2, radians=radians)
	if isinstance(bearing, list):
		# y u do this
		bearing = numpy.array(bearing)
	return (dist, bearing)


def geod_distance(point1: 'shapely.Point', point2: 'shapely.Point') -> float:
	"""Returns WGS84 geodesic distance between point1 and point2 (assumed to be WGS84 coordinates) in metres."""
	return geod_distance_and_bearing(point1.y, point1.x, point2.y, point2.x)[0]


def haversine_distance(
	lat1: numpy.ndarray,
	lng1: numpy.ndarray,
	lat2: numpy.ndarray,
	lng2: numpy.ndarray,
	*,
	radians: bool = False,
) -> numpy.ndarray:
	"""Calculates haversine distance (which TPG uses), treating the earth as a sphere.

	Arguments:
		lat1: ndarray of floats
		lng1: ndarray of floats
		lat1: ndarray of floats
		lng1: ndarray of floats
		radians: If set to true, treats the lat/long arguments as being in radians, otherwise they are treated as degrees (as normal people would use for coordinates)

	Returns:
		ndarray (float) of distances in metres

	"""
	r = 6371_000
	if not radians:
		lat1 = numpy.radians(lat1)
		lat2 = numpy.radians(lat2)
		lng1 = numpy.radians(lng1)
		lng2 = numpy.radians(lng2)
	dlng = lng2 - lng1
	dlat = lat2 - lat1
	a = (numpy.sin(dlat / 2) ** 2) + numpy.cos(lat1) * numpy.cos(lat2) * (numpy.sin(dlng / 2) ** 2)
	c = 2 * numpy.asin(numpy.sqrt(a))
	return c * r


def random_point_in_bbox(
	min_x: float,
	min_y: float,
	max_x: float,
	max_y: float,
	random: numpy.random.Generator | int | None = None,
) -> shapely.Point:
	"""Uniformly generates a point somewhere in a bounding box."""
	if not isinstance(random, numpy.random.Generator):
		random = numpy.random.default_rng(random)
	x = random.uniform(min_x, max_x)
	y = random.uniform(min_y, max_y)
	return shapely.Point(x, y)


def random_point_in_poly(
	poly: shapely.Polygon | shapely.MultiPolygon, random: numpy.random.Generator | int | None = None
) -> shapely.Point:
	"""
	Uniformly-ish generates a point somewhere within a polygon.
	This won't choose anywhere directly on the edge (I think). If poly is a MultiPolygon, it will be inside one of the components, but the distribution of which one might not necesarily be uniform.

	Arguments:
		poly: shapely Polygon or MultiPolygon
		random: Optionally a numpy random generator or seed, otherwise default_rng is used
	"""
	min_x, max_x, min_y, max_y = poly.bounds
	shapely.prepare(poly)
	while True:
		point = random_point_in_bbox(min_x, max_x, min_y, max_y, random)
		if poly.contains_properly(point):
			return point


def circular_mean(angles: list[float] | numpy.ndarray) -> float:
	"""Assumes this is in radians

	Returns:
		Mean angle"""
	if isinstance(angles, list):
		angles = numpy.asarray(angles)
	sin_sum = numpy.sin(angles).sum()
	cos_sum = numpy.cos(angles).sum()
	# Convert it from numpy.floating to float otherwise that's maybe annoying
	return float(numpy.atan2(sin_sum, cos_sum))


def circular_mean_xy(x: Iterable[float], y: Iterable[float]) -> tuple[float, float]:
	"""x and y are assumed to be convertible to numpy.ndarray! I can't be arsed type hinting list-like

	Returns:
		mean of x, mean of y
		i.e. long and then lat, do not get them swapped around I swear on me mum
	"""
	if not isinstance(x, (numpy.ndarray)):
		x = numpy.asarray(x)
	if not isinstance(y, (numpy.ndarray)):
		y = numpy.asarray(y)
	x = numpy.radians(x + 180)
	y = numpy.radians((y + 90) * 2)
	mean_x = numpy.degrees(circular_mean(x))
	mean_y = numpy.degrees(circular_mean(y))
	mean_x = (mean_x % 360) - 180
	mean_y = ((mean_y % 360) / 2) - 90
	return mean_x, mean_y


def circular_mean_points(points: Iterable[shapely.Point]) -> shapely.Point:
	"""points is assumed to be convertible to numpy.ndarray!"""
	x, y = zip(*((a.x, a.y) for a in points), strict=True)
	mean_x, mean_y = circular_mean_xy(x, y)
	return shapely.Point(mean_x, mean_y)


def _dist_matrix_worker(a: Any, b: Any, *, points: 'geopandas.GeoSeries'):
	row_a = points.loc[a]
	row_b = points.loc[b]
	return a, b, geod_distance(row_a, row_b)


def distance_matrix(
	points: 'geopandas.GeoSeries', chunksize: int = 10_000, *, multiprocess: bool = True
) -> pandas.DataFrame:
	# I should probably use sklearn pairwise distances here but eh, didn't feel like it
	distances = defaultdict(dict)
	n = points.size

	if multiprocess:
		f = partial(_dist_matrix_worker, points=points)
		index_combinations = list(combinations(points.index, 2))
		for a, b, dist in process_map(
			f,
			*zip(*index_combinations, strict=True),
			chunksize=chunksize,
			desc='Calculating distances',
			leave=False,
		):
			distances[a][b] = distances[b][a] = dist
	else:
		total = (n * (n - 1)) // 2
		for a, b in tqdm(
			combinations(points.index, 2), 'Calculating distances', total, leave=False
		):
			row_a = points.loc[a]
			row_b = points.loc[b]
			dist = geod_distance(row_a, row_b)
			distances[a][b] = distances[b][a] = dist
	return pandas.DataFrame(distances)


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
	for index, row in tqdm(points.iterrows(), 'Finding uniqueness', points.size):
		point = row.geometry
		if not isinstance(point, shapely.Point):
			raise TypeError(type(point))
		others = points[points[unique_row] != points.at[index, unique_row]]
		distances[index], closest_indexes[index] = get_point_uniqueness(point, others.geometry)
	return pandas.Series(distances), pandas.Series(closest_indexes)

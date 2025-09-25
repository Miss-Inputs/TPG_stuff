import logging
from collections import defaultdict
from collections.abc import Collection, Hashable, Sequence
from functools import partial
from itertools import combinations
from typing import Any

import geopandas
import numpy
import pandas
import shapely
from scipy.optimize import differential_evolution
from tqdm.auto import tqdm
from tqdm.contrib.concurrent import process_map
from travelpygame.util import (
	geod_distance,
	geod_distance_and_bearing,
	get_antipode,
	haversine_distance,
)

logger = logging.getLogger(__name__)


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
	for index, row in tqdm(points.iterrows(), 'Finding uniqueness', points.index.size):
		point = row.geometry
		if not isinstance(point, shapely.Point):
			raise TypeError(type(point))
		others = points[points[unique_row] != points.at[index, unique_row]]  # pyright: ignore[reportArgumentType]
		distances[index], closest_indexes[index] = get_point_uniqueness(point, others.geometry)
	return pandas.Series(distances), pandas.Series(closest_indexes)


def _maximin_objective(x: numpy.ndarray, points: Collection[shapely.Point]):
	point = shapely.Point(x)
	return -min(geod_distance(point, p) for p in points)


def _find_furthest_point_single(points: Collection[shapely.Point]):
	point = next(iter(points))
	anti_lat, anti_lng = get_antipode(point.y, point.x)
	antipode = shapely.Point(anti_lng, anti_lat)
	# Can't be bothered remembering the _exact_ circumference of the earth, maybe I should to speed things up whoops
	return antipode, geod_distance(point, antipode)


def find_furthest_point_via_optimization(
	points: Collection[shapely.Point],
	initial: shapely.Point | None = None,
	max_iter: int = 1_000,
	pop_size: int = 20,
	*,
	use_tqdm: bool = True,
) -> tuple[shapely.Point, float]:
	if len(points) == 1:
		return _find_furthest_point_single(points)
	# TODO: Should be able to trivially speed up len(points) == 2 by getting the midpoint of the two antipodes, unless I'm wrong
	bounds = ((-180, 180), (-90, 90))
	with tqdm(
		desc='Differentially evolving', total=(max_iter + 1) * pop_size * 2, disable=not use_tqdm
	) as t:
		# total should be actually (max_iter + 1) * popsize * 2 but eh I'll fiddle with that later
		def callback(*_):
			# If you just pass t.update to the callback= argument it'll just stop since t.update() returns True yippeeeee
			t.update()

		result = differential_evolution(
			_maximin_objective,
			bounds,
			popsize=pop_size,
			args=(points,),
			x0=numpy.asarray([initial.x, initial.y]) if initial else None,
			maxiter=max_iter,
			mutation=(0.5, 2.0),
			tol=1e-7,  # should probably be a argument
			callback=callback,
		)

	point = shapely.Point(result.x)
	distance = -result.fun
	if not result.success:
		logger.info(result.message)
	if not isinstance(distance, float):
		# Those numpy floating types are probably going to bite me in the arse later if I don't stop them propagating
		distance = float(distance)
	return point, distance

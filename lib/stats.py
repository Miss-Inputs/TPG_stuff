from collections import Counter
from itertools import combinations

import pandas
import shapely
from shapely import MultiPolygon, Point, Polygon
from tqdm.auto import tqdm
from travelpygame.util import get_poly_vertices


def get_longest_distance(poly: Polygon | MultiPolygon, *, use_tqdm: bool = True):
	"""This is too slow and will never realistically finish, so you shouldn't use it.

	There must be some fancy schmancy way to do this…"""
	vertices = get_poly_vertices(poly)
	pairs = combinations(vertices, 2)
	if use_tqdm:
		n = len(vertices)
		pairs = tqdm(pairs, f'Getting max distance of {n} vertices', total=(n * (n - 1)) // 2)

	distances = (float(shapely.distance(x, y)) for x, y in pairs)
	return max(distances)


def get_longest_distance_from_point(poly: Polygon | MultiPolygon, point: Point):
	vertices = [v for v in get_poly_vertices(poly) if not v.equals_exact(point, 1e-7)]
	distances = [[v, point.distance(v)] for v in vertices]
	df = pandas.DataFrame(distances, columns=['point', 'distance'])
	idxmax = df['distance'].idxmax()
	antipoint = df.loc[idxmax, 'point']
	assert isinstance(antipoint, Point), type(antipoint)
	return antipoint, float(df.loc[idxmax, 'distance'])  # type: ignore[arg-type]


def summarize_counter[T](counter: Counter[T]):
	counts = pandas.Series(counter)
	percents = counts / counter.total()
	percents_formatted = percents.map('{:%}'.format)
	df = pandas.DataFrame({'count': counts, 'percent': percents_formatted})
	return df.sort_values('count', ascending=False)

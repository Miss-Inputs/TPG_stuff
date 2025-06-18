from dataclasses import dataclass
from typing import TYPE_CHECKING

import numpy
import shapely
from shapely import Point

from lib.geo_utils import geod_distance_and_bearing

if TYPE_CHECKING:
	from lib.kml import SubmissionTrackerRound


@dataclass
class RoundStats:
	average_distance: float
	"""Average distance of all submissions in km"""
	average_distance_raw: float
	"""Average distance of all submissions in km, including submissions that are so far away that they get 0 distance points"""
	centroid: Point
	"""Centroid of all submissions"""
	centroid_raw: Point
	"""Centroid of all submissions, including submissions that are so far away that they get 0 distance points"""


def get_round_stats(r: 'SubmissionTrackerRound', world_distance: float | None = None):
	"""
	Arguments:
		world_distance: Distance in km considered the size of the "world", and any submissions outside that are excluded
	"""
	n = len(r.submissions)
	x = [r.target.x] * n
	y = [r.target.y] * n
	sub_x = [sub.point.x for sub in r.submissions]
	sub_y = [sub.point.y for sub in r.submissions]

	distances, _ = geod_distance_and_bearing(sub_y, sub_x, y, x)
	if world_distance:
		included = [float(distance) <= (world_distance * 1000) for distance in distances]
	else:
		included = [True] * n
	avg = numpy.mean(distances, where=included)
	avg_raw = numpy.mean(distances)

	# TODO: Handle rare case of no submissions matching distance threshold
	all_points = shapely.MultiPoint(
		[sub.point for i, sub in enumerate(r.submissions) if included[i]]
	)
	centroid = all_points.centroid
	all_points_raw = shapely.MultiPoint([sub.point for sub in r.submissions])
	centroid_raw = all_points_raw.centroid
	return RoundStats(float(avg), float(avg_raw), centroid, centroid_raw)

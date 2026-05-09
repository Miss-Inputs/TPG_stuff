"""Helpers for travelpygame.reverse_geocode that uses the GADM paths in the settings"""

import logging
from collections.abc import Mapping
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING

import pandas
from travelpygame.reverse_geocode import reverse_geocode_regions_multiple
from travelpygame.util import read_geodataframe

from .settings import Settings

if TYPE_CHECKING:
	from geopandas import GeoDataFrame
	from travelpygame import PointSet

logger = logging.getLogger(__name__)

gadm_col_names = {0: 'COUNTRY', 1: 'NAME_1', 2: 'NAME_2', 3: 'NAME_3'}


def _get_gadm_settings_paths(settings: Settings | None) -> dict[int, Path | None]:
	settings = settings or Settings()
	return {
		0: settings.gadm_0_path,
		1: settings.gadm_1_path,
		2: settings.gadm_2_path,
		3: settings.gadm_3_path,
	}


@cache
def _load_gadm(
	level: int, paths: Mapping[int, Path | None], levels: Mapping[int, 'GeoDataFrame | None']
) -> 'GeoDataFrame | None':
	gadm = levels.get(level)
	if gadm is not None:
		return gadm
	path = paths.get(level)
	if path is not None:
		return read_geodataframe(path)
	return None


def reverse_geocode_gadm(
	points: 'PointSet',
	depth: int,
	gadm_levels: dict[int, 'GeoDataFrame | None'],
	settings: Settings | None = None,
) -> pandas.DataFrame:
	"""
	Reverse geocodes using GADM levels from settings.

	Arguments:
		points: PointSet
		depth: How many levels of GADM to return, starting at 0
		gadm_levels: GADM levels that have already been loaded
		settings: Settings if initialized elsewhere, otherwise they will be loaded from defaults or environment etc

	Returns:
		DataFrame, indexed by index of `points`, with columns being GADM levels
	"""
	# TODO: We might want an async version of this (and hence a _load_gadm_async too)
	# TODO: Could also support using the combined GADM levels in one file, and not having them as separate levels, because those are both download options
	paths = _get_gadm_settings_paths(settings)
	results: list[pandas.DataFrame] = []
	for i in range(depth):
		gadm_level = _load_gadm(paths, gadm_levels)
		if gadm_level is None:
			logger.info(
				'Wanted to reverse geocode %s using GADM, but level %d is not configured',
				points.name,
				i,
			)
			continue
		col_name = gadm_col_names[i]
		if col_name not in gadm_level.columns:
			# Could be the case if the user has customized or messed around with GADM, or maybe an old version, or they are just using a different file entirely and saying it's GADM which is like yeah okay sure, you can do that
			# TODO: We might want an argument for custom column names if that use case becomes ever worth bothering with
			logger.info(
				'Setting col name for GADM level %d to "name" as %s unexpectedly seems to not be in there',
				i,
				col_name,
			)
			col_name = 'name'
		# I don't think GADM would overlap…
		result = reverse_geocode_regions_multiple(
			points.points, gadm_level, [col_name], allow_multiple=False
		)
		if result.columns.size > 1:
			logger.warning(
				'Reverse geocoding %s at GADM level %d returned multiple columns somehow: %s',
				points.name,
				i,
				result.columns,
			)
		results.append(result)
	return pandas.concat(results, axis='columns')


def _join_row(row: pandas.Series) -> str:
	return ', '.join(value for value in row.dropna())


def reverse_geocode_gadm_address(
	points: 'PointSet',
	depth: int,
	gadm_levels: dict[int, 'GeoDataFrame | None'],
	settings: Settings | None = None,
):
	"""Reverse geocodes using one layer of GADM for all points."""
	df = reverse_geocode_gadm(points, depth, gadm_levels, settings)
	df = df.iloc[:, ::-1]
	return df.apply(_join_row, axis='columns')

from pathlib import Path
from typing import TYPE_CHECKING, cast

import pandas
from travelpygame.util import read_geodataframe

if TYPE_CHECKING:
	import geopandas
	from shapely import Point


def reverse_geocode_gadm(
	point: 'Point', gadm: 'Path | geopandas.GeoDataFrame', col_name: str = 'COUNTRY'
):
	if isinstance(gadm, Path):
		gadm = read_geodataframe(gadm)
	index = gadm.sindex.query(point, 'within')
	return gadm.iloc[index[0]][col_name]


def reverse_geocode_gadm_all(
	points: 'geopandas.GeoSeries', gadm: 'Path | geopandas.GeoDataFrame', col_name: str = 'COUNTRY'
):
	"""Reverse geocodes using one layer of GADM for all points."""
	if isinstance(gadm, Path):
		gadm = read_geodataframe(gadm)
	gadm_index, point_index = points.sindex.query(
		gadm.geometry, 'contains', output_format='indices'
	)
	d: dict[int, str] = {
		cast('int', points.index[p]): gadm.iloc[g][col_name]
		for p, g in zip(point_index, gadm_index, strict=True)
	}
	return pandas.Series(d, index=points.index)


def reverse_geocode_gadm_country(
	points: 'geopandas.GeoSeries', gadm_0: 'Path | geopandas.GeoDataFrame'
):
	return reverse_geocode_gadm_all(points, gadm_0, 'COUNTRY')

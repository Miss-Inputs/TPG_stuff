#!/usr/bin/env python3

from pathlib import Path

import geopandas
import shapely

from lib.geo_utils import random_point_in_poly

from .settings import Settings


def load_australia(gadm_path: Path):
	gadm: geopandas.GeoDataFrame = geopandas.read_file(gadm_path)
	au = gadm[gadm['GID_0'] == 'AUS'].union_all()
	assert isinstance(au, shapely.MultiPolygon), type(au)
	return au


def main() -> None:
	settings = Settings()
	if not settings.gadm_path:
		raise RuntimeError('gadm_path needs to be specified')
	au = load_australia(settings.gadm_path)
	point = random_point_in_poly(au)
	print(point)


if __name__ == '__main__':
	main()

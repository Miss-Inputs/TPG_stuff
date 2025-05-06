#!/usr/bin/env python3

import asyncio
from pathlib import Path

import geopandas
import shapely
import shapely.ops
from aiohttp import ClientSession
from pyproj import Transformer

from lib.format_utils import describe_point, format_point
from lib.geo_utils import random_point_in_poly
from settings import Settings


def load_australia(gadm_path: Path):
	gadm: geopandas.GeoDataFrame = geopandas.read_file(gadm_path)
	au = gadm[gadm['GID_0'] == 'AUS'].union_all()
	assert isinstance(au, shapely.MultiPolygon), type(au)
	return au


async def main() -> None:
	settings = Settings()
	if not settings.gadm_0_path:
		raise RuntimeError('gadm_path needs to be specified')
	n = 20
	au = await asyncio.to_thread(load_australia, settings.gadm_0_path)
	wgs84_to_albers = Transformer.from_crs('wgs84', 'EPSG:9473', always_xy=True)
	au_albers = shapely.ops.transform(wgs84_to_albers.transform, au)

	async with ClientSession() as sesh:
		for i in range(1, n + 1):
			albers_point = random_point_in_poly(au_albers)
			x, y = wgs84_to_albers.transform(albers_point.x, albers_point.y, direction='INVERSE')
			point = shapely.Point(x, y)
			desc = await describe_point(point, sesh)
			print(f'{i}: {format_point(point)} {desc}')


if __name__ == '__main__':
	asyncio.run(main(), debug=False)

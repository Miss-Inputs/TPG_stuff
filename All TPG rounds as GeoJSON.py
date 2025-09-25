#!/usr/bin/env python3

import asyncio

import geopandas
import pandas
from travelpygame.tpg_api import get_rounds

from lib.io_utils import format_path
from settings import Settings


async def main() -> None:
	settings = Settings()

	rounds = [r.model_dump() for r in await get_rounds()]
	df = pandas.DataFrame(rounds)
	df = df.sort_values('number')

	geom = geopandas.points_from_xy(df['longitude'], df['latitude'], crs='wgs84')
	gdf = geopandas.GeoDataFrame(df.drop(columns=['latitude', 'longitude']), geometry=geom)
	print(gdf)

	if settings.rounds_path:
		output_path = format_path(settings.rounds_path, gdf['number'].max())
		gdf.to_file(output_path)


if __name__ == '__main__':
	asyncio.run(main())

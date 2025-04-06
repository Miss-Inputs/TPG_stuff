#!/usr/bin/env python3

import geopandas
import pandas

from lib.io_utils import format_path
from lib.tastycheese_map import get_tpg_rounds
from settings import Settings


def main() -> None:
	settings = Settings()

	rounds = [r.model_dump() for r in get_tpg_rounds()]
	df = pandas.DataFrame(rounds)
	print(df)
	geom = geopandas.points_from_xy(df['longitude'], df['latitude'], crs='wgs84')
	gdf = geopandas.GeoDataFrame(df.drop(columns=['latitude', 'longitude']), geometry=geom)
	print(gdf)

	if settings.rounds_path:
		output_path = format_path(settings.rounds_path, gdf['number'].max())
		gdf.to_file(output_path)


if __name__ == '__main__':
	main()

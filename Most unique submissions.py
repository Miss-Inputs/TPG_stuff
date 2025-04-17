#!/usr/bin/env python3
"""This is mostly just here as a test"""

from collections.abc import Hashable

import geopandas
import pandas

from lib.geo_utils import get_points_uniqueness
from lib.io_utils import latest_file_matching_format_pattern, read_geodataframe
from lib.reverse_geocode import reverse_geocode_gadm_all, reverse_geocode_gadm_country
from settings import Settings


def _describe_row(index: Hashable, row: pandas.Series):
	name = row.get('name') or row['username']
	country = row.get('country')
	sub_1 = row.get('oblast')
	sub_2 = row.get('kabupaten')
	sub_3 = row.get('barangay')
	if pandas.isna(country):
		return f'{name}: {row["geometry"]} ({index})'
	components = [country]
	if not pandas.isna(sub_1):
		components.append(sub_1)
	if not pandas.isna(sub_2) and sub_2 != sub_1:
		components.append(sub_2)
	if not pandas.isna(sub_3) and sub_3 != sub_2:
		components.append(sub_3)

	return f'{name}: {", ".join(reversed(components))} ({index})'

def _calc_and_print_uniqueness(submissions: geopandas.GeoDataFrame):
	submissions['uniqueness'], submissions['closest'] = get_points_uniqueness(submissions.geometry)

	print('Most unique pic:')
	print(submissions.loc[submissions['uniqueness'].idxmax()].to_dict())
	print('Closest to:')
	print(submissions.loc[submissions.loc[submissions['uniqueness'].idxmax(), 'closest']].to_dict())

	print('Most unique Miss Inputs pic:')
	my_most_unique = submissions[submissions['username'] == 'miss_inputs']['uniqueness'].idxmax()
	print(submissions.loc[my_most_unique].to_dict())
	print('Closest to:')
	print(submissions.loc[submissions.loc[my_most_unique, 'closest']].to_dict())

def main() -> None:
	settings = Settings()
	if not settings.submissions_path:
		raise RuntimeError('need submissions_path, run All TPG submissions.py first')
	path = latest_file_matching_format_pattern(settings.submissions_path.with_suffix('.geojson'))

	# Using the .geojson just to filter out duplicate (reused) submissions, do we actually want that?
	submissions: geopandas.GeoDataFrame = geopandas.read_file(path)
	if settings.gadm_0_path:
		gadm_0 = read_geodataframe(settings.gadm_0_path)
		submissions['country'] = reverse_geocode_gadm_country(submissions.geometry, gadm_0)
	if settings.gadm_1_path:
		gadm_1 = read_geodataframe(settings.gadm_1_path)
		submissions['oblast'] = reverse_geocode_gadm_all(submissions.geometry, gadm_1, 'NAME_1')
	if settings.gadm_2_path:
		gadm_2 = read_geodataframe(settings.gadm_2_path)
		submissions['kabupaten'] = reverse_geocode_gadm_all(submissions.geometry, gadm_2, 'NAME_2')
	if settings.gadm_3_path:
		gadm_3 = read_geodataframe(settings.gadm_3_path)
		submissions['barangay'] = reverse_geocode_gadm_all(submissions.geometry, gadm_3, 'NAME_3')
	submissions['description'] = {
		index: _describe_row(index, row) for index, row in submissions.iterrows()
	}
	submissions = submissions.set_index('description', verify_integrity=True)
	submissions = submissions.drop(
		columns=[
			'discord_id',
			'place',
			'fivek',
			'antipode_5k',
			'total_subs',
			'first_round',
			'latest_round',
			'round',
		]
	)
	print(submissions)
	_calc_and_print_uniqueness(submissions)


if __name__ == '__main__':
	main()

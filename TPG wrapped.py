#!/usr/bin/env python3

import logging
from functools import cache, cached_property
from pathlib import PurePath

import geopandas
import pandas
import pycountry
from tqdm.auto import tqdm

from lib.geo_utils import circular_mean_points
from lib.io_utils import (
	latest_file_matching_format_pattern,
	read_dataframe_pickle,
	read_geodataframe,
)
from lib.reverse_geocode import reverse_geocode_gadm_all, reverse_geocode_gadm_country
from settings import Settings

logger = logging.getLogger(__name__)


def _to_flag_emoji(cc: str):
	return ''.join(chr(ord(c) + (ord('🇦') - ord('A'))) for c in cc)


class TPGWrapped:
	"""Has properties for various parts of the TPG wrapped, holds stuff, outputs text, just felt like this should be a class, etc. Not necessarily finished yet"""

	def __init__(
		self, name: str, username: str, submissions: geopandas.GeoDataFrame, rows_shown: int = 5
	):
		self.name = name
		self.username = username
		self.submissions = submissions
		self.rows_shown = rows_shown

		self.user_submissions = submissions[submissions['username'] == username].copy()
		self.first_usages = self.user_submissions[self.user_submissions['first_use']]
		times_used = (
			self.user_submissions.groupby(['latitude', 'longitude'], sort=False).size().to_dict()
		)
		self.user_submissions['times_used'] = self.user_submissions.apply(
			lambda row: times_used.get((row['latitude'], row['longitude'])),  # type: ignore[overload]
			axis='columns',
		)

	@cached_property
	def unique_country_flags(self):
		"""Returns flags as str, but the names of countries/territories if they're something that doesn't have a flag"""
		return self.user_submissions['flag'].fillna(self.user_submissions['country']).unique()

	@cached_property
	def unique_subdivisions(self):
		"""Returns subdivisions qualified with the country as a tuple, i.e. (country, sub), beware of ('', '')"""
		full_subdivs = pandas.Series(
			list(
				zip(
					self.user_submissions['country'].fillna(''),
					self.user_submissions['oblast'].fillna(''),
					strict=True,
				)
			),
			index=self.user_submissions.index,
		)
		return full_subdivs.dropna().unique()

	@property
	def unique_subdivisions_formatted(self):
		return ', '.join(cs[1] for cs in self.unique_subdivisions if cs != ('', ''))

	@property
	def average_point(self):
		return circular_mean_points(self.first_usages.geometry.to_numpy())

	@property
	def average_point_weighted(self):
		return circular_mean_points(self.user_submissions.geometry.to_numpy())

	def to_text(self):
		parts = [
			f"{self.name}'s TPG Wrapped for Season 2",
			f'Rounds played this season: {self.user_submissions.index.size}\n'
			+ f'Number of unique photos: {self.first_usages.index.size}\n'
			+ f'Unique countries: {len(self.unique_country_flags)} {" ".join(self.unique_country_flags)}\n'
			+ f'Unique subdivisions: {len(self.unique_subdivisions)} ({self.unique_subdivisions_formatted})',
			# Average ranking
		]

		average_point = self.average_point
		average_point_weighted = self.average_point_weighted
		average_point_lines = [
			f'Average submission of unique submissions: {average_point.y},{average_point.x}'
		]
		average_point_lines.append(
			f'Average point of all your submissions: {average_point_weighted.y},{average_point_weighted.x}:'
		)
		parts.append('\n'.join(average_point_lines))
		# TOOD: Reverse geocode average point

		most_used = (
			self.user_submissions[self.user_submissions['first_use']]
			.sort_values('times_used', ascending=False)
			.head(self.rows_shown)
		)
		most_used_lines = ['Most submitted locations:']
		for i, (_, row) in enumerate(most_used.iterrows(), 1):
			most_used_lines.append(f'{i}. {row["description"]} ({row["times_used"]} times)')
		parts.append('\n'.join(most_used_lines))

		# TODO: Move logic here into properties
		# TODO: The people want counts of countries across both unique locations and all submissions
		country_usage_lines = ['Most submitted countries:']
		country_usage = self.first_usages.groupby('country', dropna=False, sort=False).size()
		most_used_countries = country_usage.sort_values(ascending=False).head(self.rows_shown)
		for i, (country, usage) in enumerate(most_used_countries.items(), 1):
			if pandas.isna(country):  # type: ignore[argumentType] #what the hell
				country_usage_lines.append(f'{i}. <unknown> ({usage} times)')
			else:
				country_usage_lines.append(
					f'{i}. {get_flag_emoji(country)} {country} ({usage} times)'
				)
		parts.append('\n'.join(country_usage_lines))

		subdiv_usage_lines = ['Most submitted subdivisions:']
		subdiv_usage = self.user_submissions.groupby(
			['country', 'oblast'], dropna=False, sort=False
		).size()
		most_used_subdivs = subdiv_usage.sort_values(ascending=False).head(self.rows_shown)
		for i, ((country, subdiv), usage) in enumerate(most_used_subdivs.items(), 1):  # type: ignore[reportGeneralTypeIssues] #aaaa
			if pandas.isna(country):  # type: ignore[argumentType]
				subdiv_usage_lines.append(f'{i}. <unknown> ({usage} times)')
			else:
				subdiv_usage_lines.append(
					f'{i}. {get_flag_emoji(country)} {subdiv}, {country} ({usage} times)'
				)
		parts.append('\n'.join(subdiv_usage_lines))

		# Most consecutive same photos (probably won't try doing this)
		# Most consecutive unique photos
		# Most consecutive unique countries
		# Most obscure countries submitted
		# Most obscure subdivisions submitted
		# Most unique locations
		# Least unique locations

		closest_submissions_lines = ['Closest submissions:']
		closest_submissions = self.user_submissions.sort_values('distance').head(self.rows_shown)
		for i, (_, row) in enumerate(closest_submissions.iterrows(), 1):
			closest_submissions_lines.append(
				f'{i}. Round {row["round"]} {_to_flag_emoji(row["target_country"])}: {row["description"]} ({row["distance"] / 1000:,.3f} km)'
			)
		parts.append('\n'.join(closest_submissions_lines))

		highest_rank_lines = ['Highest rank:']
		highest_rank = self.user_submissions.sort_values('place').head(self.rows_shown)
		for i, (_, row) in enumerate(highest_rank.iterrows(), 1):
			highest_rank_lines.append(
				f'{i}. Round {row["round"]} {_to_flag_emoji(row["target_country"])}: {row["description"]} ({row["place"]})'
			)
		parts.append('\n'.join(highest_rank_lines))

		highest_rank_pct_lines = ['Highest rank %:']
		highest_rank_pct = self.user_submissions.sort_values('place_percent').head(self.rows_shown)
		for i, (_, row) in enumerate(highest_rank_pct.iterrows(), 1):
			highest_rank_pct_lines.append(
				f'{i}. Round {row["round"]} {_to_flag_emoji(row["target_country"])}: {row["description"]} ({row["place_percent"]:%})'
			)
		parts.append('\n'.join(highest_rank_pct_lines))

		most_points_lines = ['Most points:']
		most_points = self.user_submissions.sort_values('score', ascending=False).head(
			self.rows_shown
		)
		for i, (_, row) in enumerate(most_points.iterrows(), 1):
			most_points_lines.append(
				f'{i}. Round {row["round"]} {_to_flag_emoji(row["target_country"])}: {row["description"]} ({row["score"]:.2f})'
			)
		parts.append('\n'.join(most_points_lines))

		return '\n\n'.join(parts)

	@property
	def output_filename(self) -> PurePath:
		name = self.username
		if name[0] == '.':
			name = f' {name[1:]}'
		return PurePath(name)


def _describe_row(row: pandas.Series):
	country = row.get('country')
	sub_1 = row.get('oblast')
	sub_2 = row.get('kabupaten')
	sub_3 = row.get('barangay')
	if pandas.isna(country):
		return f'{(row["latitude"], row["longitude"])}'
	components = [country]
	if not pandas.isna(sub_1):
		components.append(sub_1)
	if not pandas.isna(sub_2) and sub_2 != sub_1:
		components.append(sub_2)
	if not pandas.isna(sub_3) and sub_3 != sub_2:
		components.append(sub_3)

	return f'{", ".join(reversed(components))}'


@cache
def get_flag_emoji(country_name: str | None) -> str | None:
	if pandas.isna(country_name):
		return '🏳️'
	others = {
		# Mapping some things manually because GADM has older names for things, or iso-codes doesn't have something as a common name that you would expect it to, or some other weird cases. Please don't cancel me for any of this
		'Northern Cyprus': None,  # eh, GADM has it there separately, how would you really emoji that
		'Democratic Republic of the Congo': '🇨🇩',
		'Swaziland': '🇸🇿',
		'Turkey': '🇹🇷',
	}
	if country_name in others:
		return others[country_name]
	try:
		countries = pycountry.countries.search_fuzzy(country_name)
	except LookupError:
		logger.warning('Could not find country %s', country_name)
		return None
	if not countries:
		return None
	return getattr(countries[0], 'flag', None)


def add_countries_etc_from_gadm(submissions: geopandas.GeoDataFrame, settings: Settings):
	first_used = submissions[submissions['first_use']].copy()
	if settings.gadm_0_path:
		gadm_0 = read_geodataframe(settings.gadm_0_path)
		first_used['country'] = reverse_geocode_gadm_country(first_used.geometry, gadm_0)
		first_used['flag'] = first_used['country'].map(get_flag_emoji)  # type: ignore[overload] #what?
	if settings.gadm_1_path:
		gadm_1 = read_geodataframe(settings.gadm_1_path)
		first_used['oblast'] = reverse_geocode_gadm_all(first_used.geometry, gadm_1, 'NAME_1')
	if settings.gadm_2_path:
		gadm_2 = read_geodataframe(settings.gadm_2_path)
		first_used['kabupaten'] = reverse_geocode_gadm_all(first_used.geometry, gadm_2, 'NAME_2')
	if settings.gadm_3_path:
		gadm_3 = read_geodataframe(settings.gadm_3_path)
		first_used['barangay'] = reverse_geocode_gadm_all(first_used.geometry, gadm_3, 'NAME_3')
	first_used['description'] = first_used.apply(_describe_row, axis='columns')
	first_used.loc[first_used.duplicated('description', keep=False), 'description'] += (
		' (' + first_used['latitude'].astype(str) + ',' + first_used['longitude'].astype(str) + ')'
	)
	# There's definitely a better way to do this but I'm sleepy. The idea here is just that I don't call reverse_geocode_gadm_all on resubmitted pics, only the first use
	d = first_used.to_dict(orient='index')
	for _, group in submissions.groupby(['latitude', 'longitude']):
		first_index = group[group['first_use']].first_valid_index()
		first = d[first_index]
		for k in ('country', 'flag', 'oblast', 'kabupaten', 'barangay', 'description'):
			if k not in submissions.columns:
				# avoid weird warning (probably related to https://github.com/pandas-dev/pandas/issues/55025 which claims to be resolved)
				submissions[k] = None
			submissions.loc[group.index, k] = first[k]


def main() -> None:
	settings = Settings()
	if not settings.submissions_with_scores_path:
		raise RuntimeError(
			'needs submissions_with_scores_path, run TPG submissions with scores.py first'
		)
	path = latest_file_matching_format_pattern(settings.submissions_with_scores_path)
	submissions = read_dataframe_pickle(path, desc='Loading submissions with scores', leave=False)
	submissions = submissions.rename(columns={'country': 'target_country'})
	submissions = submissions[submissions['round'] >= 215]
	submissions['place_percent'] = submissions['place'] / submissions['total_subs']
	submissions['first_use'] = ~submissions.duplicated(['latitude', 'longitude'], keep='first')
	submissions = geopandas.GeoDataFrame(
		submissions,
		geometry=geopandas.points_from_xy(submissions['longitude'], submissions['latitude']),
		crs='wgs84',
	)
	add_countries_etc_from_gadm(submissions, settings)

	names = submissions.drop_duplicates('username').set_index('username')['name'].to_dict()
	usernames = submissions['username'].unique()
	for username in tqdm(usernames, 'Creating wrappeds'):
		name = names.get(username, username)
		wrapped = TPGWrapped(name, username, submissions)
		# TODO: Ideally this should be async
		if settings.tpg_wrapped_output_path:
			path = settings.tpg_wrapped_output_path / wrapped.output_filename.with_suffix('.txt')
			path.write_text(wrapped.to_text(), 'utf-8')


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	main()

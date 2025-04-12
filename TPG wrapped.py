#!/usr/bin/env python3

import asyncio
import logging
from functools import cache, cached_property
from pathlib import PurePath

import aiohttp
import geopandas
import pandas
import pycountry
from aiohttp import ClientSession
from tqdm.auto import tqdm

from lib.geo_utils import circular_mean_points
from lib.io_utils import (
	latest_file_matching_format_pattern,
	read_dataframe_pickle_async,
	read_geodataframe_async,
)
from lib.other_utils import describe_row, format_point
from lib.reverse_geocode import (
	reverse_geocode_address,
	reverse_geocode_gadm_all,
	reverse_geocode_gadm_country,
)
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

	def most_used_unique_countries(self, *, unique_locs: bool = False):
		locs = self.first_usages if unique_locs else self.user_submissions
		country_usage = locs.groupby('country', dropna=False, sort=False).size()
		return country_usage.sort_values(ascending=False).head(self.rows_shown)

	def _most_used_countries_text(self, *, unique_locs: bool = False):
		country_usage_lines = [
			'Most submitted countries of unique locations:'
			if unique_locs
			else 'Most submitted countries:'
		]
		most_used_countries = self.most_used_unique_countries(unique_locs=unique_locs)
		for i, (country, usage) in enumerate(most_used_countries.items(), 1):
			if pandas.isna(country):  # type: ignore[argumentType] #what the hell
				country_usage_lines.append(f'{i}. <unknown> ({usage} times)')
			else:
				country_usage_lines.append(
					f'{i}. {get_flag_emoji(country)} {country} ({usage} times)'
				)
		return '\n'.join(country_usage_lines)

	async def to_text(self, session: ClientSession):
		"""session is used here for geocoding"""
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
		average_point_address = await reverse_geocode_address(
			average_point.y, average_point.x, session
		)
		average_point_weighted_address = await reverse_geocode_address(
			average_point_weighted.y, average_point_weighted.x, session
		)
		average_point_lines = [
			f'Average point of unique submissions: {format_point(average_point)}'
		]
		if average_point_address:
			average_point_lines.append(average_point_address)
		average_point_lines.append(
			f'Average point of all your submissions: {average_point_weighted.y},{average_point_weighted.x}'
		)
		if average_point_weighted_address:
			average_point_lines.append(average_point_weighted_address)
		parts.append('\n'.join(average_point_lines))

		most_used = (
			self.user_submissions[self.user_submissions['first_use']]
			.sort_values('times_used', ascending=False)
			.head(self.rows_shown)
		)
		most_used_lines = ['Most submitted locations:']
		for i, (_, row) in enumerate(most_used.iterrows(), 1):
			most_used_lines.append(
				f'{i}. {await describe_row(row, session)} ({row["times_used"]} times)'
			)
		parts.extend(
			(
				'\n'.join(most_used_lines),
				self._most_used_countries_text(),
				self._most_used_countries_text(unique_locs=True),
			)
		)
		# TODO: Move logic here into properties

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
				f'{i}. Round {row["round"]} {_to_flag_emoji(row["target_country"])}: {await describe_row(row, session)} ({row["distance"] / 1000:,.3f} km)'
			)
		parts.append('\n'.join(closest_submissions_lines))

		highest_rank_lines = ['Highest rank:']
		highest_rank = self.user_submissions.sort_values('place').head(self.rows_shown)
		for i, (_, row) in enumerate(highest_rank.iterrows(), 1):
			highest_rank_lines.append(
				f'{i}. Round {row["round"]} {_to_flag_emoji(row["target_country"])}: {await describe_row(row, session)} ({row["place"]})'
			)
		parts.append('\n'.join(highest_rank_lines))

		highest_rank_pct_lines = ['Highest rank %:']
		highest_rank_pct = self.user_submissions.sort_values('place_percent').head(self.rows_shown)
		for i, (_, row) in enumerate(highest_rank_pct.iterrows(), 1):
			highest_rank_pct_lines.append(
				f'{i}. Round {row["round"]} {_to_flag_emoji(row["target_country"])}: {await describe_row(row, session)} ({row["place_percent"]:%})'
			)
		parts.append('\n'.join(highest_rank_pct_lines))

		most_points_lines = ['Most points:']
		most_points = self.user_submissions.sort_values('score', ascending=False).head(
			self.rows_shown
		)
		for i, (_, row) in enumerate(most_points.iterrows(), 1):
			most_points_lines.append(
				f'{i}. Round {row["round"]} {_to_flag_emoji(row["target_country"])}: {await describe_row(row, session)} ({row["score"]:.2f})'
			)
		parts.append('\n'.join(most_points_lines))

		return '\n\n'.join(parts)

	@property
	def output_filename(self) -> PurePath:
		name = self.username
		if name[0] == '.':
			name = f' {name[1:]}'
		return PurePath(name)


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


async def add_countries_etc_from_gadm(submissions: geopandas.GeoDataFrame, settings: Settings):
	# TODO: Could probably load the 4 levels concurrently
	if settings.gadm_0_path:
		gadm_0 = await read_geodataframe_async(settings.gadm_0_path)
		submissions['country'] = reverse_geocode_gadm_country(submissions.geometry, gadm_0)
		# TODO: Where country is null, try Nominatim instead
		submissions['flag'] = submissions['country'].map(get_flag_emoji)  # type: ignore[overload] #what?
	if settings.gadm_1_path:
		gadm_1 = await read_geodataframe_async(settings.gadm_1_path)
		submissions['oblast'] = reverse_geocode_gadm_all(submissions.geometry, gadm_1, 'NAME_1')
	if settings.gadm_2_path:
		gadm_2 = await read_geodataframe_async(settings.gadm_2_path)
		submissions['kabupaten'] = reverse_geocode_gadm_all(submissions.geometry, gadm_2, 'NAME_2')
	if settings.gadm_3_path:
		gadm_3 = await read_geodataframe_async(settings.gadm_3_path)
		submissions['barangay'] = reverse_geocode_gadm_all(submissions.geometry, gadm_3, 'NAME_3')


async def get_and_write_wrapped_for_user(
	settings: Settings,
	submissions: geopandas.GeoDataFrame,
	session: ClientSession,
	name: str,
	username: str,
):
	wrapped = TPGWrapped(name, username, submissions)
	if settings.tpg_wrapped_output_path:
		path = settings.tpg_wrapped_output_path / wrapped.output_filename.with_suffix('.txt')
		text = await wrapped.to_text(session)
		await asyncio.to_thread(path.write_text, text, 'utf-8')


async def main() -> None:
	settings = Settings()
	if not settings.submissions_with_scores_path:
		# TODO: To make this easier, we should have some kind of load_or_get_submissions_with_scores and call that instead
		raise RuntimeError(
			'needs submissions_with_scores_path, run TPG submissions with scores.py first'
		)

	path = latest_file_matching_format_pattern(settings.submissions_with_scores_path)
	submissions = await read_dataframe_pickle_async(
		path, desc='Loading submissions with scores', leave=False
	)
	logger.info(
		'Loaded %d submissions from %s, max round: %d',
		submissions.index.size,
		path,
		submissions['round'].max(),
	)

	submissions = submissions.rename(columns={'country': 'target_country'})
	submissions = submissions[submissions['round'] >= 215]
	submissions['place_percent'] = submissions['place'] / submissions['total_subs']
	submissions['first_use'] = ~submissions.duplicated(['latitude', 'longitude'], keep='first')
	submissions = geopandas.GeoDataFrame(
		submissions,
		geometry=geopandas.points_from_xy(submissions['longitude'], submissions['latitude']),
		crs='wgs84',
	)
	await add_countries_etc_from_gadm(submissions, settings)

	names = submissions.drop_duplicates('username').set_index('username')['name'].to_dict()
	usernames = submissions['username'].unique()

	async with aiohttp.ClientSession() as sesh:
		tasks = []
		for username in usernames:
			name = names.get(username, username)
			tasks.append(
				asyncio.create_task(
					get_and_write_wrapped_for_user(settings, submissions, sesh, name, username),
					name=f'fwrapped_{username}',
				)
			)
		await tqdm.gather(*tasks, desc='Creating a wrapped for each user')


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	asyncio.run(main(), debug=False)

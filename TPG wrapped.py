#!/usr/bin/env python3

import asyncio
import logging
from collections.abc import Hashable
from functools import cached_property
from pathlib import Path, PurePath

import aiohttp
import geopandas
import pandas
from aiohttp import ClientSession
from tqdm.auto import tqdm

from lib.format_utils import country_name_to_code, describe_row, format_ordinal, format_point
from lib.geo_utils import circular_mean_points
from lib.io_utils import (
	latest_file_matching_format_pattern,
	read_dataframe_pickle_async,
	read_geodataframe_async,
)
from lib.reverse_geocode import (
	reverse_geocode_address,
	reverse_geocode_components,
	reverse_geocode_gadm_all,
	reverse_geocode_gadm_country,
)
from settings import Settings

logger = logging.getLogger(__name__)


def _to_flag_emoji(cc: str) -> str:
	if cc == 'XW':
		# I'm going to use this for water rounds, which we don't have in season 2, but just for the sake of making this robust
		return '🌊'
	return ''.join(chr(ord(c) + (ord('🇦') - ord('A'))) for c in cc)


async def _describe_round_row(row: pandas.Series, session: ClientSession):
	round_num = row['round']
	flag = _to_flag_emoji(row['target_cc'])
	return f'Round {round_num} {flag}: {await describe_row(row, session)}'


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
		"""Returns country codes, but the names of countries/territories if they're something that doesn't have an ISO code"""
		return (
			self.user_submissions['flag']
			.fillna(self.user_submissions['country'])
			.fillna('🏳️')
			.unique()
		)

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
		return ', '.join(cs[1] or '??' for cs in self.unique_subdivisions if cs != ('', ''))

	@property
	def average_point(self):
		return circular_mean_points(self.first_usages.geometry.to_numpy())

	@property
	def average_point_weighted(self):
		return circular_mean_points(self.user_submissions.geometry.to_numpy())

	def most_used_unique_countries(self, *, unique_locs: bool = False):
		locs = self.first_usages if unique_locs else self.user_submissions
		country_usage = locs.groupby(['country', 'flag'], dropna=False, sort=False).size()
		return country_usage.sort_values(ascending=False).head(self.rows_shown)

	def _most_used_countries_text(self, *, unique_locs: bool = False):
		country_usage_lines = [
			'Most of your unique photos were from these countries:'  # Hmm I dunno if I like the wording here
			if unique_locs
			else 'You were most often submitting photos from these countries:'
		]
		most_used_countries = self.most_used_unique_countries(unique_locs=unique_locs)
		for i, ((flag, country), usage) in enumerate(most_used_countries.items(), 1):  # type: ignore[reportGeneralTypeIssues]
			if pandas.isna(country):  # type: ignore[argumentType] #what the hell
				country_usage_lines.append(f'{i}. <unknown> ({usage} times)')
			else:
				country_usage_lines.append(f'{i}. {flag} {country} ({usage} times)')
		return '\n'.join(country_usage_lines)

	def most_obscure_countries(self):
		other_submissions = self.submissions[self.submissions['username'] != 'username']
		counts = other_submissions[['country', 'flag']].value_counts(sort=True, ascending=True)
		assert isinstance(counts.index, pandas.MultiIndex), type(counts.index)
		counts = counts[counts.index.isin(self.user_submissions['country'].dropna().unique(), 0)]
		return counts.head(self.rows_shown)

	def _opening_text(self):
		countries_formatted = ' '.join(self.unique_country_flags)
		opening_lines = [
			f'You played {self.user_submissions.index.size} rounds this season',
			f'and you submitted {self.first_usages.index.size} unique photos.',
			f"You've been to {len(self.unique_country_flags)} different countries: {countries_formatted}",
			f"You've been to {len(self.unique_subdivisions)} different subdivisions: ({self.unique_subdivisions_formatted})",
			f'You placed {format_ordinal(self.user_submissions["place"].mean())} on average.',
			f'Your median placement was {format_ordinal(self.user_submissions["place"].median())}.',
			f'On average, you placed in the top {self.user_submissions["place_percent"].mean():%} of players each round.',
		]
		return '\n'.join(opening_lines)

	async def _average_point_text(self, session: ClientSession):
		average_point = self.average_point
		average_point_weighted = self.average_point_weighted
		average_point_address = await reverse_geocode_address(
			average_point.y, average_point.x, session
		)
		average_point_weighted_address = await reverse_geocode_address(
			average_point_weighted.y, average_point_weighted.x, session
		)
		average_point_lines = [
			f'The average location of all your submissions is {format_point(average_point)}.'
		]
		if average_point_address:
			average_point_lines.append(f"That's located at {average_point_address}")
		average_point_lines.append(
			f'Average point of all your submissions: {format_point(average_point_weighted)}'
		)
		if average_point_weighted_address:
			average_point_lines.append(f"That's located at {average_point_weighted_address}")
		return '\n'.join(average_point_lines)

	async def to_text(self, session: ClientSession):
		"""session is used here for geocoding"""
		parts = [
			f"{self.name}'s TPG Wrapped for Season 2",
			self._opening_text(),
			await self._average_point_text(session),
		]

		most_used = (
			self.user_submissions[self.user_submissions['first_use']]
			.sort_values('times_used', ascending=False)
			.head(self.rows_shown)
		)
		most_used_lines = ['These are your favourite locations. You submitted these the most!']
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

		subdiv_usage_lines = ['You were most often submitting photos from these subdivisions:']
		subdiv_usage = self.user_submissions.groupby(
			['flag', 'country', 'oblast'], dropna=False, sort=False
		).size()
		most_used_subdivs = subdiv_usage.sort_values(ascending=False).head(self.rows_shown)
		for i, ((flag, country, subdiv), usage) in enumerate(most_used_subdivs.items(), 1):  # type: ignore[reportGeneralTypeIssues] #aaaa
			if pandas.isna(country):  # type: ignore[argumentType]
				subdiv_usage_lines.append(f'{i}. <unknown> ({usage} times)')
			else:
				subdiv_usage_lines.append(f'{i}. {flag} {subdiv}, {country} ({usage} times)')
		parts.append('\n'.join(subdiv_usage_lines))

		# Most consecutive same photos (probably won't try doing this)
		# Most consecutive unique photos
		# Most consecutive unique countries
		most_obscure_countries_lines = [
			'These are the most obscure countries that you submitted photos from, compared to everyone else.'
		]
		for i, ((country, flag), usage) in enumerate(self.most_obscure_countries().items(), 1):  # type: ignore[reportGeneralTypeIssues] #aaaa
			most_obscure_countries_lines.append(
				f'{i}. {flag} {country}, submitted by other players {usage} times'
			)
		parts.append('\n'.join(most_obscure_countries_lines))

		# Most obscure countries submitted
		# Most obscure subdivisions submitted
		# Most unique locations
		# Least unique locations

		closest_submissions_lines = ['You got closest in these rounds:']
		# distance here is haversine distances which TPG scoring uses. Would comparing geodesic distances be interesting?
		closest_submissions = self.user_submissions.sort_values('distance').head(self.rows_shown)
		for i, (_, row) in enumerate(closest_submissions.iterrows(), 1):
			closest_submissions_lines.append(
				f'{i}. {await _describe_round_row(row, session)} ({row["distance"] / 1000:,.3f} km away)'
			)
		parts.append('\n'.join(closest_submissions_lines))
		furthest_lines = ['But these rounds were too far away for you. :(']
		furthest = self.user_submissions.sort_values('distance', ascending=False).head(
			self.rows_shown
		)
		for i, (_, row) in enumerate(furthest.iterrows(), 1):
			furthest_lines.append(
				f'{i}. {await _describe_round_row(row, session)} ({row["distance"] / 1000:,.3f} km away)'
			)
		parts.append('\n'.join(furthest_lines))

		highest_rank_lines = ['You got the best placing on these rounds!']
		highest_rank = self.user_submissions.sort_values('place').head(self.rows_shown)
		for i, (_, row) in enumerate(highest_rank.iterrows(), 1):
			highest_rank_lines.append(
				f'{i}. {await _describe_round_row(row, session)} ({format_ordinal(row["place"])})'
			)
		parts.append('\n'.join(highest_rank_lines))

		highest_rank_pct_lines = ['You were in the top percentage of players in these rounds.']
		highest_rank_pct = self.user_submissions.sort_values('place_percent').head(self.rows_shown)
		for i, (_, row) in enumerate(highest_rank_pct.iterrows(), 1):
			highest_rank_pct_lines.append(
				f'{i}. {await _describe_round_row(row, session)} ({row["place_percent"]:%})'
			)
		parts.append('\n'.join(highest_rank_pct_lines))

		most_points_lines = ['These rounds scored you the most points!']
		most_points = self.user_submissions.sort_values('score', ascending=False).head(
			self.rows_shown
		)
		for i, (_, row) in enumerate(most_points.iterrows(), 1):
			most_points_lines.append(
				f'{i}. {await _describe_round_row(row, session)} ({row["score"]:.2f})'
			)
		parts.append('\n'.join(most_points_lines))

		return '\n\n'.join(parts)

	@property
	def output_filename(self) -> PurePath:
		name = self.username
		if name[0] == '.':
			name = f' {name[1:]}'
		return PurePath(name)


async def _try_get_cc(
	index: Hashable, row: pandas.Series, country_names: pandas.Series, session: ClientSession
) -> tuple[str | None, str | None, str | None]:
	name = country_names[index]  # type: ignore[reportCallIssue] #blahhh
	# Territories that are in GADM but not ISO, so might be reverse geocoded as something else. Obligatory disclaimer that I am neither supporting or opposing either side of any disputes, this is just potentially interesting for stats
	funny_territories = {
		'Northern Cyprus',
		'Akrotiri and Dhekelia',
		'Clipperton Island',
		'Paracel Islands',
		'Spratly Islands',
	}
	if name in funny_territories:
		return name, None, None

	cc = country_name_to_code(name)
	if cc:
		return name, cc, _to_flag_emoji(cc)
	reverse_geo = await reverse_geocode_components(row['latitude'], row['longitude'], session)
	if reverse_geo and reverse_geo.features:
		props = reverse_geo.features[0].properties.geocoding
		cc = props.country_code
		if cc:
			return (
				props.country if pandas.isna(name) else name,
				cc.upper(),
				_to_flag_emoji(cc.upper()),
			)
	if row['latitude'] <= -60:
		return 'Antarctica', 'AQ', _to_flag_emoji('AQ')
	return name, None, name or '🏳️'


async def _try_get_subdivision(
	index: Hashable, row: pandas.Series, names: pandas.Series, session: ClientSession
) -> str | None:
	name = names[index]  # type: ignore[reportCallIssue] #blahhh
	if not pandas.isna(name):
		return name

	reverse_geo = await reverse_geocode_components(row['latitude'], row['longitude'], session)
	if reverse_geo and reverse_geo.features:
		props = reverse_geo.features[0].properties.geocoding
		subdiv = props.state
		if subdiv:
			return subdiv
	return None


async def _get_gadm_countries(
	submissions: geopandas.GeoDataFrame, gadm_path: Path, session: ClientSession
):
	gadm_0 = await read_geodataframe_async(gadm_path)
	countries = reverse_geocode_gadm_country(submissions.geometry, gadm_0)
	df = pandas.DataFrame.from_dict(
		{
			index: await _try_get_cc(index, row, countries, session)
			for index, row in submissions.iterrows()
		},
		orient='index',
	)
	df.columns = ['country', 'cc', 'flag']
	return df


async def _get_gadm_subdivs(
	submissions: geopandas.GeoDataFrame, gadm_path: Path, session: ClientSession
):
	gadm_1 = await read_geodataframe_async(gadm_path)
	s = reverse_geocode_gadm_all(submissions.geometry, gadm_1, 'NAME_1')
	return pandas.Series(
		{
			index: await _try_get_subdivision(index, row, s, session)
			for index, row in submissions.iterrows()
		},
		name='oblast',
	)


async def _add_countries_etc_from_gadm(
	submissions: geopandas.GeoDataFrame, settings: Settings, session: ClientSession
):
	tasks = []
	if settings.gadm_0_path:
		tasks.append(_get_gadm_countries(submissions, settings.gadm_0_path, session))
	if settings.gadm_1_path:
		tasks.append(_get_gadm_subdivs(submissions, settings.gadm_1_path, session))
	# if settings.gadm_2_path:
	# 	gadm_2 = await read_geodataframe_async(settings.gadm_2_path)
	# 	submissions['kabupaten'] = reverse_geocode_gadm_all(submissions.geometry, gadm_2, 'NAME_2')
	# if settings.gadm_3_path:
	# 	gadm_3 = await read_geodataframe_async(settings.gadm_3_path)
	# 	submissions['barangay'] = reverse_geocode_gadm_all(submissions.geometry, gadm_3, 'NAME_3')
	return [
		await task for task in tqdm.as_completed(tasks, desc='Finding countries/subdivisions/etc.')
	]


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
	submissions = submissions[submissions['round'] >= 215].copy()
	logger.info('%d submissions for season 2', submissions.index.size)

	submissions['target_cc'] = submissions.pop('country').fillna('XW')
	submissions['place_percent'] = submissions['place'] / submissions['total_subs']
	submissions['first_use'] = ~submissions.duplicated(['latitude', 'longitude'], keep='first')
	submissions = geopandas.GeoDataFrame(
		submissions,
		geometry=geopandas.points_from_xy(submissions['longitude'], submissions['latitude']),
		crs='wgs84',
	)

	names = submissions.drop_duplicates('username').set_index('username')['name'].to_dict()
	usernames = submissions['username'].unique()

	async with aiohttp.ClientSession() as sesh:
		submissions = pandas.concat(
			[submissions, *await _add_countries_etc_from_gadm(submissions, settings, sesh)],
			axis='columns',
		)
		assert isinstance(submissions, geopandas.GeoDataFrame), type(submissions)
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

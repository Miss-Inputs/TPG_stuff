"""Dunno what else to call this"""

import contextlib
import logging
from functools import cache
from typing import TYPE_CHECKING, Any

import pandas
import pycountry

from lib.reverse_geocode import reverse_geocode_address

if TYPE_CHECKING:
	from aiohttp import ClientSession
	from shapely import Point

logger = logging.getLogger(__name__)


def format_xy(x: float, y: float) -> str:
	# This could potentially have an argument for using that weird northing/easting format instead of decimal degrees
	return f'{y}, {x}'


def format_point(p: 'Point') -> str:
	return format_xy(p.x, p.y)


async def describe_coord(lat: float, lng: float, session: 'ClientSession') -> str:
	address = await reverse_geocode_address(lat, lng, session)
	if not address:
		if lat <= -60:
			# Nominatim has trouble with Antarctica for some reason
			return f'<Antarctica ({format_xy(lng, lat)})>'
		return f'<Unknown ({format_xy(lng, lat)})>'
	return address


async def describe_point(p: 'Point', session: 'ClientSession') -> str:
	return await describe_coord(p.y, p.x, session)


async def describe_row(row: 'pandas.Series', session: 'ClientSession') -> str:
	"""Describes a row of a DataFrame that is expected to have "latitude" and "longitude" columns, or a geometry."""
	geometry = row.get('geometry')
	if geometry:
		lng = geometry.x
		lat = geometry.y
	else:
		lng = row['longitude']
		lat = row['latitude']

	return await describe_coord(lat, lng, session)


@cache
def country_name_to_code(country_name: str | None) -> str | None:
	"""Converts a country name to an ISO 3166-1 alpha-2 code, useful for GADM etc

	Returns:
		Uppercase country code, or None if the country is unknown"""
	if pandas.isna(country_name):
		return None
	others = {
		# Mapping some things manually because GADM has older names for things, or iso-codes doesn't have something as a common name that you would expect it to, or some other weird cases. Please don't cancel me for any of this
		'Democratic Republic of the Congo': 'CD',
		'Kosovo': 'XK',  # pycountry will simply return Serbia from the fuzzy search…
		'Northern Cyprus': None,  # eh, GADM has it there separately, whaddya do
		'Swaziland': 'SZ',
		'Turkey': 'TR',
		# Or because pycountry search_fuzzy insists on checking subdivision names first sometimes
		'Sint Maarten': 'SX',
		'Curaçao': 'CW',
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
	if len(countries) > 1:
		logger.debug(
			'pycountry search_fuzzy for %s returned %d matches: %s, using first match',
			country_name,
			len(countries),
			countries,
		)
	return getattr(countries[0], 'alpha_2', None)


def get_ordinal(n: int) -> str:
	if 10 <= n % 100 <= 20:
		return 'th'
	return {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')

def format_ordinal(n: float) -> str:
	if not n.is_integer():
		#meh
		return f'{n:.2f}th'
	n = int(n)
	return f'{n}{get_ordinal(n)}'

async def print_round(n: int, row: Any, sesh: 'ClientSession'):
	loc_address = await reverse_geocode_address(row.target_lat, row.target_lng, sesh)
	print(f'{n}: Round {row.round}: {row.target_lat, row.target_lng} {loc_address}')
	sub_address = await reverse_geocode_address(row.latitude, row.longitude, sesh)
	print(f'Submission: {row.latitude}, {row.longitude} {sub_address}')
	print(
		f'Distance: {row.distance / 1000:4g}km Place: {row.place}/{row.total_subs} Score: {row.score}'
	)
	with contextlib.suppress(AttributeError):
		print(
			f'Geodesic distance: {row.geod_distance / 1000:.4g}km Heading from photo to loc: {row.heading}°'
		)

	print('-' * 10)

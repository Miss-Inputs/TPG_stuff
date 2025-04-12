"""I couldn't think of what to categorize these functions as sorry"""

from typing import TYPE_CHECKING

from lib.reverse_geocode import reverse_geocode_address

if TYPE_CHECKING:
	import pandas
	from aiohttp import ClientSession
	from shapely import Point


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

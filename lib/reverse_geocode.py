from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

import aiohttp
import pandas
import pydantic_core
import requests
from async_lru import alru_cache
from pydantic import BaseModel

from lib.io_utils import read_geodataframe

if TYPE_CHECKING:
	import geopandas
	from shapely import Point


class NominatimReverseJSONv2(BaseModel, extra='allow'):
	place_id: int
	licence: str
	"""Copyright string"""
	osm_type: str
	"""node, way, etc"""
	osm_id: int
	lat: float
	lon: float
	category: str
	"""man_made, etc"""
	type: str
	place_rank: int
	importance: float
	addresstype: str
	name: str
	"""Name of the node"""
	display_name: str
	boundingbox: tuple[float, float, float, float]
	"""min lat, max lat, min long, max long"""


class NominatimReverseJSONv2WithAddressDetails(NominatimReverseJSONv2):
	address: dict[str, str]


class NominatimGeocoding(BaseModel, extra='allow'):
	version: str
	"""0.1.0"""
	attribution: str
	"""Copyright string"""
	licence: str
	"""ODbL"""
	query: Literal['']


class NominatimGeocodingProperties(BaseModel, extra='allow'):
	"""Could have basically anything in it"""

	place_id: int
	osm_type: str
	osm_id: int
	osm_key: str
	osm_value: str
	osm_type: str
	accuracy: int
	label: str
	name: str | None = None
	postcode: str | None = None
	street: str | None = None
	district: str | None = None
	city: str | None = None
	state: str | None = None
	country: str | None = None
	"""Should _usually_ be there unless you're in a strange location (e.g. Antarctica)"""
	country_code: str | None = None
	admin: dict[str, str]
	"""keys: level9, level7, level4, etc"""


class NominatimProperties(BaseModel, extra='forbid'):
	"""huh? What a pointless object"""

	geocoding: NominatimGeocodingProperties


class NominatimFeature(BaseModel, extra='forbid'):
	type: Literal['Feature']
	properties: NominatimProperties
	geometry: Any
	"""type = point and coordinates or whatever, don't care"""


class NominatimReverseGeocodeJSON(BaseModel, extra='allow'):
	type: Literal['FeatureCollection']
	geocoding: NominatimGeocoding
	features: list[NominatimFeature]


class GeocodeError(Exception):
	pass


@cache
def reverse_geocode_address_sync(
	lat: float,
	lng: float,
	session: requests.Session | None = None,
	lang: str = 'en',
	timeout: int = 10,
) -> str | None:
	"""Finds an address for a point using synchronous requests.

	Arguments:
		lat: Latitude of point in WGS84.
		lng: Longitude of point in WGS84.
		session: Optional requests.Session if you have one, otherwise does not use a session. Recommended if you are using this in a loop, etc.
		timeout: Request timeout in seconds, defaults to 10 seconds.

	Returns:
		Address as string, or None if nothing could be found.
	"""
	get = session.get if session else requests.get
	url = 'https://nominatim.geocoding.ai/reverse'
	params = {
		'lat': lat,
		'lon': lng,
		'format': 'jsonv2',
		'addressdetails': 0,
		'accept-language': lang,
	}

	response = get(url, params=params, timeout=timeout)
	response.raise_for_status()
	content = response.content
	if content == b'{"error":"Unable to geocode"}':
		# TODO: Hmm I could do that a lot better surely, I just don't want to unnecessarily parse JSON twice
		return None
	return NominatimReverseJSONv2.model_validate_json(content).display_name


@alru_cache
async def reverse_geocode_address(
	lat: float,
	lng: float,
	session: aiohttp.ClientSession,
	lang: str = 'en',
	request_timeout: int = 10,
) -> str | None:
	"""Finds an address for a point using asynchronous requests.

	Raises:
		GeocodeError: If some weird error happens.

	Arguments:
		lat: Latitude of point in WGS84.
		lng: Longitude of point in WGS84.
		session: Optional requests.Session if you have one, otherwise does not use a session. Recommended if you are using this in a loop, etc.
		timeout: Request timeout in seconds, defaults to 10 seconds.

	Returns:
		Address as string, or None if nothing could be found.
	"""
	url = 'https://nominatim.geocoding.ai/reverse'
	params = {
		'lat': lat,
		'lon': lng,
		'format': 'jsonv2',
		'addressdetails': 0,
		'accept-language': lang,
	}

	async with session.get(
		url, params=params, timeout=aiohttp.ClientTimeout(request_timeout)
	) as response:
		response.raise_for_status()
		text = await response.text()
	j = pydantic_core.from_json(text)
	error = j.get('error')
	if error == 'Unable to geocode':
		return None
	if error:
		raise GeocodeError(error)
	return NominatimReverseJSONv2.model_validate(j).display_name


def reverse_geocode_components_sync(
	lat: float, lng: float, session: requests.Session | None = None, timeout: int = 10
) -> NominatimReverseGeocodeJSON | None:
	"""Returns individual address components instead of just a string.

	Raises:
		GeocodeError: If some weird error happens that isn't just 'unable to geocode'
	"""
	get = session.get if session else requests.get
	url = 'https://nominatim.geocoding.ai/reverse'
	params = {'lat': lat, 'lon': lng, 'format': 'geocodejson', 'addressdetails': 1}

	response = get(url, params=params, timeout=timeout)
	response.raise_for_status()
	content = response.content
	j = pydantic_core.from_json(content)
	error = j.get('error')
	if error == 'Unable to geocode':
		return None
	if error:
		raise GeocodeError(error)
	return NominatimReverseGeocodeJSON.model_validate(j)


async def reverse_geocode_components(
	lat: float,
	lng: float,
	session: aiohttp.ClientSession,
	lang: str = 'en',
	request_timeout: int = 10,
) -> NominatimReverseGeocodeJSON | None:
	"""Returns individual address components instead of just a string.

	Raises:
		GeocodeError: If some weird error happens that isn't just 'unable to geocode'
	"""
	url = 'https://nominatim.geocoding.ai/reverse'
	params = {
		'lat': lat,
		'lon': lng,
		'format': 'geocodejson',
		'addressdetails': 1,
		'accept-language': lang,
	}

	async with session.get(
		url, params=params, timeout=aiohttp.ClientTimeout(request_timeout)
	) as response:
		response.raise_for_status()
		text = await response.text()
	j = pydantic_core.from_json(text)
	error = j.get('error')
	if error == 'Unable to geocode':
		return None
	if error:
		raise GeocodeError(error)
	return NominatimReverseGeocodeJSON.model_validate(j)


def reverse_geocode_gadm(
	point: 'Point', gadm: 'Path | geopandas.GeoDataFrame', col_name: str = 'COUNTRY'
):
	if isinstance(gadm, Path):
		gadm = read_geodataframe(gadm)
	index = gadm.sindex.query(point, 'within')
	return gadm.iloc[index[0]][col_name]


def reverse_geocode_gadm_all(
	points: 'geopandas.GeoSeries', gadm: 'Path | geopandas.GeoDataFrame', col_name: str = 'COUNTRY'
):
	"""Reverse geocodes using one layer of GADM for all points."""
	if isinstance(gadm, Path):
		gadm = read_geodataframe(gadm)
	gadm_index, point_index = points.sindex.query(gadm.geometry, 'contains')
	d: dict[int, str] = {
		cast('int', points.index[p]): gadm.iloc[g][col_name]
		for p, g in zip(point_index, gadm_index, strict=True)
	}
	return pandas.Series(d, index=points.index)


def reverse_geocode_gadm_country(
	points: 'geopandas.GeoSeries', gadm_0: 'Path | geopandas.GeoDataFrame'
):
	return reverse_geocode_gadm_all(points, gadm_0, 'COUNTRY')

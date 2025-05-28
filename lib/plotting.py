from collections.abc import Iterable
from enum import StrEnum
from functools import cache

from xyzservices import TileProvider


class GoogleBasemap(StrEnum):
	RoadsOnly = 'h'
	"""Returns a PNG transparent background"""
	Standard = 'm'
	Terrain = 'p'
	AlternateRoadmap = 'r'
	"""Might be just identical to Standard now?"""
	SatelliteOnly = 's'
	TerrainOnly = 't'
	"""This does not mean terrain without labels, it is simply a layer that goes over the top of other layers, so will appear mostly black by itself"""
	SatelliteWithLabels = 'y'
	"""Also known as "hybrid" """


class GoogleMapsAdditionalLayer(StrEnum):
	Traffic = 'traffic'
	Transit = 'transit'
	Bicycle = 'bicycle'


@cache
def get_google_map_provider(
	layer: GoogleBasemap,
	additional_layers: Iterable[GoogleMapsAdditionalLayer] | None = None,
	locale: str | None = 'en',
) -> TileProvider:
	# traffic, transit, bicycle?
	layer_param = layer.value
	if additional_layers:
		layer_param += ',' + ','.join(add.value for add in additional_layers)
	# mt0 to mt3 are all accepted, though mt now load balances anyway
	url = f'http://mt.google.com/vt/lyrs={layer_param}'
	if locale:
		url += f'&hl={locale}'
	url += '&x={x}&y={y}&z={z}'
	return TileProvider(name=f'Google.{layer.name}', url=url, attribution='Google', max_zoom=20)

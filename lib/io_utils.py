import logging
from pathlib import Path
from typing import TYPE_CHECKING

import pandas
from async_lru import alru_cache
from travelpygame import (
	PointSet,
	load_or_fetch_per_player_submissions,
	load_points_async,
	validate_points,
)
from travelpygame.tpg_data import get_player_username
from travelpygame.util import format_point, try_set_index_name_col

from .settings import Settings

if TYPE_CHECKING:
	from geopandas import GeoDataFrame

logger = logging.getLogger(__name__)


def latest_file_matching_format_pattern(path: Path) -> Path:
	"""The file matching a formatting pattern with the highest number or letter. Only works with {} (without any position) and only really works in filenames, not in the directory part.

	If path does not contain {} then it will just return that and not check that it exists.
	"""
	if '{}' not in path.stem:
		return path
	# Replacing with * seems clunky, but I don't feel like implementing glob.translate myself
	return max(path.parent.glob(path.name.replace('{}', '*')))


_load_subs_cached = alru_cache(1)(load_or_fetch_per_player_submissions)


async def _load_by_username(username: str):
	settings = Settings()
	all_subs = await _load_subs_cached(settings.subs_per_player_path, settings.main_tpg_data_path)
	return all_subs[username]


async def load_path_or_player(
	path_or_name: str,
	lat_col: str | None = None,
	lng_col: str | None = None,
	crs_arg: str | None = None,
	name_col: str | None = None,
	*,
	force_unheadered: bool = False,
) -> tuple[str, 'GeoDataFrame']:
	if path_or_name.startswith('player:'):
		name = player_name = path_or_name.removeprefix('player:')
		username = await get_player_username(player_name)
		if username is None:
			raise KeyError(f'No player with display name {player_name} found')
		gdf = await _load_by_username(username)
	elif path_or_name.startswith('username:'):
		name = username = path_or_name.removeprefix('username:')
		gdf = await _load_by_username(username)
	else:
		path = Path(path_or_name)
		name = path.stem
		gdf = await load_points_async(
			path,
			lat_col,
			lng_col,
			crs=crs_arg or 'wgs84',
			has_header=False if force_unheadered else None,
		)
	assert gdf.crs, f'gdf {name} had no crs, which should never happen'
	if not gdf.crs.is_geographic:
		logger.warning('%s had non-geographic CRS %s, converting to WGS84', name, gdf.crs)
		gdf = gdf.to_crs('wgs84')

	gdf = gdf.set_index(name_col) if name_col else try_set_index_name_col(gdf)
	if isinstance(gdf.index, pandas.RangeIndex):
		logger.info('%s had default index, formatting points', path_or_name)
		gdf.index = pandas.Index(gdf.geometry.map(format_point))
	_, to_drop = validate_points(gdf, name_for_log=path_or_name)
	return name, gdf.drop(index=list(to_drop)) if to_drop else gdf


async def load_point_set_from_arg(
	path_or_name: str,
	lat_col: str | None = None,
	lng_col: str | None = None,
	crs_arg: str | None = None,
	name_col: str | None = None,
	projected_crs_arg: str | None = None,
	*,
	force_unheadered: bool = False,
) -> PointSet:
	name, gdf = await load_path_or_player(
		path_or_name, lat_col, lng_col, crs_arg, name_col, force_unheadered=force_unheadered
	)

	return PointSet(gdf, name, projected_crs_arg)

import asyncio
import logging
from collections.abc import Collection
from pathlib import Path
from typing import TYPE_CHECKING

import pandas
from async_lru import alru_cache
from tqdm.auto import tqdm
from travelpygame import (
	PointSet,
	get_all_point_sets,
	load_or_fetch_submission_summary,
	load_points,
	load_points_async,
	validate_points,
)
from travelpygame.tpg_data import PlayerName, PlayerUsername, get_player_username
from travelpygame.util import format_point, maybe_set_index_name_col, try_auto_set_index
from travelpygame.util.io_utils import dataframe_exts, known_geo_exts, maybe_load_geodataframe

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


_load_sub_summary_cached = alru_cache(1)(load_or_fetch_submission_summary)


@alru_cache(maxsize=1)
async def load_or_fetch_point_sets(path: Path | Settings | None = None) -> list[PointSet]:
	if not isinstance(path, Path):
		settings = path or Settings()
		path = settings.subs_per_player_path
	summary = await _load_sub_summary_cached(path)
	return get_all_point_sets(summary)


# TODO: Ideally, there would be a load_or_fetch_point_sets using the display names as the point set names too
# TODO: Ideally ideally, there would be a load by username/display name/Discord ID function that optionally just uses morphior_api.get_player_submissions


async def _load_by_username(
	settings_or_path: Path | Settings | None, username: PlayerUsername
) -> PointSet:
	for point_set in await load_or_fetch_point_sets(settings_or_path):
		if point_set.name == username:
			return point_set
	raise KeyError(f'Username {username} not found')


async def _load_by_display_name(
	settings_or_path: Path | Settings | None, name: PlayerName
) -> PointSet:
	# TODO: Arguably better to just warn/error if name could refer to one or two usernames
	username = await get_player_username(name)
	if username is None:
		raise KeyError(f'Player {name} not found')
	return await _load_by_username(settings_or_path, username)


# TODO: Load by Discord ID I guess


async def load_point_set_from_path(
	path: Path,
	lat_col: str | None = None,
	lng_col: str | None = None,
	crs_arg: str | None = None,
	point_name_col: str | None = None,
	projected_crs_arg: str | None = None,
	name: str | None = None,
	*,
	force_unheadered: bool = False,
):
	"""Point set name will be set to the path stem if `name` is None"""
	name = name or path.stem
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

	gdf, new_name_col = maybe_set_index_name_col(gdf, point_name_col, path)
	if not new_name_col:
		# Should this always be what we want to do?
		logger.info('%s had default index, formatting points', path)
		gdf.index = pandas.Index(gdf.geometry.map(format_point), name='name')

	_, to_drop = validate_points(gdf, name_for_log=path)
	if to_drop:
		gdf = gdf.drop(index=list(to_drop))
	return PointSet(gdf, name, projected_crs_arg)


async def load_point_set_from_arg(
	path_or_name: str,
	lat_col: str | None = None,
	lng_col: str | None = None,
	crs_arg: str | None = None,
	point_name_col: str | None = None,
	projected_crs_arg: str | None = None,
	settings_or_path: Path | Settings | None = None,
	*,
	force_unheadered: bool = False,
) -> PointSet:
	if path_or_name.startswith('player:'):
		player_name = path_or_name.removeprefix('player:')
		return await _load_by_display_name(settings_or_path, player_name)
	if path_or_name.startswith('username:'):
		username = path_or_name.removeprefix('username:')
		return await _load_by_username(settings_or_path, username)
	return await load_point_set_from_path(
		Path(path_or_name),
		lat_col,
		lng_col,
		crs_arg,
		point_name_col,
		projected_crs_arg,
		# could have another parameter for custom point set name but eh
		force_unheadered=force_unheadered,
	)


def _listdir_sync(path: Path):
	return list(path.iterdir())


async def listdir_async(path: Path):
	return await asyncio.to_thread(_listdir_sync, path)


def load_point_sets_from_folder(
	folder: Path,
	extensions: Collection[str] | None = None,
	*,
	force_all: bool = False,
	use_tqdm: bool = True,
):
	"""Loads a list of PointSet objects from a folder. Will always load .geojson/.gpkg files, additional extensions can be specified."""
	frames: dict[Path, GeoDataFrame] = {}
	geo_exts = {*known_geo_exts, *extensions} if extensions else known_geo_exts
	with tqdm(_listdir_sync(folder), f'Loading files in {folder.stem}', disable=not use_tqdm) as t:
		for child in t:
			t.set_postfix(child=child.stem)
			if child.is_dir():
				continue
			ext = child.suffix[1:].lower()
			# TODO: Is there a better way to know whether a file is one we want before we try loading it? Catching any sort of unknown file error relies on having a specific GeoPandas engine, pyogrio uses pyogrio.errors.DataSourceError and fiona uses fiona.errors.DriverError
			if ext in dataframe_exts or ext in geo_exts:
				gdf = load_points(child, use_tqdm=False)
				gdf = try_auto_set_index(gdf)
				frames[child] = gdf
			elif force_all:
				gdf = maybe_load_geodataframe(child, use_tqdm=False)
				if gdf is None:
					logger.debug('Skipping unsupported file %s', child)
				else:
					gdf = try_auto_set_index(gdf)
					frames[child] = gdf
	return [PointSet(gdf, path.stem) for path, gdf in frames.items()]

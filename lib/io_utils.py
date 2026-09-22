import asyncio
import logging
from collections.abc import Collection, Hashable
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas
import shapely
from async_lru import alru_cache
from pyproj import CRS
from tqdm.auto import tqdm
from travelpygame.point_set import PointSet, validate_points
from travelpygame.submission_data import (
	AllSubmissionData,
	SubmissionSummary,
	convert_cellery_geojson,
	get_all_official_data,
	get_all_point_sets,
)
from travelpygame.util import (
	format_point,
	get_polygons,
	load_points,
	load_points_async,
	maybe_set_index_name_col,
	read_dataframe,
	read_geodataframe,
	try_auto_set_index,
)
from travelpygame.util.io_utils import dataframe_exts, known_geo_exts, maybe_load_geodataframe

from .settings import Settings

if TYPE_CHECKING:
	from geopandas import GeoDataFrame
	from travelpygame.tpg_data import PlayerName, PlayerUsername

logger = logging.getLogger(__name__)

_wgs84_crs = CRS.from_epsg(4326)


def latest_file_matching_format_pattern(path: Path) -> Path:
	"""The file matching a formatting pattern with the highest number or letter. Only works with {} (without any position) and only really works in filenames, not in the directory part.

	If path does not contain {} then it will just return that and not check that it exists.
	"""
	if '{}' not in path.stem:
		return path
	# Replacing with * seems clunky, but I don't feel like implementing glob.translate myself
	return max(path.parent.glob(path.name.replace('{}', '*')))


def load_aliases(path: Path | Settings | None) -> dict['PlayerName', 'PlayerName']:
	if isinstance(path, Settings):
		path = path.aliases_path
	if path is None:
		return {}
	df = read_dataframe(path)
	if 'name' not in df.columns or 'alias' not in df.columns:
		logger.info(
			'Could not load alias file %s, missing name/alias columns. Columns found were: %s',
			path,
			df.columns,
		)
		return {}
	return df.set_index('alias')['name'].to_dict()  # ty: ignore[invalid-return-type] #trust me bro


@alru_cache
async def get_all_submission_data(
	cellery_export_path_pattern: Path | None,
	rounding: int | None = 6,
	aliases_path: Path | None = None,
	*,
	forbid_extra: bool = False,
) -> AllSubmissionData:
	"""Gets submission/round info from exported data from Cellery's site if we have that, or official TPG API if not."""
	# TODO: We should be saving the result somewhere
	if cellery_export_path_pattern:
		try:
			path = latest_file_matching_format_pattern(cellery_export_path_pattern)
		except ValueError:
			pass
		else:
			aliases = await asyncio.to_thread(load_aliases, aliases_path)
			return await convert_cellery_geojson(path, rounding, aliases, forbid_extra=forbid_extra)
	logger.info('No TPG tracker export found, using official TPG only')
	return await get_all_official_data(rounding, forbid_extra=forbid_extra)


async def get_submission_summary(
	cellery_export_path_pattern: Path | None,
	rounding: int | None = 6,
	aliases_path: Path | None = None,
	*,
	forbid_extra: bool = False,
) -> SubmissionSummary:
	data = await get_all_submission_data(
		cellery_export_path_pattern, rounding, aliases_path, forbid_extra=forbid_extra
	)
	return SubmissionSummary(data.grouped_submissions)


async def load_or_fetch_submission_summary(
	path: Path | None,
	cellery_export_path_pattern: Path | None = None,
	rounding: int | None = 6,
	aliases_path: Path | None = None,
	*,
	forbid_extra: bool = False,
) -> SubmissionSummary:
	"""Loads submission summary from a path if provided and exists, or imports it (and then saves if path is provided)."""
	if path and await asyncio.to_thread(path.info.is_file):
		try:
			return await asyncio.to_thread(SubmissionSummary.from_file, path)
		except FileNotFoundError:
			pass

	sub_summary = await get_submission_summary(
		cellery_export_path_pattern, rounding, aliases_path, forbid_extra=forbid_extra
	)
	if path:
		await asyncio.to_thread(sub_summary.save_to_file, path)
	return sub_summary


async def load_or_fetch_point_sets(
	path: Path | Settings | None = None,
	min_datetime: datetime | None = None,
	min_count: int | None = None,
) -> list[PointSet]:
	if isinstance(path, Path):
		export_path = None
		aliases_path = None
	else:
		settings = path or Settings()
		path = settings.submission_summary_path
		export_path = settings.tpg_export_path
		aliases_path = settings.aliases_path
	summary = await load_or_fetch_submission_summary(
		path, export_path, aliases_path=aliases_path, forbid_extra=True
	)
	return get_all_point_sets(summary.per_player, min_datetime, min_count)


def set_name_col(gdf: 'GeoDataFrame', name_col: Hashable | None, log_context: Any = None):
	gdf, new_name_col = maybe_set_index_name_col(gdf, name_col, log_context)
	if not new_name_col:
		# Should this always be what we want to do?
		logger.info('%s had default index, formatting points', log_context)
		gdf.index = pandas.Index(gdf.geometry.map(format_point), name='name')
	return gdf


async def _load_by_username(
	settings_or_path: Path | Settings | None, username: 'PlayerUsername'
) -> PointSet:
	for point_set in await load_or_fetch_point_sets(settings_or_path):
		if point_set.name == username:
			return point_set
	raise KeyError(f'Username {username} not found')


# TODO: Load by Discord ID I guess? Although that would only be usable with SubmissionSummary created from main TPG data


async def load_point_set_from_path(
	path: Path,
	lat_col: str | None = None,
	lng_col: str | None = None,
	crs_arg: str | None = None,
	point_name_col: str | None = None,
	projected_crs_arg: str | None = None,
	name: str | None = None,
	*,
	force_wgs84: bool = False,
	force_unheadered: bool = False,
) -> PointSet:
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
	if force_wgs84 and not gdf.crs.equals(_wgs84_crs):
		logger.info('Converting %s from %s to WGS84', name, gdf.crs)
		gdf = gdf.to_crs(_wgs84_crs)
	elif not gdf.crs.is_geographic:
		logger.warning('%s had non-geographic CRS %s, converting to WGS84', name, gdf.crs)
		gdf = gdf.to_crs(_wgs84_crs)

	gdf = set_name_col(gdf, point_name_col, path)

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
	force_wgs84: bool = False,
	force_unheadered: bool = False,
) -> PointSet:
	if path_or_name.startswith('player:'):
		username = path_or_name.removeprefix('player:')
		return await _load_by_username(settings_or_path, username)
	return await load_point_set_from_path(
		Path(path_or_name),
		lat_col,
		lng_col,
		crs_arg,
		point_name_col,
		projected_crs_arg,
		# could have another parameter for custom point set name but eh
		force_wgs84=force_wgs84,
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


def load_polygons(path: Path) -> shapely.Polygon | shapely.MultiPolygon | None:
	gdf = read_geodataframe(path)
	polygons = get_polygons(gdf)
	if not polygons:
		return None
	return polygons[0] if len(polygons) == 1 else shapely.MultiPolygon(polygons)

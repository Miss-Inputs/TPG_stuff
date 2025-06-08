import asyncio
import warnings
from collections.abc import Hashable, Iterable
from pathlib import Path
from typing import Any

import geopandas
import pandas
from pyzstd import ZstdFile
from tqdm.auto import tqdm


def latest_file_matching_format_pattern(path: Path) -> Path:
	"""The file matching a formatting pattern with the highest number or letter. Only works with {} (without any position) and only really works in filenames, not in the directory part.

	If path does not contain {} then it will just return that and not check that it exists.
	"""
	if '{}' not in path.stem:
		return path
	# Replacing with * seems clunky, but I don't feel like implementing glob.translate myself
	return max(path.parent.glob(path.name.replace('{}', '*')))


def read_dataframe_pickle(path: Path, **tqdm_kwargs) -> pandas.DataFrame:
	"""Reads a pickled DataFrame from a file path, displaying a progress bar for long files.

	Raises:
		TypeError: If the pickle file does not actually contain a DataFrame.
	"""
	size = path.stat().st_size
	desc = tqdm_kwargs.pop('desc', f'Reading {path}')
	leave = tqdm_kwargs.pop('leave', False)
	with (
		path.open('rb') as f,
		tqdm.wrapattr(
			f, 'read', total=size, bytes=True, leave=leave, desc=desc, **tqdm_kwargs
		) as t,
	):
		# Don't really need to use pandas.read_pickle here, but also don't really need not to
		obj = pandas.read_pickle(t)  # type:ignore[blah] #supposedly, the wrapattr stream isn't entirely compatible with what pandas.read_pickle (or pickle.load) wants, but it's fine
	if not isinstance(obj, pandas.DataFrame):
		raise TypeError(f'Unpickled object was {type(obj)}, DataFrame expected')
	return obj


async def read_dataframe_pickle_async(path: Path, **tqdm_kwargs) -> pandas.DataFrame:
	"""Reads a pickled DataFrame from a file path in a separate thread, displaying a progress bar for long files."""
	# Could use aiofiles, but eh
	return await asyncio.to_thread(read_dataframe_pickle, path, **tqdm_kwargs)


def format_path(path: Path, n: Any):
	"""Replaces {} in a path stem with n."""
	return path.with_stem(path.stem.format(n))


class UnsupportedFileException(Exception):
	"""File type was not supported."""


def read_geodataframe(path: Path) -> geopandas.GeoDataFrame:
	"""Reads a GeoDataFrame from a path, which can be compressed using Zstandard.

	Raises:
		TypeError: If path ever contains something other than a GeoDataFrame.
	"""
	if path.suffix.lower() == '.zst':
		with (
			ZstdFile(path, 'r') as zst,
			warnings.catch_warnings(category=RuntimeWarning, action='ignore'),
		):
			# shut up nerd I don't care if it has a GPKG application_id or whatever
			gdf = geopandas.read_file(zst)
	else:
		gdf = geopandas.read_file(path)
	if not isinstance(gdf, geopandas.GeoDataFrame):
		# Not sure if this ever happens, or if the type hint is just like that
		raise TypeError(f'Expected {path} to contain GeoDataFrame, got {type(gdf)}')
	return gdf


async def read_geodataframe_async(path: Path) -> geopandas.GeoDataFrame:
	"""Reads a GeoDataFrame from a path in another thread, which can be compressed using Zstandard."""
	return await asyncio.to_thread(read_geodataframe, path)


def geodataframe_to_csv(
	gdf: geopandas.GeoDataFrame,
	path: Path,
	lat_col_name: Hashable = 'lat',
	lng_col_name: Hashable = 'lng',
	*,
	include_z: bool = False,
	insert_before: bool = False,
	index: bool = True,
):
	"""Outputs a GeoDataFrame to CSV with lat and lng columns."""
	df = gdf.drop(columns=gdf.active_geometry_name)
	coords = gdf.get_coordinates(include_z=include_z)
	a = [df, coords[coords.columns[::-1]]]  # generally we want lat before lng
	df = pandas.concat(reversed(a) if insert_before else a, axis='columns')
	df = df.rename(columns={'x': lng_col_name, 'y': lat_col_name})
	df.to_csv(path, index=index)


dataframe_compressed_exts = {'gz', 'bz2', 'zip', 'xz', 'zst'}
csv_exts = {'csv', 'tsv'}
excel_exts = {'xls', 'xlsx', 'xlsm', 'xlsb', 'odf', 'ods', 'odt'}
pickle_exts = {'pickle', 'pkl'}
"""hmm there's nothing really that requires pickle data to be any sort of extension, but might as well be one of those"""
other_df_readers = {
	'feather': pandas.read_feather,
	'orc': pandas.read_orc,
	'parquet': pandas.read_parquet,
}
"""I don't know what these really do or if they work as expected because I don't use these file types but they exist and seem straightforward"""
dataframe_exts = csv_exts | excel_exts | pickle_exts | other_df_readers.keys()


def read_dataframe(
	path: Path,
	ext: str | None = None,
	csv_encoding: str = 'utf-8',
	csv_sep: str | None = None,
	sheet_name: int | str = 0,
	*,
	has_header: bool | None = None,
) -> pandas.DataFrame:
	"""Loads a DataFrame from a path, using the right loader for the file extension.

	Arguments:
		path: File path
		ext: Override file format, or leave as None to autodetect from extension
		csv_encoding: If csv, text encoding to use
		csv_sep: If csv, field separator to use, defaults to comma (or tab if extension is tsv)
		sheet_name: If Excel, sheet name/index to use
		has_header: If csv/Excel, the data has column headers, defaults to infer (or True for Excel)

	Raises:
		UnsupportedFileException: If not a known file type.
	"""
	if not ext:
		suffixes = path.suffixes
		ext = suffixes[-1][1:].lower()
		if ext in dataframe_compressed_exts:
			# not sure if read_csv/read_excel like compressed files, oh well
			ext = suffixes[-2][1:].lower()

	if ext in csv_exts:
		csv_sep = csv_sep or ('\t' if ext == 'tsv' else ',')
		# @.@ argh why
		if has_header is None:
			header = 'infer'
		elif has_header:
			header = 1
		else:
			header = None
		return pandas.read_csv(path, sep=csv_sep, encoding=csv_encoding, header=header)
	if ext in excel_exts:
		header_row = 0 if has_header in {True, None} else None
		# @.@ is there a better way to write that…
		return pandas.read_excel(path, sheet_name, header=header_row)
	if ext in pickle_exts:
		return read_dataframe_pickle(path)

	if ext in other_df_readers:
		return other_df_readers[ext](path)
	# TODO: fwf (as .txt maybe?), hdf (.h5), html, json, sas (.sas7bdat, whatever that is), spss (spss_data.sav? I dunno), stata (.dta), xml probably
	raise UnsupportedFileException(f'Unknown extension: {ext}')


async def read_dataframe_async(
	path: Path,
	ext: str | None = None,
	csv_encoding: str = 'utf-8',
	csv_sep: str | None = None,
	sheet_name: int | str = 0,
	*,
	has_header: bool | None = None,
) -> pandas.DataFrame:
	"""Loads a DataFrame from a path, using the right loader for the file extension.

	Arguments:
		path: File path
		ext: Override file format, or leave as None to autodetect from extension
		csv_encoding: If csv, text encoding to use
		csv_sep: If csv, field separator to use, defaults to comma (or tab if extension is tsv)
		sheet_name: If Excel, sheet name/index to use
		has_header: If csv/Excel, the data has column headers, defaults to infer (or True for Excel)
	"""
	return await asyncio.to_thread(
		read_dataframe, path, ext, csv_encoding, csv_sep, sheet_name, has_header=has_header
	)


def find_first_matching_column(df: pandas.DataFrame, col_names: Iterable[Hashable]):
	for name in col_names:
		if name in df.columns:
			return name
	return None


latitude_column_names = {'lat', 'latitude', 'y', 0}
longitude_column_names = {'lng', 'lon', 'longitude', 'x', 1}


def _load_df_as_points(
	path: Path,
	latitude_column_name: Hashable | None = None,
	longitude_column_name: Hashable | None = None,
	crs: Any = 'wgs84',
	ext: str | None = None,
	*,
	has_header: bool | None = None,
	keep_lnglat_cols: bool = False,
) -> geopandas.GeoDataFrame:
	df = read_dataframe(path, ext, has_header=has_header)
	latitude_column_name = latitude_column_name or find_first_matching_column(
		df, latitude_column_names
	)
	longitude_column_name = longitude_column_name or find_first_matching_column(
		df, longitude_column_names
	)

	if latitude_column_name is None or longitude_column_name is None:
		raise UnsupportedFileException(
			f'This file does not seem to contain any columns for points (found: {df.columns})'
		)
	lat = df[latitude_column_name]
	lng = df[longitude_column_name]

	if not keep_lnglat_cols:
		df = df.drop(columns=[latitude_column_name, longitude_column_name])
	geometry = geopandas.points_from_xy(lng, lat, crs=crs)
	return geopandas.GeoDataFrame(df, geometry=geometry)


def load_points(
	path: Path,
	latitude_column_name: Hashable | None = None,
	longitude_column_name: Hashable | None = None,
	crs: Any = 'wgs84',
	ext: str | None = None,
	*,
	has_header: bool | None = None,
	keep_lnglat_cols: bool = False,
):
	"""Loads a file containing coordinates as a GeoDataFrame, either as a DataFrame (csv/Excel/pickle/etc) containing longitude and latitude columns, or a file directly supported by geopandas"""
	if not ext:
		suffixes = path.suffixes
		ext = suffixes[-1][1:].lower()
		if ext in dataframe_compressed_exts:
			ext = suffixes[-2][1:].lower()
	if ext not in dataframe_exts:
		return read_geodataframe(path)
	return _load_df_as_points(
		path,
		latitude_column_name,
		longitude_column_name,
		crs,
		ext,
		has_header=has_header,
		keep_lnglat_cols=keep_lnglat_cols,
	)


async def load_points_async(
	path: Path,
	latitude_column_name: Hashable | None = None,
	longitude_column_name: Hashable | None = None,
	crs: Any = 'wgs84',
	ext: str | None = None,
	*,
	has_header: bool | None = None,
	keep_lnglat_cols: bool = False,
):
	"""Loads a file containing coordinates as a GeoDataFrame asynchronously, either as a DataFrame (csv/Excel/pickle/etc) containing longitude and latitude columns, or a file directly supported by geopandas"""
	if not ext:
		suffixes = path.suffixes
		ext = suffixes[-1][1:].lower()
		if ext in dataframe_compressed_exts:
			ext = suffixes[-2][1:].lower()
	if ext not in dataframe_exts:
		return await read_geodataframe_async(path)
	return await asyncio.to_thread(
		_load_df_as_points,
		path,
		latitude_column_name,
		longitude_column_name,
		crs,
		ext,
		has_header=has_header,
		keep_lnglat_cols=keep_lnglat_cols,
	)

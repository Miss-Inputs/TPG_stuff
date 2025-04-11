import warnings
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


def format_path(path: Path, n: Any):
	"""Replaces {} in a path stem with n."""
	return path.with_stem(path.stem.format(n))


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

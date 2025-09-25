import asyncio
from pathlib import Path
from typing import Any


def latest_file_matching_format_pattern(path: Path) -> Path:
	"""The file matching a formatting pattern with the highest number or letter. Only works with {} (without any position) and only really works in filenames, not in the directory part.

	If path does not contain {} then it will just return that and not check that it exists.
	"""
	if '{}' not in path.stem:
		return path
	# Replacing with * seems clunky, but I don't feel like implementing glob.translate myself
	return max(path.parent.glob(path.name.replace('{}', '*')))

def format_path(path: Path, n: Any):
	"""Replaces {} in a path stem with n."""
	return path.with_stem(path.stem.format(n))


class UnsupportedFileException(Exception):
	"""File type was not supported."""


async def read_lines_async(path: Path, encoding: str = 'utf-8'):
	text = await asyncio.to_thread(path.read_text, encoding)
	return [line for line in text.splitlines() if line]

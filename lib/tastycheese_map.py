from pathlib import Path
from typing import Any, Literal

import pydantic_core
import requests
from pydantic import BaseModel, Field, TypeAdapter
from tqdm.auto import tqdm

from lib.io_utils import format_path, latest_file_matching_format_pattern

user_agent = "Hello it's me Miss Inputs I'm poking around in your site"


class TPGRound(BaseModel):
	number: int
	"""Round number, starting with 1 and incrementing constantly."""
	latitude: float
	longitude: float
	water: bool
	ongoing: bool
	country: str | None
	"""Two letter uppercase ISO 3166-1 code, or None if the round is not in any particular country."""


_round_list_adapter = TypeAdapter(list[TPGRound])


def get_tpg_rounds(session: requests.Session | None = None, timeout: int = 10) -> list[TPGRound]:
	"""Gets a list of all rounds in Travel Pics Game, not including side games, etc."""
	url = 'https://tpg.tastedcheese.site/api/getRounds/'
	headers = {'User-Agent': user_agent}

	response = (
		session.get(url, timeout=timeout, headers=headers)
		if session
		else requests.get(url, timeout=timeout, headers=headers)
	)
	response.raise_for_status()
	return _round_list_adapter.validate_json(response.text)


class TPGSubmission(BaseModel):
	discord_id: str
	name: str
	"""Discord display name of the player who submitted this."""
	username: str | None
	"""Discord username of the player who submitted this, can be None for deleted accounts."""
	latitude: float
	"""WGS84 latitude of the picture. Serialized in JSON as string."""
	longitude: float
	"""WGS84 longitude of the picture. Serialized in JSON as string."""
	place: int
	"""Placement of this picture so far, starting at 1st place."""
	fivek: bool = Field(validation_alias='5k')
	"""Whether this picture counted as a 5K or not."""
	antipode_5k: bool


_sub_list_adapter = TypeAdapter(list[TPGSubmission])


def get_round_submissions(
	round_num: int | Literal['ongoing'], session: requests.Session | None = None, timeout: int = 10
) -> list[TPGSubmission]:
	"""Gets all the submissions for a round. The current round number will not return anything, "ongoing" must be used instead of the number to get the submissions that have been added to the map so far.
	Requesting a round that does not exist will return status = OK, but not have any submissions yet.

	Arguments:
		round_num: A number of a previous round, or "ongoing" for the current one.
		session: Optional requests.Session if you have one, otherwise does not use a session. Recommended if you are using this in a loop, etc.
		timeout: Request timeout in seconds, defaults to 10 seconds.
	"""
	url = 'https://tpg.tastedcheese.site/api/getSubmissions/'
	headers = {'User-Agent': user_agent}
	params = {'round': round_num}

	response = (
		session.get(url, timeout=timeout, headers=headers, params=params)
		if session
		else requests.get(url, timeout=timeout, headers=headers, params=params)
	)
	response.raise_for_status()
	return _sub_list_adapter.validate_json(response.text)


def get_all_submissions(
	max_round_num: int | None = None, session: requests.Session | None = None, timeout: int = 10
) -> dict[int, list[TPGSubmission]]:
	"""Gets all Travel Pics Game submissions that have been added to the map.

	Arguments:
		max_round_num: Latest round number if known, this will also display the progress bar better.
		session: Optional requests.Session if you have one, otherwise creates a new one.
		timeout: Request timeout in seconds, defaults to 10 seconds.

	Returns:
		{round number: list of submissions for that round}
	"""
	if session is None:
		session = requests.Session()

	subs: dict[int, list[TPGSubmission]] = {}
	round_num = 1
	with tqdm(total=max_round_num) as t:
		while (max_round_num is None) or (round_num <= max_round_num):
			round_subs = get_round_submissions(
				'ongoing' if round_num == max_round_num else round_num, session, timeout
			)
			t.update()
			if not round_subs:
				break
			subs[round_num] = round_subs
			round_num += 1
	return subs


def get_submissions(
	path: Path | None = None, max_round_num: int | None = None
) -> dict[int, list[dict[str, Any]]]:
	"""Gets all submissions and optionally saves it to a file as JSON.

	Arguments:
		path: Path which doesn't need to exist yet. {} will be replaced with the number of the latest round.
		max_round_num: Latest round number if known, this will also display the progress bar better.

	Returns:
		{round number: list of submissions converted to dict}."""
	all_submissions = get_all_submissions(max_round_num)
	if path:
		j = pydantic_core.to_json(all_submissions)
		path = format_path(path, max(all_submissions.keys()))
		path.write_bytes(j)
	return {
		round_num: [s.model_dump() for s in subs] for round_num, subs in all_submissions.items()
	}


_submission_json_adapter = TypeAdapter(dict[int, list[dict[str, Any]]])


def load_or_get_submissions(
	path: Path | None = None, max_round_num: int | None = None
) -> dict[int, list[dict[str, Any]]]:
	"""If path is provided, loads all submissions from that file if it exists (and if max_round_num is known, that the file is up to the most recent round), or gets them and saves them as JSON if not. Recommended to avoid spamming the endpoint every time.
	If path is None, behaves the same as get_submissions.

	Arguments:
		path: Path to a JSON. If it doesn't exist, {} will be replaced with the number of the latest round.
		max_round_num: Latest round number if known, this will also display the progress bar better.

	Returns:
		{round number: list of submissions converted to dict}.
	"""
	if not path:
		return get_submissions(path, max_round_num)
	try:
		latest_path = latest_file_matching_format_pattern(path)
	except ValueError:
		# file not there
		return get_submissions(path, max_round_num)

	if max_round_num:
		# Ensure we create a new file if we have more rounds
		latest_path_stem = path.stem.format(max_round_num)
		if latest_path.stem != latest_path_stem:
			latest_path = latest_path.with_stem(latest_path_stem)
	else:
		latest_path = latest_file_matching_format_pattern(path)

	try:
		contents = latest_path.read_bytes()
		subs = _submission_json_adapter.validate_json(contents)
	except FileNotFoundError:
		subs = get_submissions(path, max_round_num)
	return subs


class TPGPlayer(BaseModel):
	discord_id: int
	name: str
	username: str


_player_list_adapter = TypeAdapter(list[TPGPlayer])


def get_players(session: requests.Session | None = None, timeout: int = 10):
	url = 'https://tpg.tastedcheese.site/api/getPlayers/'
	headers = {'User-Agent': user_agent}

	get = session.get if session else requests.get
	response = get(url, timeout=timeout, headers=headers)
	response.raise_for_status()
	return _player_list_adapter.validate_json(response.text)


# https://tpg.tastedcheese.site/api/getUserSubmissions/?name={name}: [{latitude, longitude, label e.g. "Round 3", "Rounds 4, 33"}] requires a display name and not a username though

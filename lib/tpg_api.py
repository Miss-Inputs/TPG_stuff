"""API used by the new site, https://travelpicsgame.com"""

from collections.abc import Sequence
from datetime import datetime

from aiohttp import ClientSession
from pydantic import BaseModel, TypeAdapter

user_agent = 'https://github.com/Miss-Inputs/TPG_stuff'


class TPGRound(BaseModel, extra='forbid'):
	number: int
	"""Round number, starting with 1 and incrementing constantly."""
	latitude: float
	longitude: float
	water: bool
	ongoing: bool
	country: str | None
	"""Two letter uppercase ISO 3166-1 code, or None if the round is not in any particular country."""
	# These two fields are strings containing ints but we automagically convert them to dates
	start_timestamp: datetime | None
	end_timestamp: datetime | None
	season: int


_round_list_adapter = TypeAdapter(list[TPGRound])


async def get_rounds(session: ClientSession | None = None) -> Sequence[TPGRound]:
	if session is None:
		async with ClientSession() as sesh:
			return await get_rounds(sesh)
	url = 'https://travelpicsgame.com/api/v1/rounds'
	async with session.get(url, headers={'User-Agent': user_agent}) as response:
		response.raise_for_status()
		text = await response.text()
	return _round_list_adapter.validate_json(text)

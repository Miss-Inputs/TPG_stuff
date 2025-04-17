#!/usr/bin/env python3
"""If tastyCheese tracker is not available yet or incomplete for a particular round, export the submission tracker and use this to convert it"""

import logging
from argparse import ArgumentParser
from dataclasses import dataclass
from itertools import islice
from pathlib import Path
from xml.etree import ElementTree

import pydantic_core

from lib.tastycheese_map import get_players

logger = logging.getLogger(__name__)


@dataclass
class Submission:
	name: str
	rank: int
	lat: float
	lng: float


class SubmissionError(Exception): ...


def _parse_placemark(placemark: ElementTree.Element):
	name = placemark.findtext('{http://www.opengis.net/kml/2.2}name') or ''
	# description: Link to the Discord message, which doesn't do much for us
	point = placemark.find('{http://www.opengis.net/kml/2.2}Point')
	if point is None:
		raise SubmissionError(f"{name}'s submission is pointless!")

	coordinates_text = point.findtext('{http://www.opengis.net/kml/2.2}coordinates')
	if not coordinates_text:
		raise SubmissionError(f"{name}'s submission has a point with no coordinates?")
	coordinates = coordinates_text.strip().split(',')
	# Third element is probably elevation but is unused and always 0 for TPG
	lng = float(coordinates[0])
	lat = float(coordinates[1])
	return name, lat, lng


def parse_submission_kml(path: Path | ElementTree.ElementTree):
	tree = path if isinstance(path, ElementTree.ElementTree) else ElementTree.parse(path)
	#I fucking hate namespaces!!!! Fuck you XML!!!
	doc = tree.find('{http://www.opengis.net/kml/2.2}Document', {})
	assert doc is not None, f'{path} has no document'
	folder = doc.find('{http://www.opengis.net/kml/2.2}Folder')
	assert folder is not None, f'{path} has no folder'

	placemark_iter = folder.iter('{http://www.opengis.net/kml/2.2}Placemark')
	# Assume the first two elements are the round location itself and the antipode respectively
	_round_placemark, _antipode_placemark = islice(placemark_iter, 2)
	# But we will ignore them for now

	for i, placemark in enumerate(placemark_iter, 1):
		try:
			name, lat, lng = _parse_placemark(placemark)
		except SubmissionError:
			logger.exception('Unexpected submission error:')
			continue

		# TODO: * indicates ties but we'll figure that out some other day
		yield Submission(name.removesuffix(' *'), i, lat, lng)


def main() -> None:
	argparser = ArgumentParser()
	argparser.add_argument('kml_path', type=Path)
	argparser.add_argument('round_num', type=int)
	args = argparser.parse_args()
	path: Path = args.kml_path
	round_num: int = args.round_num

	#The KML only includes the display name, so attempt to map it back to username
	#Note that this doesn't always work and so you will probably have to fix this manually anyway
	users = {player.name: (player.username, player.discord_id) for player in get_players()}
	logger.info('Got username for %d players', len(users))

	subs = []
	for sub in parse_submission_kml(path):
		username, discord_id = users.get(sub.name, (sub.name, None))
		subs.append(
			{
				'discord_id': discord_id,
				'name': sub.name,
				'username': username,
				'latitude': sub.lat,
				'longitude': sub.lng,
				'place': sub.rank,
				# TODO: I am being lazy and just leaving 5K/antipode 5K as false, but we do have that information in the KML so it is possible (I guess we would measure ourselves what is <= 100m from the target or <= 5km from the antipode)
				'fivek': False,
				'antipode_5k': False,
			}
		)

	output_json = pydantic_core.to_json({round_num: subs})
	output_path = path.with_suffix('.json')
	output_path.write_bytes(output_json)


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	main()

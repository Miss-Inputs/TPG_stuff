#!/usr/bin/env python3
"""Simulate rounds as though all known players are present and submitting what is known to be their best pic."""

import asyncio
import logging
import re
from argparse import ArgumentParser, BooleanOptionalAction
from collections.abc import Collection, Mapping
from pathlib import Path
from typing import TYPE_CHECKING

import shapely
from aiohttp import ClientSession
from pandas import DataFrame, Index, RangeIndex
from tqdm.contrib.logging import logging_redirect_tqdm
from travelpygame import (
	PointSet,
	Round,
	ScoringOptions,
	load_or_fetch_per_player_submissions,
	load_points,
	load_rounds,
	main_tpg_scoring,
)
from travelpygame.random_points import random_point_in_bbox, random_points_in_poly
from travelpygame.simulation import (
	SimulatedStrategy,
	Simulation,
	get_player_podium_or_losing_points,
	get_player_summary,
	get_round_summary,
)
from travelpygame.tpg_data import PlayerUsername, get_player_display_names, rounds_to_json
from travelpygame.util import (
	format_dataframe,
	format_point,
	format_xy,
	output_dataframe,
	output_geodataframe,
	read_geodataframe,
	try_auto_set_index,
)

from lib.io_utils import load_point_sets_from_folder
from lib.settings import Settings

if TYPE_CHECKING:
	from geopandas import GeoDataFrame


def compare_rounds(old_round: Round, new_round: Round, name: str | None):
	if old_round.submissions and old_round.is_scored:
		old_winner = max(old_round.submissions, key=lambda sub: sub.score or float('nan'))
		if old_winner.name != new_round.submissions[0].name:
			# TODO: Output this somewhere
			print(
				f'{old_round.name}: Winner was {old_round.submissions[0].name}, now {new_round.submissions[0].name}'
			)
	if name:
		old_sub = next((sub for sub in old_round.submissions if sub.name == name), None)
		if not old_sub:
			return
		new_sub = next(sub for sub in new_round.submissions if sub.name == name)
		if old_sub.rank is not None and new_sub.rank is not None and old_sub.rank != new_sub.rank:
			print(f'{old_round.name}: Placing changed from {old_sub.rank} to {new_sub.rank}')
		if (old_sub.latitude != new_sub.latitude) or (old_sub.longitude != new_sub.longitude):
			old_desc = old_sub.description or format_xy(old_sub.longitude, old_sub.latitude)
			new_desc = new_sub.description or format_xy(new_sub.longitude, new_sub.latitude)
			print(f'{old_round.name}: Previously submitted {old_desc}, now would submit {new_desc}')


def get_simulation(
	existing_rounds: list[Round] | None,
	point_sets: Collection[PointSet],
	scoring: ScoringOptions,
	strategy: SimulatedStrategy,
	targets_path: Path | None,
	num_random_rounds: int | None,
	region_path: Path | None,
	single_point: shapely.Point | None,
	*,
	use_haversine: bool,
) -> Simulation:
	rounds: dict[str, shapely.Point] = {}
	order: dict[str, int] = {}

	if existing_rounds:
		for r in existing_rounds:
			round_name = r.name or f'Round {r.number}'
			rounds[round_name] = r.target
			order[round_name] = r.number
	elif targets_path:
		targets = load_with_name(targets_path)
		# Should we log about stuff that isn't a Point?
		rounds = {
			str(index): point
			for index, point in targets.geometry.items()
			if isinstance(point, shapely.Point)
		}
	elif num_random_rounds:
		if region_path:
			region = read_geodataframe(region_path)
			# TODO: This wants a random seed argument
			points = random_points_in_poly(
				region, num_random_rounds, use_tqdm=True, desc='Generating random points'
			)
		else:
			points = [random_point_in_bbox(-180, -90, 180, 90) for _ in range(num_random_rounds)]
		rounds = {format_point(point): point for point in points}
	elif single_point:
		rounds = {format_point(single_point): single_point}
	else:
		raise RuntimeError('You have no rounds to be simulated')
	return Simulation(
		rounds, order or None, point_sets, scoring, strategy, use_haversine=use_haversine
	)


def output_results(
	new_rounds: list[Round],
	existing_rounds: list[Round] | None,
	name: str | None,
	round_summary_path: Path | None,
	player_summary_path: Path | None,
	podium_rounds_path: Path | None,
	losing_rounds_path: Path | None,
):
	if existing_rounds:
		for result in new_rounds:
			r = next((r for r in existing_rounds if r.name == result.name), None)
			if r:
				compare_rounds(r, result, name)

	round_summary = get_round_summary(new_rounds, name)
	print(round_summary)
	if round_summary_path:
		round_summary.to_csv(round_summary_path)

	# TODO: More detailed stats here, like maybe a whole entire leaderboard
	player_summary = get_player_summary(new_rounds)
	print(player_summary)
	if player_summary_path:
		player_summary.to_csv(player_summary_path)

	if name and (podium_rounds_path or losing_rounds_path):
		podiumming, losing = get_player_podium_or_losing_points(new_rounds, name)
		if podium_rounds_path and not podiumming.empty:
			output_geodataframe(podiumming, podium_rounds_path)
		if losing_rounds_path and not losing.empty:
			output_geodataframe(losing, losing_rounds_path)


def load_with_name(path: Path | str):
	"""Loads points from `path` with names for each point. Might not be needed anymore."""
	points = load_points(path)
	points = try_auto_set_index(points)
	if isinstance(points.index, RangeIndex):
		# Try and get something more descriptive than just the default increasing index
		points.index = Index(
			[
				format_point(geo) if isinstance(geo, shapely.Point) else str(geo)
				for geo in points.geometry
			]
		)
	return points


async def load_point_sets(
	subs_per_user: Mapping[PlayerUsername, 'GeoDataFrame'] | None,
	name: str | None,
	points_path: Path | None,
	threshold: int | None,
	additional_folders: list[Path] | None,
	additional_players_args: list[list[str]] | None,
	*,
	load_per_user: bool,
) -> list[PointSet]:
	if load_per_user and not subs_per_user:
		settings = Settings()
		async with ClientSession() as sesh:
			subs_per_user = await load_or_fetch_per_player_submissions(
				settings.subs_per_player_path, session=sesh
			)
			player_names = await get_player_display_names(sesh)
	else:
		player_names = {}

	gdf_per_user = (
		{
			player_names.get(player_username, player_username): player_pics
			for player_username, player_pics in subs_per_user.items()
			if threshold is None or player_pics.index.size >= threshold
		}
		if subs_per_user
		else {}
	)
	if points_path:
		if not name:
			print(
				'Warning: --points-path does not do anything without --name. You may want to use --add-player if these points are for a different player'
			)
		else:
			# TODO: Probably we want to combine the points rather than replace them (for example, a 5K might be just a submission and not something one keeps track of in the point set)
			gdf_per_user[name] = await asyncio.to_thread(load_with_name, points_path)
	point_sets = [PointSet(gdf, name) for name, gdf in gdf_per_user.items()]

	if additional_folders:
		for folder in additional_folders:
			point_sets += load_point_sets_from_folder(folder)

	if additional_players_args:
		for additional_name, path in additional_players_args:
			points = await asyncio.to_thread(load_with_name, path)
			point_sets.append(PointSet(points, additional_name))

	if not point_sets:
		raise RuntimeError('Nobody is able to be simulated')
	return point_sets


def parse_coords(s: str) -> shapely.Point | None:
	lat_s, lng_s = re.split(r'[,\s/;]\s*', s, maxsplit=1)
	lat = float(lat_s)
	lng = float(lng_s)
	return shapely.Point(lng, lat)


def simulate_single_round(sim: Simulation, output_path: Path | None):
	round_name, target = next(iter(sim.rounds.items()))
	result = sim.simulate_round(round_name, 1, target)
	rows = [sub.model_dump(exclude_none=True) for sub in result.submissions]
	df = DataFrame(rows)
	df.insert(0, 'rank', df.pop('rank'))

	print(format_dataframe(df, distance_cols=('distance')))
	if output_path:
		output_dataframe(df, output_path)


def main() -> None:
	argparser = ArgumentParser(description=__doc__)
	strategy_choices = {s.name.lower(): s for s in SimulatedStrategy}

	target_args = argparser.add_argument_group(
		'Target arguments', 'Which locations will be the targets of each simulated round'
	)
	sim_args = argparser.add_argument_group(
		'Simulation arguments', 'Arguments controlling how the simulation is simulated'
	)
	player_args = argparser.add_argument_group(
		'Player arguments',
		'Arguments to control what simulated players exist and what pics they have',
	)
	output_args = argparser.add_argument_group(
		'Output arguments', 'Arguments specifying what is output and where'
	)

	argparser.add_argument(
		'--name',
		help='Optionally keep track of a particular player (likely use case = yourself) and their results, and optionally to load a replacement for their point set',
	)

	exclusive_target_args = target_args.add_mutually_exclusive_group(required=False)
	exclusive_target_args.add_argument(
		'--rounds-path', '--rounds', type=Path, help='Load rounds to re-simulate as the target'
	)
	# TODO: More fine-grained arguments to load or not load players from --rounds-path if that is provided, and also targets
	exclusive_target_args.add_argument(
		'--targets',
		type=Path,
		help='If this path is specified, load points from this file to be used as each round',
	)
	exclusive_target_args.add_argument(
		'--point',
		'--single',
		type=parse_coords,
		help='If this is specified, display results for a single round (target specified lat/lng decimal degrees, if you use DMS I hate you). Ignores various output options',
	)
	exclusive_target_args.add_argument(
		'--random-rounds', type=int, help='If this is specified, generate N random rounds'
	)
	target_args.add_argument(
		'--region',
		'--random-in-region',
		metavar='path',
		help='With --random-rounds, generate points within a region instead of anywhere in the world',
	)

	sim_args.add_argument(
		'--custom-scoring',
		type=float,
		help="If specified, use a scoring method for regional TPGs with this as the world distance in km. If not specified, use main TPG scoring. This is a bit awkward but I couldn't think of anything better right now whoops",
	)
	sim_args.add_argument(
		'--strategy',
		choices=strategy_choices.keys(),
		help='Strategy of simulated players',
		default='closest',
	)
	sim_args.add_argument(
		'--use-haversine',
		action=BooleanOptionalAction,
		help='Use haversine for distances, defaults to true for consistency with main TPG',
		default=True,
	)

	output_args.add_argument(
		'--output-path', type=Path, help='Output simulated rounds as a TPG data file'
	)
	output_args.add_argument(
		'--player-summary-path',
		'--player-summary-output-path',
		'--scores-output-path',
		type=Path,
		help='Output total scores/etc of simulated players here, or if using --point, the scores of each player for the single round',
	)
	output_args.add_argument(
		'--round-summary-path',
		'--rounds-output-path',
		'--round-summary-output-path',
		type=Path,
		help='Output winners/etc of each round here.',
	)
	output_args.add_argument(
		'--podium-rounds-path',
		type=Path,
		help='With --name, output rounds where that player gets podium here',
	)
	output_args.add_argument(
		'--losing-rounds-path',
		type=Path,
		help='With --name, output rounds where that player loses here',
	)

	player_args.add_argument(
		'--load-per-player-submissions',
		action=BooleanOptionalAction,
		default=True,
		help="Load simulated players from all real TPG players, getting known submissions from main TPG data + Morphior's opponent checker data. Defaults to true",
	)
	player_args.add_argument(
		'--points-path',
		type=Path,
		help="In conjunction with --name, replace your points with those loaded from this file (to use pics that you haven't submitted yet)",
	)
	player_args.add_argument(
		'--threshold',
		type=int,
		help='Only simulate players who have submitted at least this amount of pics. This can help speed up the simulation',
	)
	player_args.add_argument(
		'--add-from-folder',
		'--add-from-directory',
		action='append',
		type=Path,
		help='Add new players from every supported file found in a folder, using the filenames as the player name (does not recurse into subdirectories, because that would be getting a bit too wild)',
	)
	player_args.add_argument(
		'--add-player',
		'--additional-player',
		dest='add_player',
		action='append',
		nargs=2,
		metavar=('name', 'point_set_path'),
		help='Add a new player with a name and points from a file (pair of arguments in that order, can be specified multiple times)',
	)
	# Everything should be optional here, and subs_per_user and targets should be completely separate, but by default load the former as usual and use main TPG data for the latter but have options to not do that, just throw an error if we end up with no players or no points
	# TODO: Grid of points for target
	# TODO: Option to also try with a new_points point set, and see how it compares, and what pics would improve your ranking etc
	args = argparser.parse_args()

	rounds_path: Path | None = args.rounds_path
	# Nitpick: Pretty sure how ArgumentParser works is that if --rounds-path is not specified, it is Path('') and not None, so the type hint is technically inaccurate but pretending that it can be None ensures the type checker catches errors in handling it not being set, so it's probably better that way
	name: str | None = args.name
	points_path: Path | None = args.points_path
	targets_path: Path | None = args.targets
	num_random_points: int | None = args.random_rounds
	region_path: Path | None = args.region

	strategy = strategy_choices[args.strategy]
	if args.custom_scoring:
		# TODO: Probably want to have some ability to load options from a file or something
		scoring = ScoringOptions(
			fivek_flat_score=7500,
			fivek_bonus=None,
			rank_bonuses=None,
			antipode_5k_flat_score=None,
			world_distance_km=args.custom_scoring,
		)
	else:
		scoring = main_tpg_scoring

	# TODO: Use main TPG data for existing_rounds by default
	existing_rounds = load_rounds(rounds_path) if rounds_path else None

	# TODO: (Optionally) get players from existing_rounds
	point_set = asyncio.run(
		load_point_sets(
			None,
			name,
			points_path,
			args.threshold,
			args.add_from_folder,
			args.add_player,
			load_per_user=args.load_per_player_submissions,
		)
	)
	simulation = get_simulation(
		existing_rounds,
		point_set,
		scoring,
		strategy,
		targets_path,
		num_random_points,
		region_path,
		args.point,
		use_haversine=args.use_haversine,
	)

	player_names = {point_set.name for point_set in simulation.point_sets}
	if name and name not in player_names:
		print(
			f'Warning: {name} was not found in TPG data or otherwise did not have any pics, so does not exist in this context. Setting to None'
		)
		name = None

	if len(simulation.rounds) == 1:
		simulate_single_round(simulation, args.player_summary_path)
		return

	new_rounds = simulation.simulate_rounds()
	output_results(
		new_rounds,
		existing_rounds,
		name,
		args.round_summary_path,
		args.player_summary_path,
		args.podium_rounds_path,
		args.losing_rounds_path,
	)
	output_path: Path | None = args.output_path
	if output_path:
		# TODO: How do we stop this being _too_ large? Should we just compress it?
		output_path.write_text(rounds_to_json(new_rounds), 'utf-8')


if __name__ == '__main__':
	logging.basicConfig(level=logging.INFO)
	with logging_redirect_tqdm():
		main()

from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings, extra='allow'):
	model_config = {'cli_parse_args': False, 'env_file': '.env'}

	gadm_0_path: Path | None = None
	"""Path to ADM_0.gpkg from GADM, for anything that might use country borders etc"""
	gadm_1_path: Path | None = None
	"""Path to ADM_1.gpkg from GADM, for anything that might use subdivision borders etc"""
	gadm_2_path: Path | None = None
	"""Path to ADM_2.gpkg from GADM, for anything that might use subdivision borders etc"""
	gadm_3_path: Path | None = None
	"""Path to ADM_3.gpkg from GADM, for anything that might use subdivision borders etc"""
	main_tpg_data_path: Path | None = None
	"""Path to save data from main TPG as JSON"""
	subs_per_player_path: Path | None = None
	"""Path to save submissions per player as GeoJSON/etc"""
	tpg_wrapped_output_path: Path | None = None
	"""Folder to save TPG wrapped to"""
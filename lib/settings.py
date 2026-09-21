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
	submission_summary_path: Path | None = None
	"""Path to save summary of all known submissions by all players"""
	tpg_export_path: Path | None = None
	"""Path (or path pattern with {} as a wildcard, e.g. tpg-tracker-{}.geojson) with exported TPG tracker data from Cellery's site, as GeoJSON"""
	tpg_wrapped_output_path: Path | None = None
	"""Folder to save all TPG wrapped output"""

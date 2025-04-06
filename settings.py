from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
	model_config = {'cli_parse_args': True, 'env_file': '.env'}

	gadm_path: Path | None = None
	"""Path to ADM_0.gpkg from GADM, for anything that might use country borders etc"""
	rounds_path: Path | None = None
	"""Path to save all rounds as GeoJSON, with the number of the most recent round replacing {0}"""
	submissions_path: Path | None = None
	"""Path to save all submissions as JSON, with the number of the most recent round replacing {0}"""
	submissions_with_scores_path: Path | None = None
	"""Path to save all submissions with their scores as pickle, with the number of the most recent round replacing {0}"""
	average_per_user_path: Path | None = None
	"""Path to save average coordinate for each user as GeoJSON, with the number of the most recent round replacing {0}"""
	theoretical_best_path: Path | None = None
	"""Path to save a pickled DataFrame containing all rounds if every user had made a submission with their best pic (out of who has submitted so far and what)"""

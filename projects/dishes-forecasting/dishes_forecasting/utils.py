import logging
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)


def read_yaml(filename: str, directory: str) -> dict:
    yaml_path_str = f"{directory}/{filename}"
    yaml_path = Path(yaml_path_str)
    try:
        with Path.open(yaml_path) as f:
            yaml_dict = yaml.safe_load(f)
        return yaml_dict
    except ValueError:
        logging.error(f"{yaml_path} is not a valid extension.")

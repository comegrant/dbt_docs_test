import logging
from typing import Dict, Optional

import yaml

from cheffelo_personalization.basket_preselector.utils.constants import ENVIRONMENT
from cheffelo_personalization.basket_preselector.utils.paths import CONFIG_DIR

logger = logging.getLogger(__name__)


def get_run_configs(
    config_version: Optional[str] = "0.3", config_name: Optional[str] = "pipeline"
) -> Dict:
    config_path = CONFIG_DIR / f"{config_version}" / f"{config_name}.yml"
    logger.info(f"Logging config from {config_path}")
    with open(config_path) as f:
        config = yaml.safe_load(f)

    logging.info("Success: pipeline configs loaded.")
    return config


def get_company_configs(company_code: str, config_name: str) -> dict:
    """Get the configuration based on company

    Returns:
        dict: configurations
    """
    if ENVIRONMENT == "dev":
        with open(CONFIG_DIR / "defaults.yml") as file:
            default_values = yaml.safe_load(file)
        return default_values[company_code]
    return {}

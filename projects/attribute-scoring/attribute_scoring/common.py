import logging
from typing import Literal, Optional

from databricks.connect import DatabricksSession
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class Args(BaseModel):
    company: Literal["LMK", "AMK", "GL", "RT"]
    env: Literal["dev", "prod"]
    target: Optional[Literal["has_chefs_favorite_taxonomy", "has_family_friendly_taxonomy"]] = None


def get_spark_session() -> DatabricksSession:
    try:
        spark = DatabricksSession.builder.getOrCreate()
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        raise

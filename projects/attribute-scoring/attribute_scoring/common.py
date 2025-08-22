import logging
from typing import Literal

from pydantic import BaseModel, Field, SecretStr
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)


class Args(BaseModel):
    company: Literal["LMK", "AMK", "GL", "RT"] = Field("AMK")
    env: Literal["dev", "test", "prod"] = Field("dev")
    alias: str = Field("challenger")


class ArgsTrain(Args):
    target: Literal["has_chefs_favorite_taxonomy", "has_family_friendly_taxonomy"] = (
        Field("has_chefs_favorite_taxonomy")
    )


class ArgsPredict(Args):
    startyyyyww: str = Field("")
    endyyyyww: str = Field("")


class DatabricksSecrets(BaseSettings):
    databricks_token: SecretStr
    databricks_host: str

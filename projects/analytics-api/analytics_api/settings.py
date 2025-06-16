from typing import Literal

from pydantic_settings import BaseSettings


class EnvSettings(BaseSettings):
    """
    Settings for the application environment.

    Attributes:
        environment (Literal["dev", "test", "prod"]): The environment the application is running in.
            Defaults to "dev".
    """

    environment: Literal["dev", "test", "prod"] = "dev"


def load_settings() -> EnvSettings:
    return EnvSettings()

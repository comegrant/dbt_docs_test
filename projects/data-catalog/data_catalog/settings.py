from pydantic import Field
from pydantic_core import MultiHostUrl
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    some_db_url: MultiHostUrl = Field(
        description="This is a database url",
    )
    environment: str = Field(default="development")

    class Config:
        env_file = ".env"

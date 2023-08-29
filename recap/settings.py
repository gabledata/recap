from pydantic import AnyUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class RecapSettings(BaseSettings):
    systems: dict[str, AnyUrl] = Field(default_factory=dict)
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="recap_",
        env_nested_delimiter="__",
    )

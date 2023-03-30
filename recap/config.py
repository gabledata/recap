import os
from pathlib import Path
from typing import Any

from pydantic import BaseModel, BaseSettings

# Set up RECAP_HOME
DEFAULT_RECAP_HOME = os.path.join(Path.home(), ".recap")
RECAP_HOME = os.environ.get("RECAP_HOME", DEFAULT_RECAP_HOME)
Path(RECAP_HOME).mkdir(parents=True, exist_ok=True)

# Set up RECAP_SECRETS_HOME
DEFAULT_SECRET_HOME = f"{RECAP_HOME}/.secrets"
SECRET_HOME = os.environ.get("RECAP_SECRETS_HOME", DEFAULT_SECRET_HOME)
Path(SECRET_HOME).mkdir(parents=True, exist_ok=True)


class StorageSettings(BaseModel):
    url: str = f"sqlite:///{RECAP_HOME}/recap.db"
    opts: dict[str, str] = {}


class ClientSettings(BaseModel):
    url: str = "http://127.0.0.1:8000"
    opts: dict[str, str] = {}


class Settings(BaseSettings):
    client_settings: ClientSettings = ClientSettings()
    storage_settings: StorageSettings = StorageSettings()
    logging_config_file: str | None = None
    uvicorn_settings: dict[str, Any] = {}

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        env_nested_delimiter = "__"
        env_prefix = "recap_"


settings = Settings(_secrets_dir=SECRET_HOME)  # type: ignore

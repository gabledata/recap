import os
from pathlib import Path

from dotenv import load_dotenv, set_key, unset_key
from pydantic import AnyUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

load_dotenv()

CONFIG_FILE = os.environ.get("RECAP_CONFIG") or os.path.expanduser("~/.recap/config")
SECRETS_DIR = os.environ.get("RECAP_SECRETS")


def touch_config():
    config_path = Path(CONFIG_FILE)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.touch(mode=0o600, exist_ok=True)
    if SECRETS_DIR:
        secrets_path = Path(SECRETS_DIR)
        secrets_path.mkdir(mode=0o700, parents=True, exist_ok=True)


touch_config()


class RecapSettings(BaseSettings):
    systems: dict[str, AnyUrl] = Field(default_factory=dict)
    model_config = SettingsConfigDict(
        # .env takes priority over CONFIG_FILE
        env_file=[CONFIG_FILE, ".env"],
        env_file_encoding="utf-8",
        env_prefix="recap_",
        env_nested_delimiter="__",
        secrets_dir=SECRETS_DIR,
    )


def set_config(key: str, val: str):
    set_key(CONFIG_FILE, key.upper(), val)


def unset_config(key: str):
    unset_key(CONFIG_FILE, key.upper())

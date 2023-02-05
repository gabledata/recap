import os
from pathlib import Path

from dynaconf import Dynaconf

RECAP_HOME = os.path.join(Path.home(), ".recap")
Path(RECAP_HOME).mkdir(parents=True, exist_ok=True)


settings = Dynaconf(
    envvar_prefix="RECAP",
    load_dotenv=True,
    root_path=RECAP_HOME,
    settings_files=["settings.toml", ".secrets.toml"],
)

import os
from dynaconf import Dynaconf
from pathlib import Path


root_path = os.path.join(Path.home(), '.recap')
Path(root_path).mkdir(parents=True, exist_ok=True)


settings = Dynaconf(
    envvar_prefix="RECAP",
    load_dotenv=True,
    root_path=root_path,
    settings_files=['settings.toml', '.secrets.toml'],
)

import logging.config
import tomli
from .config import settings
from pathlib import Path
from typing import Any


# Config key to define a custom TOML dictConfig location.
LOGGING_CONFIG_PATH = 'logging.config.path'

# Default logging config if no logging config path is set.
DEFAULT_LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
    },
    'handlers': {
        'default': {
            'formatter': 'standard',
            'class': 'rich.logging.RichHandler',
            'show_time': False,
            'show_level': False,
            'show_path': False,
        },
    },
    'loggers': {
        '': {
            'handlers': ['default'],
            'level': 'WARNING',
            'propagate': False
        },
        'recap': {
            'handlers': ['default'],
            'level': 'INFO',
            'propagate': False
        },
        'uvicorn': {
            'handlers': ['default'],
            'level': 'INFO',
            'propagate': False
        },
    }
}

def setup_logging() -> dict[str, Any]:
    """
    Configures logging. If a user defines a TOML file that conforms to Python's
    dictConfig schema:

    https://docs.python.org/3/library/logging.config.html#logging-config-dictschema

    Then the custom logging settings will be used. If no TOML file is
    configured, DEFAULT_LOGGING_CONFIG is used.
    """

    logging_config = DEFAULT_LOGGING_CONFIG
    logging_config_file_loc = settings(LOGGING_CONFIG_PATH)
    if logging_config_file_loc:
        logging_config_file_path = Path(logging_config_file_loc)
        logging_config_string = logging_config_file_path.read_text()
        logging_config = tomli.loads(logging_config_string)
    logging.config.dictConfig(logging_config)
    return logging_config

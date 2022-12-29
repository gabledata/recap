import typer
from .logging import setup_logging
from .plugins import load_command_plugins


LOGGING_CONFIG = setup_logging()
app = load_command_plugins(typer.Typer())


if __name__ == "__main__":
    app()

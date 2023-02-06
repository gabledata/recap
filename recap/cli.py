import typer

from .logging import setup_logging
from .plugins import init_command_plugins

LOGGING_CONFIG = setup_logging()
app = init_command_plugins(
    typer.Typer(
        help="""
    Recap's command line interface.

    \b
    Recap's CLI is completely pluggable. See the commands below for more
    information on individual command plugins.
"""
    )
)


if __name__ == "__main__":
    app()

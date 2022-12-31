import typer
from itertools import chain
from recap import plugins
from recap.config import settings
from rich import print_json
from pathlib import PurePosixPath


app = typer.Typer()


@app.command()
def analyzers():
    print_json(data=list(plugins.load_analyzer_plugins().keys()))


@app.command()
def browsers():
    print_json(data=list(plugins.load_browser_plugins().keys()))


@app.command()
def catalogs():
    print_json(data=list(plugins.load_catalog_plugins().keys()))


@app.command()
def commands():
    print_json(data=list(plugins.load_command_plugins().keys()))

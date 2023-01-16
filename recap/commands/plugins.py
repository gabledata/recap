import typer
from recap import plugins
from rich import print_json


app = typer.Typer(help="Lists available plugins.")


@app.command()
def analyzers():
    """
    Lists all analyzer plugins.
    """

    print_json(data=list(plugins.load_analyzer_plugins().keys()))


@app.command()
def browsers():
    """
    Lists all browser plugins.
    """

    print_json(data=list(plugins.load_browser_plugins().keys()))


@app.command()
def catalogs():
    """
    Lists all catalog plugins.
    """

    print_json(data=list(plugins.load_catalog_plugins().keys()))


@app.command()
def commands():
    """
    Lists all command plugins.
    """

    print_json(data=list(plugins.load_command_plugins().keys()))


@app.command()
def routers():
    """
    Lists all router plugins.
    """

    print_json(data=list(plugins.load_router_plugins().keys()))

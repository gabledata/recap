import typer
from recap.plugins import catalogs
from recap.config import settings
from rich import print_json
from pathlib import PurePosixPath


app = typer.Typer(help="Read and search the data catalog.")


@app.command()
def search(query: str):
    """
    Searches the data catalog. The query string syntax is dependent on the data
    catalog.
    """

    with catalogs.open(**settings('catalog', {})) as c:
        results = c.search(query)
        print_json(data=results, sort_keys=True)


@app.command("list")
def list_(
    path: str = typer.Argument('/')
):
    """
    Lists a data catalog directory's children.
    """

    with catalogs.open(**settings('catalog', {})) as c:
        results = sorted(c.ls(PurePosixPath(path)) or [])
        print_json(data=results)


@app.command()
def read(
    path: str
):
    """
    Prints metadata from a path in the data catalog.
    """

    with catalogs.open(**settings('catalog', {})) as c:
        results = c.read(PurePosixPath(path)) or []
        print_json(data=results, sort_keys=True)

import typer
from recap.plugins import catalogs
from recap.config import settings
from rich import print_json
from pathlib import PurePosixPath


app = typer.Typer()


@app.command()
def search(query: str):
    with catalogs.open(**settings('catalog', {})) as c:
        results = c.search(query)
        print_json(data=results, sort_keys=True)


@app.command("list")
def list_(
    path: str = typer.Argument('/')
):
    with catalogs.open(**settings('catalog', {})) as c:
        results = sorted(c.ls(PurePosixPath(path)) or [])
        print_json(data=results)


@app.command()
def read(
    path: str
):
    with catalogs.open(**settings('catalog', {})) as c:
        results = c.read(PurePosixPath(path))
        print_json(data=results, sort_keys=True)

import typer
from datetime import datetime
from recap import catalogs
from recap.config import settings
from rich import print_json


app = typer.Typer(help="""
    Read and search the data catalog.

    Recap's `recap catalog` command reads metadata Recap's data catalog. List
    the catalog's directory structure with `recap list`, read metadata from a
    directory with `recap read`, and search with `recap search`.
""")


@app.command()
def search(
    query: str,
    time: datetime = typer.Option(
        None, '--time', '-t',
        help=\
            "View metadata as of a point in time.",
    ),
):
    """
    Searches the data catalog.

    \b
    Recap's search syntax depends on the catalog plugin that's used. Recap
    stores its metadata in SQLite by default. You can use SQLite's json_extract
    syntax to search the catalog:

    \b
        recap catalog search "json_extract(metadata, '$.\"sqlalchemy.columns\".some_col') IS NOT NULL"

    \b
    The database file defaults to `~/.recap/catalog/recap.db`, if you wish to
    open a SQLite client directly.
    """

    with catalogs.create_catalog(**settings('catalog', {})) as c:
        results = c.search(query, time)
        print_json(data=results, sort_keys=True)


@app.command("list")
def list_(
    path: str = typer.Argument('/'),
    time: datetime = typer.Option(
        None, '--time', '-t',
        help=\
            "List directory children for a path.",
    ),
):
    """
    Lists a data catalog directory's children.
    """

    with catalogs.create_catalog(**settings('catalog', {})) as c:
        results = sorted(c.ls(path, time) or [])
        print_json(data=results)


@app.command()
def read(
    path: str,
    time: datetime = typer.Option(
        None, '--time', '-t',
        help=\
            "View metadata as of a point in time.",
    ),
):
    """
    Prints metadata from a path in the data catalog.
    """

    with catalogs.create_catalog(**settings('catalog', {})) as c:
        results = c.read(path, time) or []
        print_json(data=results, sort_keys=True)

from datetime import datetime

import typer
from rich import print_json

from recap import catalogs
from recap.config import settings
from recap.metadata import Metadata

# TODO This is silly. These should be init'd in recap.__init__.
from recap.schemas.schema import Schema

app = typer.Typer(
    help="""
    Read and search the data catalog.

    Recap's `recap catalog` command reads metadata Recap's data catalog. List
    the catalog's directory structure with `recap list`, read metadata from a
    directory with `recap read`, and search with `recap search`.
"""
)


def type_from_string(type: str) -> type[Metadata] | None:
    for cls in Metadata.__subclasses__():
        if cls.key() == type:
            return cls


@app.command()
def search(
    type_str: str,
    query: str,
    time: datetime = typer.Option(
        None,
        "--time",
        "-t",
        help="View metadata as of a point in time.",
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

    with catalogs.create_catalog(**settings("catalog", {})) as c:
        if type := type_from_string(type_str):
            results = c.search(query, type, time) or []
            print_json(
                data=[result.to_dict() for result in results],
            )


@app.command()
def children(
    url: str = typer.Argument("/"),
    time: datetime = typer.Option(
        None,
        "--time",
        "-t",
        help="List directory children for a path.",
    ),
):
    """
    Lists a data catalog directory's children.
    """

    with catalogs.create_catalog(**settings("catalog", {})) as c:
        results = sorted(c.children(url, time) or [])
        print_json(data=results)


@app.command()
def read(
    type_str: str,
    url: str,
    time: datetime = typer.Option(
        None,
        "--time",
        "-t",
        help="View metadata as of a point in time.",
    ),
):
    """
    Prints metadata from a path in the data catalog.
    """

    with catalogs.create_catalog(**settings("catalog", {})) as c:
        if type := type_from_string(type_str):
            if metadata := c.read(url, type, time=time):
                print_json(data=metadata.to_dict())

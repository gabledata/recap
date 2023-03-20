from __future__ import annotations

from datetime import datetime

import typer
from rich import print_json

from recap import repl
from recap.logging import setup_logging
from recap.schema import models, types

app = typer.Typer(
    help="""
        Recap's command line interface.
    """
)


@app.command()
def crawl(
    url: str,
    args: list[str] = typer.Option(
        [],
        "--arg",
        help="Arbitrary options (`--arg foo=bar`) passed to the crawler.",
    ),
):
    """
    Recursively crawl a URL and its children, storing metadata and
    relationships in Recap's data catalog.
    """

    repl.crawl(url, **args_to_dict(args))


@app.command()
def ls(
    url: str,
    args: list[str] = typer.Option(
        [],
        "--arg",
        help="Arbitrary options (`--arg foo=bar`) passed to the catalog.",
    ),
    time: datetime = typer.Option(
        None,
        help="Time travel to see what a URL's children used to be.",
    ),
    refresh: bool = typer.Option(
        False,
        help="Skip Recap's catalog and read the latest data directly from the URL.",
    ),
):
    """
    List a URL's child URLs.
    """

    print_json(data=repl.ls(url, time, refresh, **args_to_dict(args)))


@app.command()
def readers(
    url: str,
    args: list[str] = typer.Option(
        [],
        "--arg",
        help="Arbitrary options (`--arg foo=bar`) passed to the catalog.",
    ),
    time: datetime = typer.Option(
        None,
        help="Time travel to see what a URL's readers used to be.",
    ),
    refresh: bool = typer.Option(
        False,
        help="Skip Recap's catalog and read the latest data directly from the URL.",
    ),
):
    """
    See what reads from a URL.
    """

    print_json(data=repl.readers(url, time, refresh, **args_to_dict(args)))


@app.command()
def reads(
    url: str,
    args: list[str] = typer.Option(
        [],
        "--arg",
        help="Arbitrary options (`--arg foo=bar`) passed to the catalog.",
    ),
    time: datetime = typer.Option(
        None,
        help="Time travel to see what a URL used to read.",
    ),
    refresh: bool = typer.Option(
        False,
        help="Skip Recap's catalog and read the latest data directly from the URL.",
    ),
):
    """
    See what a URL reads. URLs must be accounts or jobs.
    """

    print_json(data=repl.reads(url, time, refresh, **args_to_dict(args)))


@app.command()
def schema(
    url: str,
    args: list[str] = typer.Option(
        [],
        "--arg",
        help="Arbitrary options (`--arg foo=bar`) passed to the catalog.",
    ),
    time: datetime = typer.Option(
        None,
        help="Time travel to see what a URL's schema used to be.",
    ),
    refresh: bool = typer.Option(
        False,
        help="Skip Recap's catalog and read the latest data directly from the URL.",
    ),
):
    """
    Get a Recap schema for a URL.
    """

    if schema := repl.schema(url, time, refresh, **args_to_dict(args)):
        print_json(data=types.Parser().to_obj(schema))


@app.command()
def search(
    metadata_type: str,
    query: str,
    time: datetime = typer.Option(
        None,
        help="Time travel to see what a URL's metadata used to be.",
    ),
):
    if metadata_type == "schema":
        print_json(data=[s.dict() for s in repl.search(query, models.Type, time)])


@app.command()
def writers(
    url: str,
    args: list[str] = typer.Option(
        [],
        "--arg",
        help="Arbitrary options (`--arg foo=bar`) passed to the catalog.",
    ),
    time: datetime = typer.Option(
        None,
        help="Time travel to see what a URL's writers used to be.",
    ),
    refresh: bool = typer.Option(
        False,
        help="Skip Recap's catalog and read the latest data directly from the URL.",
    ),
):
    """
    See what writes to a URL.
    """

    print_json(data=repl.writers(url, time, refresh, **args_to_dict(args)))


@app.command()
def writes(
    url: str,
    args: list[str] = typer.Option(
        [],
        "--arg",
        help="Arbitrary options (`--arg foo=bar`) passed to the catalog.",
    ),
    time: datetime = typer.Option(
        None,
        help="Time travel to see where a URL's used to write.",
    ),
    refresh: bool = typer.Option(
        False,
        help="Skip Recap's catalog and read the latest data directly from the URL.",
    ),
):
    """
    See what a URL writes. URLs must be accounts or jobs.
    """

    print_json(data=repl.writes(url, time, refresh, **args_to_dict(args)))


@app.command()
def serve():
    """
    Starts Recap's HTTP/JSON API server.
    """

    import uvicorn

    from recap.config import settings
    from recap.server import fastapp

    uvicorn.run(
        fastapp,
        log_config=setup_logging(),
        **settings.uvicorn_settings,
    )


def args_to_dict(args: list[str]) -> dict[str, str]:
    return dict([tuple(arg.split("=", 1)) for arg in args])


if __name__ == "__main__":
    setup_logging()
    app()

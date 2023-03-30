from __future__ import annotations

import typer

from recap.logging import setup_logging

setup_logging()

app = typer.Typer(
    help="""
        Recap's command line interface.
    """
)


@app.command()
def crawl(
    url: str,
):
    """
    Recursively crawl a URL and its children, storing metadata and
    relationships in Recap's data catalog.
    """

    from recap.catalog.crawler import create_crawler

    create_crawler().crawl(url)


@app.command()
def serve():
    """
    Starts Recap's HTTP/JSON API server.
    """

    import uvicorn

    from recap.catalog.server import fastapp
    from recap.config import settings

    uvicorn.run(
        fastapp,
        log_config=setup_logging(),
        **settings.uvicorn_settings,
    )


if __name__ == "__main__":
    app()

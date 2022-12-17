import typer
from . import catalog
from .config import settings
from rich import print_json


app = typer.Typer()


@app.command()
def api():
    import uvicorn

    uvicorn.run(
        "recap.api:app",
        **settings('api', {}),
    )


# TODO convert api and crawler to one `server` command that runs async.
@app.command()
def crawler():
    from . import crawlers

    with catalog.open(**settings('catalog', {})) as ca:
        for crawler_config in settings('crawlers', {}):
            with crawlers.open(ca, **crawler_config) as cr:
                cr.crawl()

@app.command()
def search(query: str):
    with catalog.open(**settings('catalog', {})) as c:
        results = c.search(query)
        print_json(data=results)


@app.command()
def list(
    path: str = typer.Argument('/')
):
    from pathlib import PurePosixPath

    with catalog.open(**settings('catalog', {})) as c:
        results = c.ls(PurePosixPath(path)) or []
        print_json(data=results)


@app.command()
def read(
    path: str
):
    from pathlib import PurePosixPath

    with catalog.open(**settings('catalog', {})) as c:
        results = c.read(PurePosixPath(path))
        print_json(data=results)


if __name__ == "__main__":
    app()

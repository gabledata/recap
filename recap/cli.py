import typer
from . import storage, search as search_module
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
    from . import crawlers, search
    from .storage.notifier import StorageNotifier

    with storage.open(**settings('storage', {})) as s:
        with search.open(**settings('search', {})) as i:
            for crawler_config in settings('crawlers', {}):
                wrapped_storage = StorageNotifier(s, i)
                with crawlers.open(wrapped_storage, **crawler_config) as cr:
                    cr.crawl()

@app.command()
def search(query: str):
    with search_module.open(**settings('search', {})) as s:
        results = s.search(query)
        print_json(data=results)


@app.command()
def list(
    path: str = typer.Argument('/')
):
    from pathlib import PurePosixPath

    with storage.open(**settings('storage', {})) as s:
        results = s.ls(PurePosixPath(path))
        print_json(data=results)


@app.command()
def read(
    path: str
):
    from pathlib import PurePosixPath

    with storage.open(**settings('storage', {})) as s:
        results = s.read(PurePosixPath(path))
        print_json(data=results)


if __name__ == "__main__":
    app()

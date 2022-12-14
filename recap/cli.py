import typer
from . import storage
from .config import settings
from rich import print, print_json


app = typer.Typer()


@app.command()
def api():
    import uvicorn

    uvicorn.run(
        "recap.api:app",
        # TODO should this all just be passed in via **settings['api']?
        host=settings('api.host', '0.0.0.0'),
        port=settings('api.port', 8000, cast=int),
    )


@app.command()
def crawler():
    from recap import crawlers

    with storage.open(**settings['storage']) as s:
        for infra, instance_dict in settings['crawlers'].items():
            for instance, instance_config in instance_dict.items():
                with crawlers.open(infra, instance, s, **instance_config) as c:
                    # TODO Make async or threaded so we can start more than one
                    c.crawl()


@app.command()
def search(query: str):
    from recap.search import JqSearch

    with storage.open(**settings['storage']) as s:
        search = JqSearch(s)
        results = search.search(query)
        print_json(data=results)


if __name__ == "__main__":
    app()

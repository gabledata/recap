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


@app.command()
def list(
    infra: str | None = None,
    instance: str | None = None,
    schema: str | None = None,
    table: str | None = None,
    view: str | None = None,
):
    with storage.open(**settings['storage']) as s:
        children = []
        if not instance:
            children = s.list_infra()
        elif not instance:
            children = s.list_instance(infra)
        elif not schema:
            children = s.list_schemas(infra, instance)
        elif table:
            children = s.list_metadata(
                infra,
                instance,
                schema,
                table=table,
            )
        elif view:
            children = s.list_metadata(
                infra,
                instance,
                schema,
                view=view,
            )
        else:
            children = {
                'tables': s.list_tables(infra, instance, schema),
                'views': s.list_views(infra, instance, schema),
            }
        print_json(data=children)


@app.command()
def read(
    infra: str,
    instance: str,
    type: str,
    schema: str | None = None,
    table: str | None = None,
    view: str | None = None,
):
    with storage.open(**settings['storage']) as s:
        metadata = s.get_metadata(
            infra,
            instance,
            type,
            schema,
            table,
            view,
        )
        print_json(data=metadata)


if __name__ == "__main__":
    app()

import typer
from . import catalog
from .config import settings
from rich import print_json
from typing import Optional


app = typer.Typer()


@app.command()
def api():
    import uvicorn
    from .api import app

    uvicorn.run(
        app,
        **settings('api', {}),
    )


@app.command()
def refresh(url: Optional[str] = typer.Argument(None)):
    from . import crawlers

    # Make sure URL is included in crawlers if it's passed in. This is needed
    # because Recap works with URLs even if no config was set for it in
    # settings.toml. It is smart enough to figure out the default module to
    # use.
    crawler_config_list = settings('crawlers', [])
    crawler_urls = set(map(lambda c: c.get('url'), crawler_config_list))
    if url and url not in crawler_urls:
        crawler_config_list.append({'url': url})

    with catalog.open(**settings('catalog', {})) as ca:
        for crawler_config in crawler_config_list:
            if not url or url == crawler_config.get('url'):
                with crawlers.open(ca, **crawler_config) as cr:
                    cr.crawl()


@app.command()
def search(query: str):
    with catalog.open(**settings('catalog', {})) as c:
        results = c.search(query)
        print_json(data=results, sort_keys=True)


@app.command()
def list(
    path: str = typer.Argument('/')
):
    from pathlib import PurePosixPath

    with catalog.open(**settings('catalog', {})) as c:
        results = sorted(c.ls(PurePosixPath(path)) or [])
        print_json(data=results)


@app.command()
def read(
    path: str
):
    from pathlib import PurePosixPath

    with catalog.open(**settings('catalog', {})) as c:
        results = c.read(PurePosixPath(path))
        print_json(data=results, sort_keys=True)


if __name__ == "__main__":
    app()

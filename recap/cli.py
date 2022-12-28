import typer
from . import catalog
from .config import settings
from .crawlers.db.analyzers import DEFAULT_ANALYZERS
from .logging import setup_logging
from .plugins import load_cli_plugins
from rich import print_json
from rich.progress import Progress, SpinnerColumn, TextColumn
from typing import List, Optional


LOGGING_CONFIG = setup_logging()
app = load_cli_plugins(typer.Typer())


@app.command()
def api():
    import uvicorn
    from .api import app

    uvicorn.run(
        app,
        log_config=LOGGING_CONFIG,
        **settings('api', {}),
    )


@app.command()
def refresh(
    url: Optional[str] = typer.Argument(None, help="URL to refresh."),
    filter: Optional[List[str]] = typer.Option(
        ['*'],
        help=\
            "Filter to crawl only certain '<schema>.<name>'. "
            "Format is Unix shell-style wildcards."
    ),
    analyzer: Optional[List[str]] = typer.Option(
        DEFAULT_ANALYZERS,
        help=\
            "Analyzer to use when crawling. "
            "Format is 'some.module.Class'."
    ),
):
    from . import crawlers

    # Make sure URL is included in crawlers if it's passed in. This is needed
    # because Recap works with URLs even if no config was set for it in
    # settings.toml. It is smart enough to figure out the default module to
    # use.
    crawler_config_list = settings('crawlers', [])
    crawler_urls = set(map(lambda c: c.get('url'), crawler_config_list))
    if url and url not in crawler_urls:
        crawler_config_list.append({'url': url})
    if filter:
        for crawler_config in crawler_config_list:
            if not url or url == crawler_config['url']:
                crawler_config['filters'] = filter
    if analyzer:
        for crawler_config in crawler_config_list:
            if not url or url == crawler_config['url']:
                crawler_config['analyzers'] = analyzer

    with catalog.open(**settings('catalog', {})) as ca:
        for crawler_config in crawler_config_list:
            if not url or url == crawler_config.get('url'):
                progress_spinner = SpinnerColumn(finished_text='[green]✓')
                progress_text = TextColumn("[progress.description]{task.description}")
                with (
                    crawlers.open(ca, **crawler_config) as cr,
                    Progress(progress_spinner, progress_text) as progress
                ):
                    # Set up the spinner description.
                    crawler_url = crawler_config.get('url')
                    crawler_infra = crawlers.guess_infra(crawler_url)
                    crawler_instance = crawlers.guess_instance(crawler_url)
                    task_id = progress.add_task(
                        description=\
                            f"Crawling {crawler_infra}://{crawler_instance} ...",
                        total=1
                    )

                    cr.crawl()

                    # Mark done, so we get a little green checkmark.
                    progress.update(task_id, completed=1)


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

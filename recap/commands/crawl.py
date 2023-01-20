import typer
from recap.config import settings
from recap.catalogs import create_catalog
from recap.crawler import create_crawler
from rich.progress import Progress, SpinnerColumn, TextColumn
from typing import Optional


app = typer.Typer()


@app.command()
def crawl(
    url: Optional[str] = typer.Argument(
        None,
        help=\
            "URL to crawl. If unset, all URLs in settings.toml are used.",
    ),
    analyzer_excludes: list[str] = typer.Option(
        [], '--exclude', '-e',
        help=\
            "Skip the specified analyzer when crawling.",
    ),
    path_filters: list[str] = typer.Option(
        [], '--filter', '-f',
        help=\
            "Crawl only certain paths. Format is Unix shell-style wildcards.",
    ),
):
    """
    Crawls infrastructure and writes metadata to the data catalog.
    """

    # Make sure URL is included in crawlers if it's passed in. This is needed
    # because Recap works with URLs even if no config was set for it in
    # settings.toml. It is smart enough to figure out the default module to
    # use.
    crawlers_configs = settings('crawlers', [])
    crawler_urls = set(map(
        lambda c: c.get('url'),
        crawlers_configs,
    ))

    if url and url not in crawler_urls:
        crawlers_configs.append({'url': url})

    if analyzer_excludes:
        for crawler_config in crawlers_configs:
            if not url or url == crawler_config['url']:
                crawler_config['excludes'] = analyzer_excludes

    if path_filters:
        for crawler_config in crawlers_configs:
            if not url or url == crawler_config['url']:
                crawler_config['filters'] = path_filters

    with create_catalog(**settings('catalog', {})) as catalog:
        for crawler_config in crawlers_configs:
            if not url or url == crawler_config['url']:
                with create_crawler(catalog, **crawler_config) as crawler:
                    spinner = SpinnerColumn(finished_text='[green]✓')
                    text = TextColumn("[progress.description]{task.description}")

                    with (Progress(spinner, text) as progress):
                        # Set up the spinner description.
                        task_id = progress.add_task(
                            description=f"Crawling {url} ...",
                            total=1,
                        )

                        crawler.crawl()

                        # Mark done, so we get a little green checkmark.
                        progress.update(task_id, completed=1)

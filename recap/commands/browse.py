import logging
import typer
from pathlib import PurePosixPath
from recap.browsers import create_browser
from recap.config import settings
from recap.plugins import load_browser_plugins
from rich import print_json


app = typer.Typer()
log = logging.getLogger(__name__)


@app.command()
def browse(
    url: str = typer.Argument(...,
        help="URL to browse. URL configs in settings.toml will be used, if availabe.",
    ),
    path: str = typer.Argument(
        '/',
        help="Path to list children for.",
    ),
):
    """
    Returns children for a URL and path.
    """

    clean_path = str(PurePosixPath(path))
    browser_plugins = load_browser_plugins()
    crawlers_configs = settings('crawlers', [])
    crawler_urls = set(map(
        lambda c: c.get('url'),
        crawlers_configs,
    ))

    if url and url not in crawler_urls:
        crawlers_configs.append({'url': url})

    for crawler_config in crawlers_configs:
        if url == crawler_config['url']:
            for browser_name in browser_plugins.keys():
                try:
                    with create_browser(browser_name, **crawler_config) as browser:
                        if children := browser.children(clean_path):
                            print_json(data=sorted([c.name() for c in children]))
                        else:
                            print_json(data=[])
                except Exception as e:
                        log.debug(
                            'Skipped browser for url=%s name=%s',
                            url,
                            browser_name,
                            exc_info=e,
                        )

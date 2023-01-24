import logging
import typer
from pathlib import PurePosixPath
from recap.analyzers import create_analyzer
from recap.browsers import create_browser
from recap.config import settings
from recap.paths import create_catalog_path
from recap.plugins import load_browser_plugins
from recap.typing import AnalyzerInspector
from rich import print_json


app = typer.Typer(help="Read live metadata from a system, bypassing the catalog.")
log = logging.getLogger(__name__)


@app.command()
def read(
    plugin: str = typer.Argument(...,
        help="Analyzer plugin name. Run `recap plugins analyzers` to get a full list.",
    ),
    url: str = typer.Argument(...,
        help="URL to analyze. URL configs in settings.toml will be used, if availabe.",
    ),
    path: str = typer.Argument(...,
        help="Path to analyze.",
    ),
):
    """
    Analyzes and prints metadata for a URL and path.
    """

    crawlers_configs = settings('crawlers', [])
    crawler_urls = set(map(
        lambda c: c.get('url'),
        crawlers_configs,
    ))

    if url and url not in crawler_urls:
        crawlers_configs.append({'url': url})

    for crawler_config in crawlers_configs:
        if url == crawler_config['url']:
            with create_analyzer(plugin, **crawler_config) as analyzer:
                inspector = AnalyzerInspector(type(analyzer))
                if catalog_path := create_catalog_path(
                    path,
                    *inspector.input_path_types(),
                ):
                    if metadata := analyzer.analyze(catalog_path):
                        metadata_dict = metadata.dict(
                            by_alias=True,
                            exclude_defaults=True,
                            exclude_none=True,
                            exclude_unset=True,
                        )
                        return_dict = metadata_dict.get(
                            '__root__',
                            metadata_dict,
                        )
                        print_json(data=return_dict, sort_keys=True)
                else:
                    raise ValueError(
                        f"Invalid path={path} for analyzer={plugin}",
                    )


@app.command("list")
def list_(
    url: str = typer.Argument(...,
        help="URL to browse. URL configs in settings.toml will be used, if availabe.",
    ),
    path: str = typer.Argument(
        '/',
        help="Path to list children for.",
    ),
):
    """
    Lists directory children for a URL and path.
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

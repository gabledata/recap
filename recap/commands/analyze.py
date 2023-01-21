import typer
from recap.analyzers import create_analyzer
from recap.config import settings
from recap.paths import create_catalog_path
from recap.typing import AnalyzerInspector
from rich import print_json


app = typer.Typer()


@app.command(
    context_settings={
        "allow_extra_args": True,
        "ignore_unknown_options": True,
    },
)
def analyze(
    plugin: str = typer.Argument(...,
        help="Analyzer plugin name. Run `recap plugins analyzers` to get a full list.",
    ),
    url: str = typer.Argument(...,
        help="URL to crawl. URL configs in settings.toml will be used, if availabe.",
    ),
    path: str = typer.Argument(...,
        help="Path to analyze.",
    ),
):
    """
    Analyzes and returns metadata for a URL and path.
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

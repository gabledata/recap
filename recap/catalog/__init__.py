import importlib
from .abstract import AbstractCatalog
from .duckdb import DEFAULT_URL
from contextlib import contextmanager
from typing import Generator
from urllib.parse import urlparse


SCHEMES_TO_MODULES = {
    'file': 'recap.catalog.duckdb',
    'http': 'recap.catalog.recap',
}


"""
Tries to get the module to load based on the URL's scheme.
"""
def guess_module_name(url: str) -> str | None:
    parsed_url = urlparse(url)
    return SCHEMES_TO_MODULES.get(parsed_url.scheme)


@contextmanager
def open(**config) -> Generator[AbstractCatalog, None, None]:
    # Default to DuckDB for search if `catalog.module` isn't configured.
    url = config.get('url', DEFAULT_URL)
    guessed_module_name = guess_module_name(url)
    module_name = config.get('module', guessed_module_name)

    if not module_name:
        raise ValueError(
            "No `search.module` config found while creating a "
            "search index object."
        )
    module = importlib.import_module(module_name)
    with module.open(**config) as s:
        yield s

import importlib
from .abstract import AbstractStorage
from contextlib import contextmanager
from fsspec.registry import known_implementations
from typing import Generator
from urllib.parse import urlparse


"""
Get all fsspec schemes and put them into a dict like:
{
    'file': 'recap.storage.fs',
    's3': 'recap.storage.fs',
    ...
}
"""
FSSPEC_SCHEMES_TO_MODULES = dict(
    map(
        lambda s: (s, 'recap.storage.fs'),
        known_implementations,
    )
)

"""
`SCHEMES_TO_MODULES` defines the module location for each scheme type.
"""
SCHEMES_TO_MODULES = FSSPEC_SCHEMES_TO_MODULES | {
    # Override fsspec's HTTP reader with Recap's. Users can still use fsspec's
    # by explicitly setting `module = "recap.storage.fs"`
    'http': 'recap.storage.recap',
    # In the future, other implementations like SqliteStorage or DuckDbStorage
    # could go here, too.
}


"""
Tries to get the module to load based on the URL's scheme.
"""
def guess_module_name(url: str) -> str | None:
    parsed_url = urlparse(url)
    return SCHEMES_TO_MODULES.get(parsed_url.scheme)


@contextmanager
def open(**config) -> Generator[AbstractStorage, None, None]:
    # Default to FilesystemStorage if `storage.url` is not set.
    url = config.get('url', 'file://')
    guessed_module_name = guess_module_name(url)
    module_name = config.get('module', guessed_module_name)
    if not module_name:
        raise ValueError(
            f"Unable to find appropriate module for url='{url}'. "
            "Try setting a 'module' field in the config block."
        )
    module = importlib.import_module(module_name)
    with module.open(**config) as s:
        yield s

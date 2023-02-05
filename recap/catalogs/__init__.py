"""
Recap catalogs store metadata and expose read and search APIs. Recap ships with
a database catalog and Recap catalog implementation. The database catalog is
enabled by default (with SQLite).
"""

from contextlib import contextmanager
from typing import Generator

from recap.plugins import load_catalog_plugins

from .abstract import AbstractCatalog


@contextmanager
def create_catalog(
    plugin: str = "db",
    **config,
) -> Generator["AbstractCatalog", None, None]:
    catalog_plugins = load_catalog_plugins()
    catalog_plugin_module = catalog_plugins.get(plugin)
    assert (
        catalog_plugin_module
    ), f"Unable to find catalog plugin module={catalog_plugin_module}"
    with catalog_plugin_module.create_catalog(**config) as catalog:
        yield catalog

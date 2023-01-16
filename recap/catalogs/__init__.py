from .abstract import AbstractCatalog
from contextlib import contextmanager
from recap.plugins import load_catalog_plugins
from typing import Generator


@contextmanager
def create_catalog(
    plugin: str = 'db',
    **config,
) -> Generator['AbstractCatalog', None, None]:
    catalog_plugins = load_catalog_plugins()
    catalog_plugin_module = catalog_plugins.get(plugin)
    assert catalog_plugin_module, \
        f"Unable to find catalog plugin module={catalog_plugin_module}"
    with catalog_plugin_module.create_catalog(**config) as catalog:
        yield catalog

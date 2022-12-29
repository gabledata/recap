from .abstract import AbstractCatalog
from contextlib import contextmanager
from typing import Generator


@contextmanager
def open(**config) -> Generator['AbstractCatalog', None, None]:
    from recap.plugins import load_catalog_plugins
    type = config.get('type', 'duckdb')
    catalog_plugins = load_catalog_plugins()
    catalog_plugin_cls = catalog_plugins.get(type)
    assert catalog_plugin_cls, \
        f"Unable to find catalog plugin={type}"
    with catalog_plugin_cls.open(**config) as c:
        yield c

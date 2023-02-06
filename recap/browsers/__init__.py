"""
Different data infrastructure have different types of objects (tables, columns,
files, topics, partitions, and so on).

Recap uses a browser abstraction map infrastructure objects into a standard
directory format. A different browser is used for each type of infrastructure.
Browsers do not actually analyze a system's data for metadata; they simply show
what's available.

Recap comes with a database browser and a filesystem browser. Other browsers
can be implemented and added as plugins.
"""

from contextlib import contextmanager
from typing import Generator

from recap.plugins import load_browser_plugins

from .abstract import AbstractBrowser


@contextmanager
def create_browser(
    plugin: str,
    **config,
) -> Generator["AbstractBrowser", None, None]:
    browser_plugins = load_browser_plugins()
    if browser_module := browser_plugins.get(plugin):
        with browser_module.create_browser(**config) as browser:
            yield browser

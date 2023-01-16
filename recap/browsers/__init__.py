from .abstract import AbstractBrowser
from contextlib import contextmanager
from recap.plugins import load_browser_plugins
from typing import Generator


@contextmanager
def create_browser(
    plugin: str,
    **config,
) -> Generator['AbstractBrowser', None, None]:
    browser_plugins = load_browser_plugins()
    if browser_module := browser_plugins.get(plugin):
        with browser_module.create_browser(**config) as browser:
            yield browser

from contextlib import contextmanager
from typing import Generator

from recap.plugins import load_analyzer_plugins

from .abstract import AbstractAnalyzer


@contextmanager
def create_analyzer(
    plugin: str,
    **config,
) -> Generator["AbstractAnalyzer", None, None]:
    analyzer_plugins = load_analyzer_plugins()
    if analyzer_module := analyzer_plugins.get(plugin):
        with analyzer_module.create_analyzer(**config) as analyzer:
            yield analyzer

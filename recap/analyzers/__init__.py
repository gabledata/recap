from .abstract import AbstractAnalyzer
from contextlib import contextmanager
from recap.plugins import load_analyzer_plugins
from typing import Generator


@contextmanager
def create_analyzer(
    plugin: str,
    **config,
) -> Generator['AbstractAnalyzer', None, None]:
    analyzer_plugins = load_analyzer_plugins()
    if analyzer_module := analyzer_plugins.get(plugin):
        with analyzer_module.create_analyzer(**config) as analyzer:
            yield analyzer

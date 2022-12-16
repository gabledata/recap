import importlib
from .abstract import AbstractSearchIndex
from contextlib import contextmanager
from typing import Generator


@contextmanager
def open(**config) -> Generator[AbstractSearchIndex, None, None]:
    module_name = config['module']
    module = importlib.import_module(module_name)
    with module.open(**config) as s:
        yield s

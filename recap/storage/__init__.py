import importlib
from .abstract import AbstractStorage
from contextlib import contextmanager
from typing import Generator


@contextmanager
def open(**config) -> Generator[AbstractStorage, None, None]:
    module_name = config['module']
    module = importlib.import_module(module_name)
    with module.open(**config) as s:
        yield s

import importlib
from .abstract import AbstractStorage

def open(**config) -> AbstractStorage:
    module_name = config['module']
    module = importlib.import_module(module_name)
    return module.open(**config)

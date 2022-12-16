import importlib
from .abstract import AbstractIndexer, AbstractSearch

# TODO Two different open methods feels hacky.
def open_search(**config) -> AbstractSearch:
    module_name = config['module']
    module = importlib.import_module(module_name)
    return module.open_search(**config)

def open_indexer(**config) -> AbstractIndexer:
    module_name = config['module']
    module = importlib.import_module(module_name)
    return module.open_indexer(**config)

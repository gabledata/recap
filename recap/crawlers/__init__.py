import importlib
# TODO When I create an AbstractCrawler, use it here
from recap.crawlers.db import Crawler
from recap.storage.abstract import AbstractStorage


# TODO should type the return
def open(
    infra: str,
    instance: str,
    storage: AbstractStorage, **config
) -> Crawler:
    module_name = config['module']
    module = importlib.import_module(module_name)
    return module.open(
        infra,
        instance,
        storage,
        **config
    )

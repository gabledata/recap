import importlib
# TODO When I create an AbstractCrawler, use it here
from contextlib import contextmanager
from recap.crawlers.db import Crawler
from recap.storage.abstract import AbstractStorage
from typing import Generator

@contextmanager
def open(
    infra: str,
    instance: str,
    storage: AbstractStorage, **config
) -> Generator[Crawler, None, None]:
    module_name = config['module']
    module = importlib.import_module(module_name)
    with module.open(
        infra,
        instance,
        storage,
        **config
    ) as c:
        yield c

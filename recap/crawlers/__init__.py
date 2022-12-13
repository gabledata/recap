from contextlib import contextmanager
from recap.crawlers import db
from recap.storage.abstract import AbstractStorage
from typing import Generator
from urllib.parse import urlparse


registry = {
    'postgresql': db,
    # TODO add other DB schemes here
}


# TODO should type the return
def open(
    infra: str,
    instance: str,
    storage: AbstractStorage, **config
):
    url = urlparse(config['url'])
    return registry[url.scheme].open(
        infra,
        instance,
        storage,
        **config
    )

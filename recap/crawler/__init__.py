from contextlib import contextmanager
from recap.crawler import db
from recap.storage.abstract import TableStorage
from typing import Generator
from urllib.parse import urlparse


registry = {
    'postgresql': db,
    # TODO add other DB schemes here
}


def open(storage: TableStorage, **config):
    url = urlparse(config['url'])
    return registry[url.scheme].open(storage, **config)

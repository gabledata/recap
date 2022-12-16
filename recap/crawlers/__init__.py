from contextlib import contextmanager
from recap.crawlers import db
from recap.storage.abstract import AbstractStorage
from typing import Generator
from urllib.parse import urlparse


registry = {
    # These are the included SQLAlchemy dialects
    'mssql': db,
    'mysql': db,
    'oracle': db,
    'postgresql': db,
    'sqlite': db,
}


# TODO should type the return
def open(
    infra: str,
    instance: str,
    storage: AbstractStorage, **config
):
    url = urlparse(config['url'])
    # Handle schemes like "postgresql+psycopg2"
    scheme_prefix = url.scheme.split('+')[0]
    return registry[scheme_prefix].open(
        infra,
        instance,
        storage,
        **config
    )

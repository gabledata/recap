import importlib
# TODO When I create an AbstractCrawler, use it here
from contextlib import contextmanager
from recap.crawlers.db import Crawler
from recap.storage.abstract import AbstractStorage
from typing import Generator
from urllib.parse import urlparse


def guess_infra(url: str) -> str | None:
    parsed_url = urlparse(url)
    # Given `posgrestql+psycopg2://foo:bar@baz/some_db`, return `postgresql`.
    return parsed_url.scheme.split('+')[0]

def guess_instance(url: str) -> str | None:
    parsed_url = urlparse(url)
    # Given `posgrestql+psycopg2://foo:bar@baz/some_db`, return `some_db`.
    path_fragments = parsed_url \
        .path \
        .strip('/') \
        .split('/')
    return path_fragments[0] if path_fragments else None


@contextmanager
def open(
    storage: AbstractStorage,
    **config,
) -> Generator[Crawler, None, None]:
    assert 'url' in config, \
        f"No url defined for crawler config: {config}"
    default_infra = guess_infra(config['url'])
    default_instance = guess_instance(config['url'])
    infra = config.get('infra', default_infra)
    instance = config.get('instance', default_instance)
    # This is the only module right now. Should add a guess_module method
    # (like in storage's `__init__.open`) when we add more.
    module_name = 'recap.crawlers.db'
    module = importlib.import_module(module_name)
    with module.open(
        infra,
        instance,
        storage,
        **config
    ) as c:
        yield c

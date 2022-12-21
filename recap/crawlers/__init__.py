import importlib
# TODO When I create an AbstractCrawler, use it here
from contextlib import contextmanager
from recap.crawlers.abstract import AbstractCrawler
from recap.catalog.abstract import AbstractCatalog
from typing import Generator
from urllib.parse import urlparse


def guess_infra(url: str) -> str | None:
    parsed_url = urlparse(url)
    # Given `posgrestql+psycopg2://foo:bar@baz/some_db`, return `postgresql`.
    return parsed_url.scheme.split('+')[0]

def guess_instance(url: str) -> str | None:
    parsed_url = urlparse(url)
    # Given `posgrestql+psycopg2://foo:bar@baz/some_db`, return `baz`.
    return parsed_url.netloc.split('@')[-1]


@contextmanager
def open(
    catalog: AbstractCatalog,
    **config,
) -> Generator[AbstractCrawler, None, None]:
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
        catalog,
        **config
    ) as c:
        yield c

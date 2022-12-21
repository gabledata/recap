import sqlalchemy as sa
from .browser import DatabaseBrowser
from .crawler import DatabaseCrawler
from contextlib import contextmanager
from recap.catalog.abstract import AbstractCatalog
from recap.crawlers.db.analyzers import DEFAULT_ANALYZERS
from typing import Generator


@contextmanager
def open(
    infra: str,
    instance: str,
    catalog: AbstractCatalog,
    **config,
) -> Generator[DatabaseCrawler, None, None]:
    engine = sa.create_engine(config['url'])
    browser = DatabaseBrowser(engine)
    # TODO make analyzers configurable
    analyzers = [analyzer(engine) for analyzer in DEFAULT_ANALYZERS]
    yield DatabaseCrawler(infra, instance, catalog, browser, analyzers)

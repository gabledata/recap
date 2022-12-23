import importlib
import sqlalchemy as sa
from .browser import DatabaseBrowser
from .analyzers import AbstractTableAnalyzer, DEFAULT_ANALYZERS
from .crawler import DatabaseCrawler
from contextlib import contextmanager
from recap.catalog.abstract import AbstractCatalog
from typing import Generator


def get_analyzer(module_and_class: str) -> AbstractTableAnalyzer:
    module, clazz = module_and_class.rsplit('.', maxsplit=1)
    return getattr(importlib.import_module(module), clazz)


@contextmanager
def open(
    infra: str,
    instance: str,
    catalog: AbstractCatalog,
    **config,
) -> Generator[DatabaseCrawler, None, None]:
    engine = sa.create_engine(config['url'])
    browser = DatabaseBrowser(engine)
    analyzers = config.get('analyzers', DEFAULT_ANALYZERS)
    analyzers = map(lambda c: get_analyzer(c), analyzers)
    analyzers = [clazz(engine) for clazz in analyzers] # pyright: ignore [reportGeneralTypeIssues]
    filters = config.get('filters', [])
    yield DatabaseCrawler(
        infra,
        instance,
        catalog,
        browser,
        analyzers,
        filters,
    )

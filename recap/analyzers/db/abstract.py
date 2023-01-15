import logging
import sqlalchemy as sa
from abc import abstractmethod
from contextlib import contextmanager
from pathlib import PurePosixPath
from pydantic import BaseModel
from recap.analyzers.abstract import AbstractAnalyzer
from recap.browsers.db import DatabasePath
from typing import Generator


log = logging.getLogger(__name__)


class AbstractDatabaseAnalyzer(AbstractAnalyzer):
    def __init__(
        self,
        engine: sa.engine.Engine,
    ):
        self.engine = engine

    def analyze(self, path: PurePosixPath) -> BaseModel | None:
        if len(path.parts) > 8:
            schema = path.parts[6]
            table = path.parts[8]
            is_view = path.parts[7] == 'views'
            return self.analyze_table(schema, table, is_view)
        return None

    @abstractmethod
    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> BaseModel | None:
        raise NotImplementedError

    @classmethod
    @contextmanager
    def open(cls, **config) -> Generator['AbstractDatabaseAnalyzer', None, None]:
        assert 'url' in config, \
            f"Config for {cls.__name__} is missing `url` config."
        engine = sa.create_engine(config['url'])
        yield cls(engine)

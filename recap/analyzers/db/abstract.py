import logging
import sqlalchemy as sa
from abc import abstractmethod
from contextlib import contextmanager
from pathlib import PurePosixPath
from recap.analyzers.abstract import AbstractAnalyzer
from recap.browsers.db import DatabasePath
from typing import Any, Generator


log = logging.getLogger(__name__)


class AbstractDatabaseAnalyzer(AbstractAnalyzer):
    def __init__(
        self,
        engine: sa.engine.Engine,
    ):
        self.engine = engine

    def analyze(self, path: PurePosixPath) -> dict[str, Any]:
        database_path = DatabasePath(path)
        schema = database_path.schema
        table = database_path.table
        if schema and table:
            is_view = path.parts[3] == 'views'
            return self.analyze_table(schema, table, is_view)
        return {}

    @abstractmethod
    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> dict[str, Any]:
        raise NotImplementedError

    @classmethod
    @contextmanager
    def open(cls, **config) -> Generator['AbstractDatabaseAnalyzer', None, None]:
        assert 'url' in config, \
            f"Config for {cls.__name__} is missing `url` config."
        engine = sa.create_engine(config['url'])
        yield cls(engine)

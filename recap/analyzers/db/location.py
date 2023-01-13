import logging
import sqlalchemy as sa
from .abstract import AbstractDatabaseAnalyzer
from contextlib import contextmanager
from pathlib import PurePosixPath
from recap.browsers.db import DatabaseBrowser
from typing import Any, Generator


log = logging.getLogger(__name__)


class TableLocationAnalyzer(AbstractDatabaseAnalyzer):
    def __init__(
        self,
        root: PurePosixPath,
        engine: sa.engine.Engine,
    ):
        self.root = root
        self.engine = engine
        self.database = root.parts[2]
        self.instance = root.parts[4]

    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> dict[str, Any]:
        if schema and table:
            table_or_view = 'view' if is_view else 'table'
            return {
                'location': {
                    'database': self.database,
                    'instance': self.instance,
                    'schema': schema,
                    table_or_view: table,
                }
            }
        return {}

    @classmethod
    @contextmanager
    def open(cls, **config) -> Generator['TableLocationAnalyzer', None, None]:
        assert 'url' in config, \
            f"Config for {cls.__name__} is missing `url` config."
        engine = sa.create_engine(config['url'])
        root = DatabaseBrowser.root(**config)
        yield TableLocationAnalyzer(root, engine)

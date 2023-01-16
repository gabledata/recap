import logging
import sqlalchemy as sa
from .abstract import AbstractDatabaseAnalyzer
from contextlib import contextmanager
from pathlib import PurePosixPath
from pydantic import Field
from recap.analyzers.abstract import BaseMetadataModel
from recap.browsers.db import DatabaseBrowser, TablePath, ViewPath
from typing import Generator


log = logging.getLogger(__name__)


class Location(BaseMetadataModel):
    database: str
    instance: str
    # Schema is a reserved word in BaseModel.
    schema_: str = Field(alias='schema')
    # TODO Should validate either table or view is set and show in JSON schema.
    table: str | None = None
    view: str | None = None


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

    def analyze(
        self,
        path: TablePath | ViewPath,
    ) -> Location | None:
        is_table = isinstance(path, TablePath)
        table = path.table if is_table else path.view
        table_or_view = 'table' if is_table else 'view'
        location_dict = {
            'database': self.database,
            'instance': self.instance,
            'schema': path.schema_,
            table_or_view: table,
        }
        return Location.parse_obj(location_dict)

    @classmethod
    @contextmanager
    def open(cls, **config) -> Generator['TableLocationAnalyzer', None, None]:
        assert 'url' in config, \
            f"Config for {cls.__name__} is missing `url` config."
        engine = sa.create_engine(config['url'])
        with DatabaseBrowser.open(**config) as browser:
            yield TableLocationAnalyzer(browser.instance.path(), engine)

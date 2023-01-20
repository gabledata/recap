import logging
import sqlalchemy
from contextlib import contextmanager
from recap.analyzers.abstract import AbstractAnalyzer, BaseMetadataModel
from recap.browsers.db import create_browser, TablePath, ViewPath
from typing import Generator


log = logging.getLogger(__name__)


class Index(BaseMetadataModel):
    columns: list[str]
    unique: bool


class Indexes(BaseMetadataModel):
    __root__: dict[str, Index] = {}


class TableIndexAnalyzer(AbstractAnalyzer):
    def __init__(self, engine: sqlalchemy.engine.Engine):
        self.engine = engine

    def analyze(
        self,
        path: TablePath | ViewPath,
    ) -> Indexes | None:
        table = path.table if isinstance(path, TablePath) else path.view
        indexes = {}
        index_dicts = sqlalchemy.inspect(self.engine).get_indexes(
            table,
            path.schema_,
        )
        for index_dict in index_dicts:
            indexes[index_dict['name']] = {
                'columns': index_dict.get('column_names', []),
                'unique': index_dict['unique'],
            }
        if indexes:
            return Indexes.parse_obj(indexes)
        return None


@contextmanager
def create_analyzer(
    **config,
) -> Generator['TableIndexAnalyzer', None, None]:
    with create_browser(**config) as browser:
        yield TableIndexAnalyzer(browser.engine)

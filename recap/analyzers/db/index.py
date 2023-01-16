import logging
import sqlalchemy as sa
from .abstract import AbstractDatabaseAnalyzer
from recap.analyzers.abstract import BaseMetadataModel
from recap.browsers.db import TablePath, ViewPath


log = logging.getLogger(__name__)


class Index(BaseMetadataModel):
    columns: list[str]
    unique: bool


class Indexes(BaseMetadataModel):
    __root__: dict[str, Index] = {}


class TableIndexAnalyzer(AbstractDatabaseAnalyzer):
    def analyze(
        self,
        path: TablePath | ViewPath,
    ) -> Indexes | None:
        table = path.table if isinstance(path, TablePath) else path.view
        indexes = {}
        index_dicts = sa.inspect(self.engine).get_indexes(table, path.schema_)
        for index_dict in index_dicts:
            indexes[index_dict['name']] = {
                'columns': index_dict.get('column_names', []),
                'unique': index_dict['unique'],
            }
        if indexes:
            return Indexes.parse_obj(indexes)
        return None

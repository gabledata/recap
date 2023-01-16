import logging
import sqlalchemy as sa
from .abstract import AbstractDatabaseAnalyzer
from recap.analyzers.abstract import BaseMetadataModel


log = logging.getLogger(__name__)


class Index(BaseMetadataModel):
    columns: list[str]
    unique: bool


class Indexes(BaseMetadataModel):
    __root__: dict[str, Index] = {}


class TableIndexAnalyzer(AbstractDatabaseAnalyzer):
    def analyze(
        self,
        schema: str,
        table: str | None = None,
        view: str | None = None,
        **_,
    ) -> Indexes | None:
        table = self._table_or_view(table, view)
        indexes = {}
        index_dicts = sa.inspect(self.engine).get_indexes(table, schema)
        for index_dict in index_dicts:
            indexes[index_dict['name']] = {
                'columns': index_dict.get('column_names', []),
                'unique': index_dict['unique'],
            }
        if indexes:
            return Indexes.parse_obj(indexes)
        return None

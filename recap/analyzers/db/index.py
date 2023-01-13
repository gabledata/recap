import logging
import sqlalchemy as sa
from .abstract import AbstractDatabaseAnalyzer
from recap.analyzers.abstract import BaseMetadataModel
from typing import List


log = logging.getLogger(__name__)


class Index(BaseMetadataModel):
    columns: List[str]
    unique: bool


class Indexes(BaseMetadataModel):
    __root__: dict[str, Index] = {}


class TableIndexAnalyzer(AbstractDatabaseAnalyzer):
    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> Indexes | None:
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

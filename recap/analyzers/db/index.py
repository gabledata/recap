import logging
import sqlalchemy as sa
from .abstract import AbstractDatabaseAnalyzer
from typing import Any


log = logging.getLogger(__name__)


class TableIndexAnalyzer(AbstractDatabaseAnalyzer):
    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> dict[str, Any]:
        indexes = {}
        index_dicts = sa.inspect(self.engine).get_indexes(table, schema)
        for index_dict in index_dicts:
            indexes[index_dict['name']] = {
                'columns': index_dict.get('column_names', []),
                'unique': index_dict['unique'],
            }
        return {'indexes': indexes} if indexes else {}

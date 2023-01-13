import logging
import sqlalchemy as sa
from .abstract import AbstractDatabaseAnalyzer
from typing import Any


log = logging.getLogger(__name__)


class TableForeignKeyAnalyzer(AbstractDatabaseAnalyzer):
    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> dict[str, Any]:
        fk_dict = sa.inspect(self.engine).get_foreign_keys(table, schema)
        return {'foreign_keys': fk_dict} if fk_dict else {}

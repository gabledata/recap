import logging
import sqlalchemy as sa
from .abstract import AbstractDatabaseAnalyzer
from typing import Any


log = logging.getLogger(__name__)


class TablePrimaryKeyAnalyzer(AbstractDatabaseAnalyzer):
    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> dict[str, Any]:
        pk_dict = sa.inspect(self.engine).get_pk_constraint(table, schema)
        return {'primary_key': pk_dict} if pk_dict else {}

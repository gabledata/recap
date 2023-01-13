import logging
import sqlalchemy as sa
from .abstract import AbstractDatabaseAnalyzer
from typing import Any


log = logging.getLogger(__name__)


class TableViewDefinitionAnalyzer(AbstractDatabaseAnalyzer):
    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> dict[str, Any]:
        if not is_view:
            return {}
        # TODO sqlalchemy-bigquery doesn't work right with this API
        # https://github.com/googleapis/python-bigquery-sqlalchemy/issues/539
        if self.engine.dialect.name == 'bigquery':
            table = f"{schema}.{table}"
        def_dict = sa.inspect(self.engine).get_view_definition(table, schema)
        return {'view_definition': def_dict} if def_dict else {}

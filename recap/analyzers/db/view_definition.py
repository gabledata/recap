import logging
import sqlalchemy as sa
from .abstract import AbstractDatabaseAnalyzer
from pydantic import BaseModel


log = logging.getLogger(__name__)


class ViewDefinition(BaseModel):
    __root__: str | None = None


class TableViewDefinitionAnalyzer(AbstractDatabaseAnalyzer):
    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> ViewDefinition | None:
        if is_view:
            # TODO sqlalchemy-bigquery doesn't work right with this API
            # https://github.com/googleapis/python-bigquery-sqlalchemy/issues/539
            if self.engine.dialect.name == 'bigquery':
                table = f"{schema}.{table}"
            def_dict = sa \
                .inspect(self.engine) \
                .get_view_definition(table, schema)
            if def_dict:
                return ViewDefinition.parse_obj(def_dict)
        return None

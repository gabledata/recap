import logging
import sqlalchemy as sa
from .abstract import AbstractDatabaseAnalyzer
from recap.analyzers.abstract import BaseMetadataModel


log = logging.getLogger(__name__)


class ForeignKey(BaseMetadataModel):
    constrained_columns: list[str]
    referred_columns: list[str]
    referred_schema: str
    referred_table: str


class ForeignKeys(BaseMetadataModel):
    __root__: dict[str, ForeignKey] = {}


class TableForeignKeyAnalyzer(AbstractDatabaseAnalyzer):
    def analyze(
        self,
        schema: str,
        table: str | None = None,
        view: str | None = None,
        **_,
    ) -> ForeignKeys | None:
        table = self._table_or_view(table, view)
        results = {}
        fks = sa.inspect(self.engine).get_foreign_keys(table, schema)
        for fk_dict in fks or []:
            results[fk_dict['name']] = ForeignKey(
                constrained_columns=fk_dict['constrained_columns'],
                referred_columns=fk_dict['referred_columns'],
                referred_schema=fk_dict['referred_schema'],
                referred_table=fk_dict['referred_table'],
            )
        if results:
            return ForeignKeys.parse_obj(results)
        return None

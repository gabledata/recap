import logging
import sqlalchemy as sa
from .abstract import AbstractDatabaseAnalyzer
from pydantic import BaseModel
from typing import List


log = logging.getLogger(__name__)


class ForeignKey(BaseModel):
    constrained_columns: List[str]
    referred_columns: List[str]
    referred_schema: str
    referred_table: str


class ForeignKeys(BaseModel):
    __root__: dict[str, ForeignKey] = {}


class TableForeignKeyAnalyzer(AbstractDatabaseAnalyzer):
    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> ForeignKeys | None:
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

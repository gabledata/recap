import logging
import sqlalchemy as sa
from .abstract import AbstractDatabaseAnalyzer
from pydantic import BaseModel
from typing import Any, List


log = logging.getLogger(__name__)


class PrimaryKey(BaseModel):
    name: str
    constrained_columns: List[str]


class TablePrimaryKeyAnalyzer(AbstractDatabaseAnalyzer):
    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> PrimaryKey | None:
        pk_dict = sa.inspect(self.engine).get_pk_constraint(table, schema)
        if pk_dict and pk_dict.get('name'):
            return PrimaryKey.parse_obj(pk_dict)
        return None

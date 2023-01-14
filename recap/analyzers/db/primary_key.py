import logging
import sqlalchemy as sa
from .abstract import AbstractDatabaseAnalyzer
from recap.analyzers.abstract import BaseMetadataModel


log = logging.getLogger(__name__)


class PrimaryKey(BaseMetadataModel):
    name: str
    constrained_columns: list[str]


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

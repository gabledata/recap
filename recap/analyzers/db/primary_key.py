import logging
import sqlalchemy as sa
from .abstract import AbstractDatabaseAnalyzer
from recap.analyzers.abstract import BaseMetadataModel
from recap.browsers.db import TablePath, ViewPath


log = logging.getLogger(__name__)


class PrimaryKey(BaseMetadataModel):
    name: str
    constrained_columns: list[str]


class TablePrimaryKeyAnalyzer(AbstractDatabaseAnalyzer):
    def analyze(
        self,
        path: TablePath | ViewPath,
    ) -> PrimaryKey | None:
        table = path.table if isinstance(path, TablePath) else path.view
        pk_dict = sa.inspect(self.engine).get_pk_constraint(
            table,
            path.schema_,
        )
        if pk_dict and pk_dict.get('name'):
            return PrimaryKey.parse_obj(pk_dict)
        return None

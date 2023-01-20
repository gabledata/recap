import logging
import sqlalchemy
from contextlib import contextmanager
from recap.analyzers.abstract import AbstractAnalyzer, BaseMetadataModel
from recap.browsers.db import create_browser, TablePath, ViewPath
from typing import Generator


log = logging.getLogger(__name__)


class Column(BaseMetadataModel):
    autoincrement: bool | None
    default: str | None
    nullable: bool
    type: str
    generic_type: str | None
    comment: str | None


class Columns(BaseMetadataModel):
    __root__: dict[str, Column] = {}


class TableColumnAnalyzer(AbstractAnalyzer):
    def __init__(self, engine: sqlalchemy.engine.Engine):
        self.engine = engine

    def analyze(
        self,
        path: TablePath | ViewPath,
    ) -> Columns | None:
        table = path.table if isinstance(path, TablePath) else path.view
        results = {}
        columns = sqlalchemy.inspect(self.engine).get_columns(
            table,
            path.schema_,
        )
        for column in columns:
            try:
                generic_type = column['type'].as_generic()
                # Strip length/precision to make generic strings more generic.
                if isinstance(generic_type, sqlalchemy.sql.sqltypes.String):
                    generic_type.length = None
                elif isinstance(generic_type, sqlalchemy.sql.sqltypes.Numeric):
                    generic_type.precision = None
                    generic_type.scale = None
                column['generic_type'] = str(generic_type)
            except NotImplementedError as e:
                # Unable to convert. Probably a weird type like PG's OID.
                log.debug(
                    'Unable to get generic type for path=%s column=%s',
                    path,
                    column.get('name', column),
                    exc_info=e,
                )
            # The `type` field is not JSON encodable; convert to string.
            column['type'] = str(column['type'])
            column_name = column['name']
            del column['name']
            results[column_name] = Column(
                autoincrement=column.get('autoincrement'),
                default=column['default'],
                generic_type=column.get('generic_type'),
                nullable=column['nullable'],
                type=str(column['type']),
                comment=column.get('comment')
            )
        if results:
            return Columns.parse_obj(results)
        return None


@contextmanager
def create_analyzer(**config) -> Generator['TableColumnAnalyzer', None, None]:
    with create_browser(**config) as browser:
        yield TableColumnAnalyzer(browser.engine)

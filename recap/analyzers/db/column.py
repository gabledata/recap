import logging
import sqlalchemy as sa
from .abstract import AbstractDatabaseAnalyzer
from pydantic import BaseModel


log = logging.getLogger(__name__)


class Column(BaseModel):
    autoincrement: bool
    default: str | None
    nullable: bool
    type: str
    generic_type: str | None
    comment: str | None


class Columns(BaseModel):
    __root__: dict[str, Column] = {}


class TableColumnAnalyzer(AbstractDatabaseAnalyzer):
    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> Columns | None:
        results = {}
        columns = sa.inspect(self.engine).get_columns(table, schema)
        for column in columns:
            if column.get('comment', None) is None:
                del column['comment']
            try:
                generic_type = column['type'].as_generic()
                # Strip length/precision to make generic strings more generic.
                if isinstance(generic_type, sa.sql.sqltypes.String):
                    generic_type.length = None
                elif isinstance(generic_type, sa.sql.sqltypes.Numeric):
                    generic_type.precision = None
                    generic_type.scale = None
                column['generic_type'] = str(generic_type)
            except NotImplementedError as e:
                # Unable to convert. Probably a weird type like PG's OID.
                log.debug(
                    'Unable to get generic type for table=%s.%s column=%s',
                    schema,
                    table,
                    column.get('name', column),
                    exc_info=e,
                )
            # The `type` field is not JSON encodable; convert to string.
            column['type'] = str(column['type'])
            column_name = column['name']
            del column['name']
            results[column_name] = Column(
                autoincrement=column['autoincrement'],
                default=column['default'],
                generic_type=column.get('generic_type'),
                nullable=column['nullable'],
                type=str(column['type']),
                comment=column.get('comment')
            )
        if results:
            return Columns.parse_obj(results)
        return None

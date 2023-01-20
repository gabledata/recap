import logging
import sqlalchemy
from .columns import TableColumnAnalyzer, Columns
from contextlib import contextmanager
from pydantic import BaseModel
from recap.analyzers.abstract import AbstractAnalyzer, BaseMetadataModel
from recap.browsers.db import create_browser, TablePath, ViewPath
from typing import Generator


log = logging.getLogger(__name__)


class BaseColumnProfile(BaseModel):
    count: int


class BinaryColumnProfile(BaseColumnProfile):
    min_length: int | None
    max_length: int | None
    distinct: int | None
    nulls: int | None


class DateColumnProfile(BaseColumnProfile):
    min: str | None
    max: str | None
    distinct: int | None
    nulls: int | None
    unix_epochs: int | None


class NumericColumnProfile(BaseColumnProfile):
    min: int | float | None
    max: int | float | None
    average: int | float | None
    sum: int | float | None
    nulls: int | None
    zeros: int | None
    negatives: int | None


class StringColumnProfile(BaseColumnProfile):
    min_length: int | None
    max_length: int | None
    distinct: int | None
    nulls: int | None
    empty_strings: int | None


ColumnProfile = (
    BinaryColumnProfile |
    DateColumnProfile |
    NumericColumnProfile |
    StringColumnProfile |
    # This must be at the end, or the REST API might use it when encoding JSON
    # instead of more specific types.
    BaseColumnProfile
)


class Profile(BaseMetadataModel):
    __root__: dict[str, ColumnProfile] = {}


class TableProfileAnalyzer(AbstractAnalyzer):
    def __init__(self, engine: sqlalchemy.engine.Engine):
        self.engine = engine

    def analyze(
        self,
        path: TablePath | ViewPath,
    ) -> Profile | None:
        table = path.table if isinstance(path, TablePath) else path.view
        column_analyzer = TableColumnAnalyzer(self.engine)
        # TODO This is very proof-of-concept...
        # TODO ZOMG SQL injection attacks all over!
        # TODO Is db.Table().select the right way to paramaterize tables?
        columns = column_analyzer.analyze(path) or Columns()
        sql_col_queries = ''
        numeric_types = [
            'BIGINT', 'FLOAT', 'INT', 'INTEGER', 'NUMERIC', 'REAL', 'SMALLINT'
        ]
        date_types = [
            'DATE', 'DATETIME', 'TIMESTAMP'
        ]
        # TODO Excluding 'JSON' because PG's 'JSONB' doesn't have LENGTH()
        string_types = [
            'CHAR', 'CLOB', 'NCHAR', 'NVARCHAR', 'TEXT', 'VARCHAR'
        ]
        binary_types = [
            'BLOB', 'VARBINARY'
        ]
        stat_types = [
            'min',
            'max',
            'average',
            'sum',
            'distinct',
            'nulls',
            'zeros',
            'negatives',
            'min_length',
            'max_length',
            'empty_strings',
            'unix_epochs',
        ]

        with self.engine.connect() as conn:
            varchar_type = 'VARCHAR'

            # BigQuery doesn't havt FLOAT or VARCHAR, so use its type.
            # TODO SQLAlchemy should expose a dialect type for a generic type.
            if conn.dialect.name == 'bigquery':
                varchar_type = 'STRING'
            elif conn.dialect.name == 'snowflake':
                conn.execute("ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE")

            for column_name, column in columns.dict()['__root__'].items():
                generic_type = column['generic_type']
                quoted_column_name = f'"{column_name}"'
                if conn.dialect.name in ['bigquery', 'mysql']:
                    quoted_column_name = f'`{column_name}`'
                if generic_type in numeric_types:
                    # TODO add approx median and quantiles
                    # TODO can we use a STRUCT or something here?
                    sql_col_queries += f"""
                        , MIN({quoted_column_name}) AS min_{column_name}
                        , MAX({quoted_column_name}) AS max_{column_name}
                        , AVG({quoted_column_name}) AS average_{column_name}
                        , SUM({quoted_column_name}) AS sum_{column_name}
                        , COUNT(DISTINCT {quoted_column_name}) AS {column_name}_distinct
                        , SUM(CASE WHEN {quoted_column_name} IS NULL THEN 1 ELSE 0 END) AS nulls_{column_name}
                        , SUM(CASE WHEN {quoted_column_name} = 0 THEN 1 ELSE 0 END) AS zeros_{column_name}
                        , SUM(CASE WHEN {quoted_column_name} < 0 THEN 1 ELSE 0 END) AS negatives_{column_name}
                    """
                if generic_type in string_types:
                    sql_col_queries += f"""
                        , MIN(LENGTH({quoted_column_name})) AS min_length_{column_name}
                        , MAX(LENGTH({quoted_column_name})) AS max_length_{column_name}
                        , COUNT(DISTINCT {quoted_column_name}) AS distinct_{column_name}
                        , SUM(CASE WHEN {quoted_column_name} IS NULL THEN 1 ELSE 0 END) AS nulls_{column_name}
                        , SUM(CASE WHEN {quoted_column_name} LIKE '' THEN 1 ELSE 0 END) AS empty_strings_{column_name}
                    """
                if generic_type in binary_types:
                    sql_col_queries += f"""
                        , MIN(LENGTH({quoted_column_name})) AS min_length_{column_name}
                        , MAX(LENGTH({quoted_column_name})) AS max_length_{column_name}
                        , COUNT(DISTINCT {quoted_column_name}) AS distinct_{column_name}
                        , SUM(CASE WHEN {quoted_column_name} IS NULL THEN 1 ELSE 0 END) AS nulls_{column_name}
                    """
                if generic_type in date_types:
                    sql_col_queries += f"""
                        , CAST(MIN({quoted_column_name}) AS {varchar_type}) AS min_{column_name}
                        , CAST(MAX({quoted_column_name}) AS {varchar_type}) AS max_{column_name}
                        , COUNT(DISTINCT {quoted_column_name}) AS distinct_{column_name}
                        , SUM(CASE WHEN {quoted_column_name} IS NULL THEN 1 ELSE 0 END) AS nulls_{column_name}
                        , SUM(CASE WHEN {quoted_column_name} = TIMESTAMP '1970-01-01 00:00:00' THEN 1 ELSE 0 END) AS unix_epochs_{column_name}
                    """

            quoted_schema = f'"{path.schema_}"'
            quoted_table = f'"{table}"'
            if conn.dialect.name in ['bigquery', 'mysql']:
                quoted_schema = f'`{path.schema_}`'
                quoted_table = f'`{table}`'

            sql = f"""
                SELECT
                    COUNT(*) AS count {sql_col_queries}
                FROM
                    {quoted_schema}.{quoted_table} t
            """

            rows = conn.execute(sql)
            row = dict(rows.first() or {})
            results = {}

            for column_name in columns.dict()['__root__'].keys():
                col_stats = results.get(column_name, {'count': row['count']})
                for stat_type in stat_types:
                    stat_name = f"{stat_type}_{column_name}"
                    if stat_name in row:
                        stat_value = row[stat_name]
                        # JSON encoder can't handle decimal.Decimal
                        import decimal
                        if isinstance(row[stat_name], decimal.Decimal):
                            stat_value = float(row[stat_name])
                        col_stats[stat_type] = stat_value
                results[column_name] = col_stats

            results = {}
            for column_name, column in columns.__root__.items():
                generic_type = column.generic_type
                col_stats = {'count': row['count']}
                for stat_type in stat_types:
                    stat_name = f"{stat_type}_{column_name}"
                    if stat_name in row:
                        stat_value = row[stat_name]
                        # JSON encoder can't handle decimal.Decimal
                        import decimal
                        if isinstance(row[stat_name], decimal.Decimal):
                            stat_value = float(row[stat_name])
                        col_stats[stat_type] = stat_value
                if generic_type in numeric_types:
                    results[column_name] = NumericColumnProfile(**col_stats)
                elif generic_type in string_types:
                    results[column_name] = StringColumnProfile(**col_stats)
                elif generic_type in binary_types:
                    results[column_name] = BinaryColumnProfile(**col_stats)
                elif generic_type in date_types:
                    results[column_name] = DateColumnProfile(**col_stats)
                else:
                    results[column_name] = BaseColumnProfile(**col_stats)

            return Profile.parse_obj(results)


@contextmanager
def create_analyzer(
    **config,
) -> Generator['TableProfileAnalyzer', None, None]:
    with create_browser(**config) as browser:
        yield TableProfileAnalyzer(browser.engine)

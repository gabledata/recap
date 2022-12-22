import sqlalchemy as sa
from .browser import DatabaseBrowser
from abc import ABC, abstractmethod
from typing import Any


class AbstractTableAnalyzer(ABC):
    def __init__(
        self,
        engine: sa.engine.Engine
    ):
        self.engine = engine

    @abstractmethod
    def analyze(self, schema: str, table: str) -> dict[str, Any]:
        raise NotImplementedError


class TableColumnAnalyzer(AbstractTableAnalyzer):
    def analyze(self, schema: str, table: str) -> dict[str, Any]:
        results = {}
        columns = sa.inspect(self.engine).get_columns(table, schema)
        # The type field is not JSON encodable; convert to string.
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
            except NotImplementedError:
                # Unable to convert. Probably a weird type like PG's OID.
                pass
            column['type'] = str(column['type'])
            column_name = column['name']
            del column['name']
            results[column_name] = column
        return {'columns': results} if results else {}


class TableIndexAnalyzer(AbstractTableAnalyzer):
    def analyze(self, schema: str, table: str) -> dict[str, Any]:
        indexes = {}
        index_dicts = sa.inspect(self.engine).get_indexes(table, schema)
        for index_dict in index_dicts:
            indexes[index_dict['name']] = {
                'columns': index_dict.get('column_names', []),
                'unique': index_dict['unique'],
            }
        return {'indexes': indexes} if indexes else {}


class TablePrimaryKeyAnalyzer(AbstractTableAnalyzer):
    def analyze(self, schema: str, table: str) -> dict[str, Any]:
        pk_dict = sa.inspect(self.engine).get_pk_constraint(table, schema)
        return {'primary_key': pk_dict} if pk_dict else {}


class TableForeignKeyAnalyzer(AbstractTableAnalyzer):
    def analyze(self, schema: str, table: str) -> dict[str, Any]:
        fk_dict = sa.inspect(self.engine).get_foreign_keys(table, schema)
        return {'foreign_keys': fk_dict} if fk_dict else {}


class ViewDefinitionAnalyzer(AbstractTableAnalyzer):
    def analyze(self, schema: str, table: str) -> dict[str, Any]:
        # TODO sqlalchemy-bigquery doesn't work right with this API
        # https://github.com/googleapis/python-bigquery-sqlalchemy/issues/539
        if self.engine.dialect.name == 'bigquery':
            table = f"{schema}.{table}"
        def_dict = sa.inspect(self.engine).get_view_definition(table, schema)
        return {'view_definition': def_dict} if def_dict else {}


class TableCommentAnalyzer(AbstractTableAnalyzer):
    def analyze(self, schema: str, table: str) -> dict[str, Any]:
        try:
            comment = sa.inspect(self.engine).get_table_comment(table, schema)
            comment_text = comment.get('text', None)
            return {'table_comment': comment_text} if comment_text else {}
        except NotImplementedError:
            # Not all dialects support comments
            return {}


class TableAccessAnalyzer(AbstractTableAnalyzer):
    def analyze(self, schema: str, table: str) -> dict[str, Any]:
        with self.engine.connect() as conn:
            results = {}
            try:
                rows = conn.execute(
                    "SELECT * FROM information_schema.role_table_grants "
                    "WHERE table_schema = %s AND table_name = %s",
                    schema,
                    table,
                )
                for row in rows.all():
                    privilege_type = row['privilege_type']
                    user_grants: dict[str, Any] = results.get(row['grantee'], {
                        'privileges': [],
                        'read': False,
                        'write': False,
                    })
                    user_grants['privileges'].append(privilege_type)
                    if privilege_type == 'SELECT':
                        user_grants['read'] = True
                    if privilege_type in ['INSERT', 'UPDATE', 'DELETE', 'TRUNCATE']:
                        user_grants['write'] = True
                    results[row['grantee']] = user_grants
            except:
                # TODO probably need a more tightly bound exception here
                # We probably don't have access to the information_schema, so
                # skip it.
                pass
            return {'access': results} if results else {}


class TableDataAnalyzer(AbstractTableAnalyzer):
    def __init__(
        self,
        engine: sa.engine.Engine
    ):
        self.engine = engine
        self.browser = DatabaseBrowser(self.engine)
        self.column_analyzer = TableColumnAnalyzer(self.engine)

    def analyze(self, schema: str, table: str) -> dict[str, Any]:
        # TODO This is very proof-of-concept...
        # TODO ZOMG SQL injection attacks all over!
        # TODO Is db.Table().select the right way to paramaterize tables?
        tables_and_views = self.browser.tables(schema) + \
            self.browser.views(schema)
        assert table in tables_and_views, \
            f"Table or view is not in schema: {schema}.{table}"
        columns = self.column_analyzer \
            .analyze(schema, table) \
            .get('columns', {})
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
                float_type = 'FLOAT64'
                varchar_type = 'STRING'
            elif conn.dialect.name == 'snowflake':
                conn.execute("ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE")

            for column_name, column in columns.items():
                generic_type = column.get('generic_type')
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

            quoted_schema = f'"{schema}"'
            quoted_table = f'"{table}"'
            if conn.dialect.name in ['bigquery', 'mysql']:
                quoted_schema = f'`{schema}`'
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

            for column_name in columns.keys():
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

            return {'data_profile': results}


DEFAULT_ANALYZERS = [
    TableAccessAnalyzer,
    TableColumnAnalyzer,
    TableCommentAnalyzer,
    TableDataAnalyzer,
    TableForeignKeyAnalyzer,
    TableIndexAnalyzer,
    TablePrimaryKeyAnalyzer,
    ViewDefinitionAnalyzer,
]
import sqlalchemy as sa
from contextlib import contextmanager
from pathlib import PurePosixPath
from recap.catalog.abstract import AbstractCatalog
from typing import Any, Generator, List


class Metadata:
    def __init__(
        self,
        engine: sa.engine.Engine
    ):
        self.engine = engine

    def schemas(self) -> List[str]:
        return sa.inspect(self.engine).get_schema_names()

    def tables(self, schema: str) -> List[str]:
        results = []
        for table in sa.inspect(self.engine).get_table_names(schema):
            if table.startswith(f"{schema}."):
                table = table[len(schema) + 1:]
            results.append(table)
        return results

    # TODO this is a dupe of tables(schema)
    def views(self, schema: str) -> List[str]:
        results = []
        for view in sa.inspect(self.engine).get_view_names(schema):
            if view.startswith(f"{schema}."):
                view = view[len(schema) + 1:]
            results.append(view)
        return results

    def columns(self, schema: str, table_or_view: str) -> dict[str, Any]:
        results = {}
        columns = sa.inspect(self.engine).get_columns(table_or_view, schema)
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
                # Unable to convert. Probably a wird type like PG's OID.
                pass
            column['type'] = str(column['type'])
            column_name = column['name']
            del column['name']
            results[column_name] = column
        return results

    def indexes(self, schema: str, table_or_view: str) -> dict[str, Any]:
        indexes = {}
        index_dicts = sa.inspect(self.engine).get_indexes(table_or_view, schema)
        for index_dict in index_dicts:
            indexes[index_dict['name']] = {
                'columns': index_dict.get('column_names', []),
                'unique': index_dict['unique'],
            }
        return indexes

    def primary_key(self, schema: str, table_or_view: str) -> dict[str, Any]:
        return sa.inspect(self.engine).get_pk_constraint(table_or_view, schema)

    def foreign_keys(self, schema: str, table_or_view: str) -> List[dict[str, str]]:
        return sa.inspect(self.engine).get_foreign_keys(table_or_view, schema)

    def view_definition(self, schema: str, view: str) -> str:
        return sa.inspect(self.engine).get_view_definition(view, schema)

    def table_comment(self, schema: str, table_or_view: str) -> str | None:
        try:
            comment = sa.inspect(self.engine).get_table_comment(table_or_view, schema)
            return comment.get('text', None)
        except NotImplementedError:
            # Not all dialects support comments
            return None

    # TODO We aren't paying attention to the table's catalog. This seems problematic.
    def role_table_grants(self, schema: str, table_or_view: str) -> dict[str, Any]:
        with self.engine.connect() as conn:
            results = {}
            try:
                rows = conn.execute(
                    "SELECT * FROM information_schema.role_table_grants "
                    "WHERE table_schema = %s AND table_name = %s",
                    schema,
                    table_or_view,
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
                # We probably don't have access to the information_schema, so skip it.
                pass
            return results

    def data_profile(self, schema: str, table_or_view: str) -> dict[str, Any]:
        # TODO This is very proof-of-concept...
        # TODO ZOMG SQL injection attacks all over!
        # TODO Is db.Table().select the right way to paramaterize tables?
        assert table_or_view in self.tables(schema) + self.views(schema), \
            f"Table or view is not in schema: {schema}.{table_or_view}"
        columns = self.columns(schema, table_or_view)
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

            sql = f"""
                SELECT
                    COUNT(*) AS count {sql_col_queries}
                FROM
                    {schema}.{table_or_view} t
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

            return results


# TODO We need an AbstractCrawler that DbCrawler inherits from.
class Crawler:
    def __init__(
        self,
        infra: str,
        instance: str,
        catalog: AbstractCatalog,
        metadata: Metadata,
    ):
        self.infra = infra
        self.instance = instance
        self.catalog = catalog
        self.metadata = metadata

    def crawl(self):
        self.catalog.touch(PurePosixPath(
            'databases', self.infra,
            'instances', self.instance,
        ))
        schemas = self.metadata.schemas()
        for schema in schemas:
            self.catalog.touch(PurePosixPath(
                'databases', self.infra,
                'instances', self.instance,
                'schemas', schema,
            ))
            views = self.metadata.views(schema)
            tables = self.metadata.tables(schema)
            for view in views:
                self._write_table_or_view(
                    schema,
                    view=view
                )
            for table in tables:
                self._write_table_or_view(
                    schema,
                    table=table
                )
            self._remove_deleted_tables(schema, tables)
        self._remove_deleted_schemas(schemas)

    # TODO Combine methods using a util that is agnostic the data being removed
    def _remove_deleted_schemas(self, schemas: List[str]):
        catalog_schemas = self.catalog.ls(PurePosixPath(
            'databases', self.infra,
            'instances', self.instance,
            'schemas'
        )) or []
        # Find schemas that are not currently in nstance
        schemas_to_remove = [s for s in catalog_schemas if s not in schemas]
        for schema in schemas_to_remove:
            self.catalog.rm(PurePosixPath(
                'databases', self.infra,
                'instances', self.instance,
                'schemas', schema,
            ))

    def _remove_deleted_tables(self, schema: str, tables: List[str]):
        catalog_tables = self.catalog.ls(PurePosixPath(
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
            'tables'
        )) or []
        # Find schemas that are not currently in nstance
        tables_to_remove = [t for t in catalog_tables if t not in tables]
        for table in tables_to_remove:
            self.catalog.rm(PurePosixPath(
                'databases', self.infra,
                'instances', self.instance,
                'schemas', schema,
                'tables', table,
            ))

    def _remove_deleted_views(self, schema: str, views: List[str]):
        catalog_views = self.catalog.ls(PurePosixPath(
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
            'views'
        )) or []
        # Find schemas that are not currently in nstance
        views_to_remove = [v for v in catalog_views if v not in views]
        for view in views_to_remove:
            self.catalog.rm(PurePosixPath(
                'databases', self.infra,
                'instances', self.instance,
                'schemas', schema,
                'views', view,
            ))

    def  _write_table_or_view(
        self,
        schema: str,
        table: str | None = None,
        view: str | None = None
    ):
        columns = None
        indexes = None
        primary_key = None
        foreign_keys = None
        view_definition = None
        table_comment = None
        role_table_grants = None
        data_profile = None
        path = PurePosixPath(
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
        )

        if table:
            path = PurePosixPath(path, 'tables', table)
            columns = self.metadata.columns(schema, table)
            indexes = self.metadata.indexes(schema, table)
            primary_key = self.metadata.primary_key(schema, table)
            foreign_keys = self.metadata.foreign_keys(schema, table)
            table_comment = self.metadata.table_comment(schema, table)
            role_table_grants = self.metadata.role_table_grants(schema, table)
            data_profile = self.metadata.data_profile(schema, table)
        elif view:
            path = PurePosixPath(path, 'views', view)
            columns = self.metadata.columns(schema, view)
            indexes = self.metadata.indexes(schema, view)
            primary_key = self.metadata.primary_key(schema, view)
            foreign_keys = self.metadata.foreign_keys(schema, view)
            view_definition = self.metadata.view_definition(schema, view)
            table_comment = self.metadata.table_comment(schema, view)
            role_table_grants = self.metadata.role_table_grants(schema, view)
            data_profile = self.metadata.data_profile(schema, view)
        else:
            raise ValueError(
                "Must specify either 'table' or 'view' when writing metadata"
            )

        location = self._location(
            schema,
            table=table,
            view=view,
        )
        # TODO Should have AbstractCatalog.write allow for multiple type dicts
        self.catalog.write(
            path,
            'location',
            location,
        )
        self.catalog.write(
            path,
            'columns',
            columns,
        )
        if primary_key:
            self.catalog.write(
                path,
                'primary_key',
                primary_key,
            )
        if indexes:
            self.catalog.write(
                path,
                'indexes',
                indexes,
            )
        if foreign_keys:
            self.catalog.write(
                path,
                'foreign_keys',
                foreign_keys,
            )
        if table_comment:
            self.catalog.write(
                path,
                'table_comment',
                table_comment,
            )
        if view_definition:
            self.catalog.write(
                path,
                'view_definition',
                view_definition,
            )
        if role_table_grants:
            self.catalog.write(
                path,
                'access',
                role_table_grants,
            )
        if data_profile:
            self.catalog.write(
                path,
                'data_profile',
                data_profile,
            )

    def _location(
        self,
        schema: str,
        table: str | None = None,
        view: str | None = None
    ) -> dict[str, str]:
        assert table or view, \
            "Must specify either 'table' or 'view' for a location dictionary"
        location = {
            'database': self.infra,
            'instance': self.instance,
            'schema': schema,
        }
        if table:
            location['table'] = table
        elif view:
            location['view'] = view
        return location


@contextmanager
def open(
    infra: str,
    instance: str,
    catalog: AbstractCatalog,
    **config
) -> Generator[Crawler, None, None]:
    engine = sa.create_engine(config['url'])
    db = Metadata(engine)
    yield Crawler(infra, instance, catalog, db)

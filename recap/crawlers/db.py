import sqlalchemy as sa
from contextlib import contextmanager
from pathlib import PurePosixPath
from recap.catalog.abstract import AbstractCatalog
from typing import List, Generator


class Metadata:
    def __init__(
        self,
        engine: sa.engine.Engine
    ):
        self.engine = engine

    def schemas(self) -> List[str]:
        return sa.inspect(self.engine).get_schema_names()

    def tables(self, schema: str) -> List[str]:
        return sa.inspect(self.engine).get_table_names(schema)

    def views(self, schema: str) -> List[str]:
        return sa.inspect(self.engine).get_view_names(schema)

    def columns(self, schema: str, table_or_view: str) -> List[dict[str, str]]:
        columns = sa.inspect(self.engine).get_columns(table_or_view, schema)
        # The type field is not JSON encodable; convert to string.
        for column in columns:
            try:
                column['generic_type'] = str(column['type'].as_generic())
            except NotImplementedError:
                # Unable to convert. Probably a wird type like PG's OID.
                pass
            column['type'] = str(column['type'])
        return columns

    def indexes(self, schema: str, table_or_view: str) -> List[dict[str, str]]:
        return sa.inspect(self.engine).get_indexes(table_or_view, schema)

    def primary_key(self, schema: str, table_or_view: str) -> List[dict[str, str]]:
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
    def role_table_grants(self, schema: str, table_or_view: str) -> List[dict[str, str]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM information_schema.role_table_grants "
                "WHERE table_schema = %s AND table_name = %s",
                schema,
                table_or_view,
            )
            return [dict(r) for r in rows.all()]


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
        # TODO should naively loop crawling forever with a sleep between passes
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
        elif view:
            path = PurePosixPath(path, 'views', view)
            columns = self.metadata.columns(schema, view)
            indexes = self.metadata.indexes(schema, view)
            primary_key = self.metadata.primary_key(schema, view)
            foreign_keys = self.metadata.foreign_keys(schema, view)
            view_definition = self.metadata.view_definition(schema, view)
            table_comment = self.metadata.table_comment(schema, view)
            role_table_grants = self.metadata.role_table_grants(schema, view)
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
                'grants',
                role_table_grants,
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

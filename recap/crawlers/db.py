import sqlalchemy as sa
from contextlib import contextmanager
from pathlib import PurePosixPath
from recap.storage.abstract import AbstractStorage
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
            column['type'] = str(column['type'])
        return columns

    # TODO get indexes and foreign keys and stuff


# TODO We need an AbstractCrawler that DbCrawler inherits from.
class Crawler:
    def __init__(
        self,
        infra: str,
        instance: str,
        storage: AbstractStorage,
        metadata: Metadata,
    ):
        self.infra = infra
        self.instance = instance
        self.storage = storage
        self.metadata = metadata

    def crawl(self):
        # TODO should naively loop crawling forever with a sleep between passes
        self.storage.touch(PurePosixPath(
            'databases', self.infra,
            'instances', self.instance,
        ))
        schemas = self.metadata.schemas()
        for schema in schemas:
            self.storage.touch(PurePosixPath(
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
        storage_schemas = self.storage.ls(PurePosixPath(
            'databases', self.infra,
            'instances', self.instance,
            'schemas'
        )) or []
        # Find schemas that are not currently in nstance
        schemas_to_remove = [s for s in storage_schemas if s not in schemas]
        for schema in schemas_to_remove:
            self.storage.rm(PurePosixPath(
                'databases', self.infra,
                'instances', self.instance,
                'schemas', schema,
            ))

    def _remove_deleted_tables(self, schema: str, tables: List[str]):
        storage_tables = self.storage.ls(PurePosixPath(
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
            'tables'
        )) or []
        # Find schemas that are not currently in nstance
        tables_to_remove = [t for t in storage_tables if t not in tables]
        for table in tables_to_remove:
            self.storage.rm(PurePosixPath(
                'databases', self.infra,
                'instances', self.instance,
                'schemas', schema,
                'tables', table,
            ))

    def _remove_deleted_views(self, schema: str, views: List[str]):
        storage_views = self.storage.ls(PurePosixPath(
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
            'views'
        )) or []
        # Find schemas that are not currently in nstance
        views_to_remove = [v for v in storage_views if v not in views]
        for view in views_to_remove:
            self.storage.rm(PurePosixPath(
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
        columns = {}
        path = PurePosixPath(
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
        )

        if table:
            path = PurePosixPath(path, 'tables', table)
            columns = self.metadata.columns(schema, table)
        elif view:
            path = PurePosixPath(path, 'views', view)
            columns = self.metadata.columns(schema, view)
        else:
            raise ValueError(
                "Must specify either 'table' or 'view' when writing metadata"
            )

        location_dict = self._location_dict(
            schema,
            table=table,
            view=view,
        )
        self.storage.write(
            path,
            'columns', columns
        )
        self.storage.write(
            path,
            'location', location_dict
        )

    def _location_dict(
        self,
        schema: str,
        table: str | None = None,
        view: str | None = None
    ) -> dict[str, str]:
        assert table or view, \
            "Must specify either 'table' or 'view' for a location dictionary"
        location_dict = {
            'database': self.infra,
            'instance': self.instance,
            'schema': schema,
        }
        if table:
            location_dict['table'] = table
        elif view:
            location_dict['view'] = view
        return location_dict


@contextmanager
def open(
    infra: str,
    instance: str,
    storage: AbstractStorage,
    **config
) -> Generator[Crawler, None, None]:
    engine = sa.create_engine(config['url'])
    db = Metadata(engine)
    yield Crawler(infra, instance, storage, db)

import sqlalchemy as sa
from recap.storage import TableStorage
from typing import List


class Instance:
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

    # TODO get indexes and foreig keys and stuff


class Crawler:
    def __init__(
        self,
        storage: TableStorage,
        instance: Instance,
    ):
        self.storage = storage
        self.instance = instance

    def crawl(self):
        self.storage.put_instance()
        schemas = self.instance.schemas()
        for schema in schemas:
            self.storage.put_schema(schema)
            views = self.instance.views(schema)
            tables = self.instance.tables(schema)
            for view in views:
                columns = {'columns': self.instance.columns(schema, view)}
                self.storage.put_metadata('columns', columns, schema, view=view)
            for table in tables:
                columns = {'columns': self.instance.columns(schema, table)}
                self.storage.put_metadata('columns', columns, schema, table=table)
            self._remove_deleted_views(schema, views)
            self._remove_deleted_tables(schema, tables)
        self._remove_deleted_schemas(schemas)

    # TODO Combine methods using a util that is agnostic the data being removed
    def _remove_deleted_schemas(self, schemas: List[str]):
        storage_schemas = self.storage.list_schemas()
        # Find schemas that are not currently in nstance
        schemas_to_remove = [s for s in storage_schemas if s not in schemas]
        for schema in schemas_to_remove:
            self.storage.remove_schema(schema)

    def _remove_deleted_tables(self, schema: str, tables: List[str]):
        storage_tables = self.storage.list_tables(schema)
        # Find schemas that are not currently in nstance
        tables_to_remove = [t for t in storage_tables if t not in tables]
        for table in tables_to_remove:
            self.storage.remove_table(schema, table)

    def _remove_deleted_views(self, schema: str, views: List[str]):
        storage_views = self.storage.list_views(schema)
        # Find schemas that are not currently in nstance
        views_to_remove = [v for v in storage_views if v not in views]
        for view in views_to_remove:
            self.storage.remove_view(schema, view)

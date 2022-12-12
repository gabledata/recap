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
        for schema in self.instance.schemas():
            self.storage.put_schema(schema)
            for view in self.instance.views(schema):
                columns = {'columns': self.instance.columns(schema, view)}
                self.storage.put_metadata('columns', columns, schema, view=view)
            for table in self.instance.tables(schema):
                columns = {'columns': self.instance.columns(schema, table)}
                self.storage.put_metadata('columns', columns, schema, table=table)

import logging
import sqlalchemy as sa
from .abstract import AbstractDatabaseAnalyzer
from pydantic import Field
from recap.analyzers.abstract import BaseMetadataModel
from recap.browsers.db import InstancePath, TablePath, ViewPath


log = logging.getLogger(__name__)


class Location(BaseMetadataModel):
    database: str
    instance: str
    # Schema is a reserved word in BaseModel.
    schema_: str = Field(alias='schema')
    # TODO Should validate either table or view is set and show in JSON schema.
    table: str | None = None
    view: str | None = None


class TableLocationAnalyzer(AbstractDatabaseAnalyzer):
    def __init__(
        self,
        instance: InstancePath,
        engine: sa.engine.Engine,
    ):
        self.instance = instance
        self.engine = engine

    def analyze(
        self,
        path: TablePath | ViewPath,
    ) -> Location | None:
        is_table = isinstance(path, TablePath)
        table = path.table if is_table else path.view
        table_or_view = 'table' if is_table else 'view'
        location_dict = {
            'database': self.instance.scheme,
            'instance': self.instance.instance,
            'schema': path.schema_,
            table_or_view: table,
        }
        return Location.parse_obj(location_dict)

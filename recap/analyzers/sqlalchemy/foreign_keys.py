import logging
import sqlalchemy
from contextlib import contextmanager
from recap.analyzers.abstract import AbstractAnalyzer, BaseMetadataModel
from recap.browsers.db import create_browser, TablePath, ViewPath
from typing import Generator


log = logging.getLogger(__name__)


class ForeignKey(BaseMetadataModel):
    constrained_columns: list[str]
    referred_columns: list[str]
    referred_schema: str
    referred_table: str


class ForeignKeys(BaseMetadataModel):
    __root__: dict[str, ForeignKey] = {}


class TableForeignKeyAnalyzer(AbstractAnalyzer):
    def __init__(self, engine: sqlalchemy.engine.Engine):
        self.engine = engine

    def analyze(
        self,
        path: TablePath | ViewPath,
    ) -> ForeignKeys | None:
        table = path.table if isinstance(path, TablePath) else path.view
        results = {}
        fks = sqlalchemy.inspect(self.engine).get_foreign_keys(
            table,
            path.schema_,
        )
        for fk_dict in fks or []:
            results[fk_dict['name']] = ForeignKey(
                constrained_columns=fk_dict['constrained_columns'],
                referred_columns=fk_dict['referred_columns'],
                referred_schema=fk_dict['referred_schema'],
                referred_table=fk_dict['referred_table'],
            )
        if results:
            return ForeignKeys.parse_obj(results)
        return None


@contextmanager
def create_analyzer(
    **config,
) -> Generator['TableForeignKeyAnalyzer', None, None]:
    with create_browser(**config) as browser:
        yield TableForeignKeyAnalyzer(browser.engine)

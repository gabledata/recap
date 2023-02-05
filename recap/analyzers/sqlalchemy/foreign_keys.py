import logging
from contextlib import contextmanager
from typing import Generator

import sqlalchemy

from recap.analyzers.abstract import AbstractAnalyzer, BaseMetadataModel
from recap.browsers.db import TablePath, ViewPath, create_browser

log = logging.getLogger(__name__)


class ForeignKey(BaseMetadataModel):
    constrained_columns: list[str]
    referred_columns: list[str]
    referred_schema: str
    referred_table: str


class ForeignKeys(BaseMetadataModel):
    __root__: dict[str, ForeignKey] = {}


class TableForeignKeyAnalyzer(AbstractAnalyzer):
    """
    Use SQLAlchemy to fetch foreign key information for a table.
    """

    def __init__(self, engine: sqlalchemy.engine.Engine):
        self.engine = engine

    def analyze(
        self,
        path: TablePath | ViewPath,
    ) -> ForeignKeys | None:
        """
        :param path: Fetch foreign key information for a table at this path.
        :returns: Foreign key information or None if there is no foreign key.
        """

        table = path.table if isinstance(path, TablePath) else path.view
        results = {}
        fks = sqlalchemy.inspect(self.engine).get_foreign_keys(
            table,
            path.schema_,
        )
        for fk_dict in fks or []:
            results[fk_dict["name"]] = ForeignKey(
                constrained_columns=fk_dict["constrained_columns"],
                referred_columns=fk_dict["referred_columns"],
                referred_schema=fk_dict["referred_schema"],
                referred_table=fk_dict["referred_table"],
            )
        if results:
            return ForeignKeys.parse_obj(results)
        return None


@contextmanager
def create_analyzer(
    **config,
) -> Generator["TableForeignKeyAnalyzer", None, None]:
    with create_browser(**config) as browser:
        yield TableForeignKeyAnalyzer(browser.engine)

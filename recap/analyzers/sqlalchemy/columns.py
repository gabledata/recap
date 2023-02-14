import logging
from contextlib import contextmanager
from typing import Generator

import sqlalchemy

from recap.analyzers.abstract import AbstractAnalyzer
from recap.browsers.db import DatabaseURL, create_browser
from recap.schemas.schema import Field, Schema

log = logging.getLogger(__name__)


class TableColumnAnalyzer(AbstractAnalyzer):
    """
    Use SQLAlchemy to fetch table schema information for a table or view. The
    schema uses SQLAlchemy's schema format.
    """

    def __init__(self, engine: sqlalchemy.engine.Engine):
        self.engine = engine

    def analyze(
        self,
        path: str,
    ) -> Schema | None:
        """
        :param path: Fetch column schema information for a table or view at
            this path.
        :returns: Column schema information.
        """

        match DatabaseURL(str(self.engine.url), path):
            case DatabaseURL(schema=str(schema), table=str(table)):
                results = {}
                columns = sqlalchemy.inspect(self.engine).get_columns(
                    table,
                    schema,
                )
                return Schema(
                    fields=[
                        Field(
                            name=column["name"],
                            type=str(column["type"]),
                            default=column["default"],
                            nullable=column["nullable"],
                            comment=column.get("comment"),
                        )
                        for column in columns
                    ],
                )


@contextmanager
def create_analyzer(**config) -> Generator["TableColumnAnalyzer", None, None]:
    with create_browser(**config) as browser:
        yield TableColumnAnalyzer(browser.engine)

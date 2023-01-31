import logging
import sqlalchemy
from contextlib import contextmanager
from recap.analyzers.abstract import AbstractAnalyzer, BaseMetadataModel
from recap.browsers.db import create_browser, TablePath, ViewPath
from typing import Generator


log = logging.getLogger(__name__)


class PrimaryKey(BaseMetadataModel):
    name: str
    constrained_columns: list[str]


class TablePrimaryKeyAnalyzer(AbstractAnalyzer):
    """
    Use SQLAlchemy to fetch primary key information for a table.
    """

    def __init__(self, engine: sqlalchemy.engine.Engine):
        self.engine = engine

    def analyze(
        self,
        path: TablePath | ViewPath,
    ) -> PrimaryKey | None:
        """
        :param path: Fetch primary key information for a table at this path.
        :returns: Primary key information or None if there is no primary key.
        """

        table = path.table if isinstance(path, TablePath) else path.view
        pk_dict = sqlalchemy.inspect(self.engine).get_pk_constraint(
            table,
            path.schema_,
        )
        if pk_dict and pk_dict.get('name'):
            return PrimaryKey.parse_obj(pk_dict)
        return None


@contextmanager
def create_analyzer(
    **config,
) -> Generator['TablePrimaryKeyAnalyzer', None, None]:
    with create_browser(**config) as browser:
        yield TablePrimaryKeyAnalyzer(browser.engine)

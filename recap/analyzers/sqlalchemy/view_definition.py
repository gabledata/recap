import logging
from contextlib import contextmanager
from typing import Generator

import sqlalchemy

from recap.analyzers.abstract import AbstractAnalyzer, BaseMetadataModel
from recap.browsers.db import ViewPath, create_browser

log = logging.getLogger(__name__)


class ViewDefinition(BaseMetadataModel):
    __root__: str | None = None


class TableViewDefinitionAnalyzer(AbstractAnalyzer):
    """
    Use SQLAlchemy to fetch a query that creates a view.
    """

    def __init__(self, engine: sqlalchemy.engine.Engine):
        self.engine = engine

    def analyze(
        self,
        path: ViewPath,
    ) -> ViewDefinition | None:
        """
        :param path: Fetch query for a view at this path.
        :returns: A view query string.
        """

        # TODO sqlalchemy-bigquery doesn't work right with this API
        # https://github.com/googleapis/python-bigquery-sqlalchemy/issues/539
        view = path.view
        if self.engine.dialect.name == "bigquery":
            view = f"{path.schema_}.{path.view}"
        def_dict = sqlalchemy.inspect(self.engine).get_view_definition(
            view, path.schema_
        )
        if def_dict:
            return ViewDefinition.parse_obj(def_dict)
        return None


@contextmanager
def create_analyzer(
    **config,
) -> Generator["TableViewDefinitionAnalyzer", None, None]:
    with create_browser(**config) as browser:
        yield TableViewDefinitionAnalyzer(browser.engine)

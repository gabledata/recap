import logging
import sqlalchemy as sa
from .abstract import AbstractDatabaseAnalyzer
from recap.analyzers.abstract import BaseMetadataModel
from recap.browsers.db import ViewPath


log = logging.getLogger(__name__)


class ViewDefinition(BaseMetadataModel):
    __root__: str | None = None


class TableViewDefinitionAnalyzer(AbstractDatabaseAnalyzer):
    def analyze(
        self,
        path: ViewPath,
    ) -> ViewDefinition | None:
        # TODO sqlalchemy-bigquery doesn't work right with this API
        # https://github.com/googleapis/python-bigquery-sqlalchemy/issues/539
        view = path.view
        if self.engine.dialect.name == 'bigquery':
            view = f"{path.schema_}.{path.view}"
        def_dict = sa \
            .inspect(self.engine) \
            .get_view_definition(view, path.schema_)
        if def_dict:
            return ViewDefinition.parse_obj(def_dict)
        return None

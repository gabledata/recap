import logging
from contextlib import contextmanager
from typing import Generator

import sqlalchemy

from recap.analyzers.abstract import AbstractAnalyzer, BaseMetadataModel
from recap.browsers.db import TablePath, ViewPath, create_browser

log = logging.getLogger(__name__)


class Comment(BaseMetadataModel):
    __root__: str | None = None


class TableCommentAnalyzer(AbstractAnalyzer):
    """
    Use SQLAlchemy to get table comments.
    """

    def __init__(self, engine: sqlalchemy.engine.Engine):
        self.engine = engine

    def analyze(
        self,
        path: TablePath | ViewPath,
    ) -> Comment | None:
        """
        :param path: Fetch table comment for a table or view at this path.
        :returns: A table comment or None if it's not set.
        """

        table = path.table if isinstance(path, TablePath) else path.view
        comment = sqlalchemy.inspect(self.engine).get_table_comment(
            table,
            path.schema_,
        )
        comment_text = comment.get("text")
        if comment_text:
            return Comment.parse_obj(comment_text)
        return None


@contextmanager
def create_analyzer(**config) -> Generator["TableCommentAnalyzer", None, None]:
    with create_browser(**config) as browser:
        yield TableCommentAnalyzer(browser.engine)

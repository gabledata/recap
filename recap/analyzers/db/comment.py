import logging
import sqlalchemy
from contextlib import contextmanager
from recap.analyzers.abstract import AbstractAnalyzer, BaseMetadataModel
from recap.browsers.db import create_browser, TablePath, ViewPath
from typing import Generator


log = logging.getLogger(__name__)


class Comment(BaseMetadataModel):
    __root__: str | None = None


class TableCommentAnalyzer(AbstractAnalyzer):
    def __init__(self, engine: sqlalchemy.engine.Engine):
        self.engine = engine

    def analyze(
        self,
        path: TablePath | ViewPath,
    ) -> Comment | None:
        table = path.table if isinstance(path, TablePath) else path.view
        comment = sqlalchemy.inspect(self.engine).get_table_comment(
            table,
            path.schema_,
        )
        comment_text = comment.get('text')
        if comment_text:
            return Comment.parse_obj(comment_text)
        return None


@contextmanager
def create_analyzer(**config) -> Generator['TableCommentAnalyzer', None, None]:
    with create_browser(**config) as browser:
        yield TableCommentAnalyzer(browser.engine)

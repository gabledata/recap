import logging
import sqlalchemy as sa
from .abstract import AbstractDatabaseAnalyzer
from recap.analyzers.abstract import BaseMetadataModel
from recap.browsers.db import TablePath, ViewPath


log = logging.getLogger(__name__)


class Comment(BaseMetadataModel):
    __root__: str | None = None


class TableCommentAnalyzer(AbstractDatabaseAnalyzer):
    def analyze(
        self,
        path: TablePath | ViewPath,
    ) -> Comment | None:
        table = path.table if isinstance(path, TablePath) else path.view
        comment = sa.inspect(self.engine).get_table_comment(
            table,
            path.schema_,
        )
        comment_text = comment.get('text')
        if comment_text:
            return Comment.parse_obj(comment_text)
        return None

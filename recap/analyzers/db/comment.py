import logging
import sqlalchemy as sa
from .abstract import AbstractDatabaseAnalyzer
from recap.analyzers.abstract import BaseMetadataModel


log = logging.getLogger(__name__)


class Comment(BaseMetadataModel):
    __root__: str | None = None


class TableCommentAnalyzer(AbstractDatabaseAnalyzer):
    def analyze(
        self,
        schema: str,
        table: str | None = None,
        view: str | None = None,
        **_,
    ) -> Comment | None:
        table = self._table_or_view(table, view)
        try:
            comment = sa.inspect(self.engine).get_table_comment(table, schema)
            comment_text = comment.get('text')
            if comment_text:
                return Comment.parse_obj(comment_text)
        except NotImplementedError as e:
            log.debug(
                'Unable to get comment for table=%s.%s',
                schema,
                table,
                exc_info=e,
            )
        return None

import logging
import sqlalchemy as sa
from .abstract import AbstractDatabaseAnalyzer
from typing import Any


log = logging.getLogger(__name__)


class TableCommentAnalyzer(AbstractDatabaseAnalyzer):
    def analyze_table(
        self,
        schema: str,
        table: str,
        is_view: bool = False
    ) -> dict[str, Any]:
        try:
            comment = sa.inspect(self.engine).get_table_comment(table, schema)
            comment_text = comment.get('text')
            return {'comment': comment_text} if comment_text else {}
        except NotImplementedError as e:
            log.debug(
                'Unable to get comment for table=%s.%s',
                schema,
                table,
                exc_info=e,
            )
            return {}

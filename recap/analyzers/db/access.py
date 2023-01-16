import logging
from .abstract import AbstractDatabaseAnalyzer
from recap.analyzers.abstract import BaseMetadataModel
from typing import Any


log = logging.getLogger(__name__)


class UserAccess(BaseMetadataModel):
    privileges: list[str]
    read: bool
    write: bool


class Access(BaseMetadataModel):
    __root__: dict[str, UserAccess] = {}


class TableAccessAnalyzer(AbstractDatabaseAnalyzer):
    def analyze(
        self,
        schema: str,
        table: str | None = None,
        view: str | None = None,
    ) -> Access | None:
        table = self._table_or_view(table, view)
        with self.engine.connect() as conn:
            results = {}
            try:
                rows = conn.execute(
                    "SELECT * FROM information_schema.role_table_grants "
                    "WHERE table_schema = %s AND table_name = %s",
                    schema,
                    table,
                )
                for row in rows.all():
                    privilege_type = row['privilege_type']
                    user_grants: dict[str, Any] = results.get(row['grantee'], {
                        'privileges': [],
                        'read': False,
                        'write': False,
                    })
                    user_grants['privileges'].append(privilege_type)
                    if privilege_type == 'SELECT':
                        user_grants['read'] = True
                    if privilege_type in ['INSERT', 'UPDATE', 'DELETE', 'TRUNCATE']:
                        user_grants['write'] = True
                    results[row['grantee']] = user_grants
            except Exception as e:
                # TODO probably need a more tightly bound exception here
                # We probably don't have access to the information_schema, so
                # skip it.
                log.debug(
                    'Unable to fetch access for table=%s.%s',
                    schema,
                    table,
                    exc_info=e,
                )
            if results:
                return Access.parse_obj(results)
            return None

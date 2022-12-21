import sqlalchemy as sa
from recap.crawlers.abstract import AbstractBrowser
from pathlib import PurePosixPath
from typing import List, Callable


class DatabaseBrowser(AbstractBrowser):
    def __init__(
        self,
        engine: sa.engine.Engine
    ):
        self.engine = engine

    def children(self, path: PurePosixPath) -> List[str]:
        if path.name == 'schemas':
            return self.schemas()
        elif path.name == 'tables':
            return self.tables(path.parent.name)
        elif path.name == 'views':
            return self.views(path.parent.name)
        return []

    def schemas(self) -> List[str]:
        return sa.inspect(self.engine).get_schema_names()

    def tables(self, schema: str) -> List[str]:
        return self._tables_or_views(
            schema,
            sa.inspect(self.engine).get_table_names,
        )

    def views(self, schema: str) -> List[str]:
        return self._tables_or_views(
            schema,
            sa.inspect(self.engine).get_view_names,
        )

    def _tables_or_views(
        self,
        schema: str,
        get_method: Callable[[str], List[str]],
    ) -> List[str]:
        results = []
        try:
            for table_or_view in get_method(schema):
                if table_or_view.startswith(f"{schema}."):
                    table_or_view = table_or_view[len(schema) + 1:]
                results.append(table_or_view)
        except:
            # Just optimistically try, and ignore if we can't get info.
            # Easier than trying to figure out if permission exists.
            pass
        return results

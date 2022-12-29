import logging
import sqlalchemy as sa
from contextlib import contextmanager
from .abstract import AbstractBrowser
from pathlib import PurePosixPath
from typing import Callable, Generator, List
from urllib.parse import urlparse


log = logging.getLogger(__name__)


class DatabasePath:
    def __init__(self, path: PurePosixPath):
        self.path = path
        self.schema = path.parts[2] if len(path.parts) > 2 else None
        self.table = path.parts[4] if len(path.parts) > 4 else None


class DatabaseBrowser(AbstractBrowser):
    def __init__(
        self,
        engine: sa.engine.Engine,
    ):
        self.url = str(engine.url)
        self.engine = engine

    def children(self, path: PurePosixPath) -> List[str]:
        num_parts = len(path.parts)
        database_path = DatabasePath(path)
        # TODO Should use a lookup table here for better performance?
        if num_parts == 1:
            # /
            return ['schemas']
        elif num_parts == 2 and path.parts[1] == 'schemas':
            # /schemas
            return self.schemas()
        elif num_parts == 3:
            # /schemas/<some_schema>
            return ['tables', 'views']
        elif num_parts == 4 and database_path.schema:
            if path.parts[3] == 'tables':
                # /schemas/<some_schema>/tables
                return self.tables(database_path.schema)
            if path.parts[3] == 'views':
                # /schemas/<some_schema>/views
                return self.views(database_path.schema)
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
                # Stripe schema name from the table/view name. Some dialects
                # include the schema name as part of the table/view. Let's keep
                # things consistent.
                if table_or_view.startswith(f"{schema}."):
                    table_or_view = table_or_view[len(schema) + 1:]
                results.append(table_or_view)
        except Exception as e:
            # Just optimistically try, and ignore if we can't get info.
            # Easier than trying to figure out if permission exists.
            log.debug(
                'Unable to fetch tables or views for schema=%s',
                schema,
                exc_info=e,
            )
        return results

    @staticmethod
    def root(**config) -> PurePosixPath:
        assert 'url' in config, \
            f"No url defined for browser config={config}"
        parsed_url = urlparse(config['url'])
        # Given `posgrestql+psycopg2://foo:bar@baz/some_db`, return `postgresql`.
        infra = parsed_url.scheme.split('+')[0]
        # Given `posgrestql+psycopg2://foo:bar@baz/some_db`, return `baz`.
        default_instance = parsed_url.netloc.split('@')[-1]
        instance = config['name'] if 'name' in config else default_instance
        return PurePosixPath(
            '/',
            'databases', infra,
            'instances', instance,
        )

    @staticmethod
    def browsable(url: str) -> bool:
        # TODO there's probably a better way to do this.
        # Seems like SQLAlchemy should have a method to check dialects.
        try:
            sa.create_engine(url)
            return True
        except Exception as e:
            log.debug('Unbrowsable. Create engine failed for url=%s', url)
            return False

    @staticmethod
    @contextmanager
    def open(**config) -> Generator['DatabaseBrowser', None, None]:
        assert 'url' in config, \
            f"No url defined for browser config={config}"
        yield DatabaseBrowser(sa.create_engine(config['url']))

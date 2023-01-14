import logging
import sqlalchemy as sa
from contextlib import contextmanager
from .abstract import AbstractBrowser
from pathlib import PurePosixPath
from typing import Callable, Generator
from urllib.parse import urlparse


log = logging.getLogger(__name__)


class DatabasePath:
    """
    Helper that exposes schema and table (or view) from a database path.
    """

    def __init__(self, path: PurePosixPath):
        self.path = path
        self.schema = path.parts[2] if len(path.parts) > 2 else None
        self.table = path.parts[4] if len(path.parts) > 4 else None


class DatabaseBrowser(AbstractBrowser):
    """
    A browser that lists database objects. DatabaseBrowser uses SQLAlchemy and
    its supported dialects (https://docs.sqlalchemy.org/en/13/dialects/).

    DatabaseBrowser follows this directory sturcture:

        /databases/<scheme>/instances/<instance>/schemas/<schema>/tables/<table>
        /databases/<scheme>/instances/<instance>/schemas/<schema>/views/<view>

    The scheme is derived from the DB URL (e.g.
    postgres://user:pass@localhost/my_db would have scheme="postgres"). The
    instance defaults to the hostname, but can be overridden via the `name`
    config. Schema is whatever SQLAlchemy returns from `get_schema_names`.
    Ditto for table and view.

    This model does not make the distinction between schemas and databases as
    defined in standard information_schema formats
    (https://en.wikipedia.org/wiki/Information_schema). PostgreSQL, in
    particular, is a little weird because it has both (the schema is usually
    `public`).
    """

    def __init__(
        self,
        engine: sa.engine.Engine,
    ):
        self.engine = engine

    def children(self, path: PurePosixPath) -> list[str]:
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

    def schemas(self) -> list[str]:
        """
        :returns: All schema names in a database. In PostgreSQL, this is
            usually just `public`. In MySQL and others, it's usually a list of
            all database names.
        """

        return sa.inspect(self.engine).get_schema_names()

    def tables(self, schema: str) -> list[str]:
        """
        :returns: All table names in a schema.
        """

        return self._tables_or_views(
            schema,
            sa.inspect(self.engine).get_table_names,
        )

    def views(self, schema: str) -> list[str]:
        """
        :returns: All view names in a schema.
        """
        return self._tables_or_views(
            schema,
            sa.inspect(self.engine).get_view_names,
        )

    def _tables_or_views(
        self,
        schema: str,
        get_method: Callable[[str], list[str]],
    ) -> list[str]:
        """
        Helper function that gets returns all tables or views for a given
        schema. This method exists because some DBs return `<schema>.<table>`
        and others just return `<table>`. To keep things standard, we strip out
        the `<schema>.` prefix if it exists.

        :param get_method: A SQLAlchemy inspection method; either
            (`get_table_names` or `get_schema_names`).
        :returns: All views or tables in a schema.
        """

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
        """
        :returns: A path of the format /databases/<scheme>/instances/<instance>
        """

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
    @contextmanager
    def open(**config) -> Generator['DatabaseBrowser', None, None]:
        assert 'url' in config, \
            f"No url defined for browser config={config}"
        yield DatabaseBrowser(sa.create_engine(config['url']))

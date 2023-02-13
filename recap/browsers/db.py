import logging
from contextlib import contextmanager
from functools import cached_property
from typing import Any, Callable, Generator

import sqlalchemy

from recap.url import URL

from .abstract import AbstractBrowser

log = logging.getLogger(__name__)


class DatabaseURL(URL):
    def __init__(
        self,
        url: str,
        subpath: str | None = None,
    ):
        super().__init__(url, subpath)
        self.catalog: str | None = None
        self.schema: str | None = None
        self.table: str | None = None
        parts = self.path_posix.parts if self.path_posix else []

        match (self.dialect, self.host_port, parts):
            case ("mysql", _, [*schema_table]):
                self.catalog = "def"
                if len(schema_table) > 1:
                    self.schema = schema_table[1]
                if len(schema_table) > 2:
                    self.table = schema_table[2]
            case ("bigquery", catalog, [*schema_table]):
                self.catalog = catalog
                if len(schema_table) > 1:
                    self.schema = schema_table[1]
                if len(schema_table) > 2:
                    self.table = schema_table[2]
            case (_, _, [*catalog_schema_table]):
                if len(catalog_schema_table) > 1:
                    self.catalog = catalog_schema_table[1]
                if len(catalog_schema_table) > 2:
                    self.schema = catalog_schema_table[2]
                if len(catalog_schema_table) > 3:
                    self.table = catalog_schema_table[3]
            case _:
                raise ValueError(f"Invalid url={self.url}")


class DatabaseBrowser(AbstractBrowser):
    """
    A browser that lists database objects. DatabaseBrowser uses SQLAlchemy and
    its supported dialects (https://docs.sqlalchemy.org/en/14/dialects/).

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
        engine: sqlalchemy.engine.Engine,
    ):
        """
        :param engine: SQLAlchemy engine to use when browsing the db.
        :param root_: The root CatalogPath that represents this DB instance.
        """

        self.engine = engine
        self.url = URL(str(engine.url))

    def children(
        self,
        path: str,
    ) -> list[str] | None:
        """
        :param path: Path to list.
        :returns: List of children for path, or None if path doesn't exist.
        """

        url = DatabaseURL(f"{self.url}{path}")

        match url:
            case DatabaseURL(catalog=_, schema=str(schema), table=None):
                return sorted(
                    self.tables(schema) + self.views(schema),
                    key=str.casefold,
                )
            case DatabaseURL(catalog=_, schema=None, table=None):
                return self.schemas()

    def schemas(self) -> list[str]:
        """
        :returns: All schema names in a database. In PostgreSQL, this is
            usually just `public`. In MySQL and others, it's usually a list of
            all database names.
        """

        return sqlalchemy.inspect(self.engine).get_schema_names()

    def tables(self, schema: str) -> list[str]:
        """
        :returns: All table names in a schema.
        """

        return self._tables_or_views(
            schema,
            sqlalchemy.inspect(self.engine).get_table_names,
        )

    def views(self, schema: str) -> list[str]:
        """
        :returns: All view names in a schema.
        """
        return self._tables_or_views(
            schema,
            sqlalchemy.inspect(self.engine).get_view_names,
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
                    table_or_view = table_or_view[len(schema) + 1 :]
                results.append(table_or_view)
        except Exception as e:
            # Just optimistically try, and ignore if we can't get info.
            # Easier than trying to figure out if permission exists.
            log.debug(
                "Unable to fetch tables or views for schema=%s",
                schema,
                exc_info=e,
            )
        return results


@contextmanager
def create_browser(
    url: str,
    engine: dict[str, Any] = {},
    **_,
) -> Generator[DatabaseBrowser, None, None]:
    yield DatabaseBrowser(
        engine=sqlalchemy.create_engine(url, **engine),
    )

import logging
import sqlalchemy
from .abstract import AbstractBrowser
from contextlib import contextmanager
from pydantic import Field
from recap.paths import CatalogPath, RootPath, create_catalog_path
from typing import Any, Callable, Generator, Union
from urllib.parse import urlparse


log = logging.getLogger(__name__)


class DatabaseRootPath(CatalogPath):
    scheme: str
    name_: str = Field(alias='name')
    template = '/databases/{scheme}/instances/{name}'


class SchemasPath(CatalogPath):
    template = '/schemas'


class SchemaPath(CatalogPath):
    schema_: str = Field(alias='schema')
    template = SchemasPath.template + '/{schema}'


class TablesPath(CatalogPath):
    schema_: str = Field(alias='schema')
    template = SchemaPath.template + '/tables'


class ViewsPath(CatalogPath):
    schema_: str = Field(alias='schema')
    template = SchemaPath.template + '/views'


class TablePath(CatalogPath):
    schema_: str = Field(alias='schema')
    table: str
    template = TablesPath.template + '/{table}'


class ViewPath(CatalogPath):
    schema_: str = Field(alias='schema')
    view: str
    template = ViewsPath.template + '/{view}'


DatabaseBrowserPath = Union[
    SchemasPath,
    SchemaPath,
    TablesPath,
    TablePath,
    ViewsPath,
    ViewPath,
]


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
        engine: sqlalchemy.engine.Engine,
        root_: DatabaseRootPath | None = None,
    ):
        self.engine = engine
        self.root_ = root_ or DatabaseBrowser.default_root(str(engine.url))

    def children(
        self,
        path: str,
    ) -> list[DatabaseBrowserPath] | None:
        catalog_path = create_catalog_path(
            path,
            *list(DatabaseBrowserPath.__args__), # pyright: ignore [reportGeneralTypeIssues]
        )
        match catalog_path:
            case RootPath():
                return [SchemasPath()]
            case SchemasPath():
                return [
                    SchemaPath(schema=s)
                    for s in self.schemas()
                ]
            case SchemaPath(schema_=schema):
                # TODO can we move this if to the case
                if schema in self.schemas():
                    return [
                        TablesPath(schema=schema),
                        ViewsPath(schema=schema),
                    ]
            case TablesPath(schema_=schema):
                return [
                    TablePath(schema=schema, table=t)
                    for t in self.tables(schema)
                ]
            case ViewsPath(schema_=schema):
                return [
                    ViewPath(schema=schema, view=v)
                    for v in self.views(schema)
                ]
        return None

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

    def root(self) -> DatabaseRootPath:
        return self.root_

    @staticmethod
    def default_root(url: str) -> DatabaseRootPath:
        parsed_url = urlparse(url)
        # Given `posgrestql+psycopg2://foo:bar@baz/some_db`, return `postgresql`.
        scheme = parsed_url.scheme.split('+')[0]
        # Given `posgrestql+psycopg2://foo:bar@baz/some_db`, return `baz`.
        return DatabaseRootPath(
            scheme=scheme,
            name=parsed_url.netloc.split('@')[-1],
        )


@contextmanager
def create_browser(
    url: str,
    name: str | None = None,
    engine: dict[str, Any] = {},
    **_,
) -> Generator[DatabaseBrowser, None, None]:
    default_root = DatabaseBrowser.default_root(url)
    yield DatabaseBrowser(
        engine=sqlalchemy.create_engine(url, **engine),
        root_=DatabaseRootPath(
            scheme=default_root.scheme,
            name=name or default_root.name_,
        ),
    )

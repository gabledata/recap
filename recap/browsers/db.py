import logging
import sqlalchemy
from .abstract import AbstractBrowser
from contextlib import contextmanager
from pathlib import PurePosixPath
from pydantic import Field
from recap.paths import CatalogPath, RootPath, create_catalog_path
from typing import Any, Callable, Generator, Union
from urllib.parse import urlparse


log = logging.getLogger(__name__)


class DatabasesPath(CatalogPath):
    template = '/databases'


class DatabasePath(CatalogPath):
    scheme: str
    template = '/databases/{scheme}'


class InstancesPath(CatalogPath):
    scheme: str
    template = '/databases/{scheme}/instances'


class InstancePath(CatalogPath):
    scheme: str
    instance: str
    template = '/databases/{scheme}/instances/{instance}'


class SchemasPath(CatalogPath):
    scheme: str
    instance: str
    template = '/databases/{scheme}/instances/{instance}/schemas'


class SchemaPath(CatalogPath):
    scheme: str
    instance: str
    schema_: str = Field(alias='schema')
    template = '/databases/{scheme}/instances/{instance}/schemas/{schema}'


class TablesPath(CatalogPath):
    scheme: str
    instance: str
    schema_: str = Field(alias='schema')
    template = '/databases/{scheme}/instances/{instance}/schemas/{schema}/tables'


class ViewsPath(CatalogPath):
    scheme: str
    instance: str
    schema_: str = Field(alias='schema')
    template = '/databases/{scheme}/instances/{instance}/schemas/{schema}/views'


class TablePath(CatalogPath):
    scheme: str
    instance: str
    schema_: str = Field(alias='schema')
    table: str
    template = '/databases/{scheme}/instances/{instance}/schemas/{schema}/tables/{table}'


class ViewPath(CatalogPath):
    scheme: str
    instance: str
    schema_: str = Field(alias='schema')
    view: str
    template = '/databases/{scheme}/instances/{instance}/schemas/{schema}/views/{view}'


DatabaseBrowserPath = Union[
    DatabasesPath,
    DatabasePath,
    InstancesPath,
    InstancePath,
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
        instance: 'InstancePath',
        engine: sqlalchemy.engine.Engine,
    ):
        self.instance = instance
        self.engine = engine

    def children(
        self,
        path: PurePosixPath,
    ) -> list[DatabaseBrowserPath] | None:
        instance_dict = self.instance.dict(by_alias=True)
        catalog_path = create_catalog_path(
            str(path),
            *list(DatabaseBrowserPath.__args__), # pyright: ignore [reportGeneralTypeIssues]
        )
        catalog_path_dict = catalog_path.dict(
            by_alias=True,
        ) if catalog_path else {}
        match catalog_path:
            case RootPath():
                return [DatabasesPath()]
            case DatabasesPath():
                return [DatabasePath(**instance_dict)]
            case DatabasePath(scheme=self.instance.scheme):
                return [InstancesPath(**instance_dict)]
            case InstancesPath(scheme=self.instance.scheme):
                return [self.instance]
            case InstancePath(
                scheme=self.instance.scheme,
                instance=self.instance.instance,
            ):
                return [SchemasPath(**instance_dict)]
            case SchemasPath(
                scheme=self.instance.scheme,
                instance=self.instance.instance,
            ):
                return [
                    SchemaPath(schema=s, **catalog_path_dict)
                    for s in self.schemas()
                ]
            case SchemaPath(
                scheme=self.instance.scheme,
                instance=self.instance.instance,
                schema_=schema
            ):
                # TODO can we move this if to the case
                if schema in self.schemas():
                    return [
                        TablesPath(**catalog_path_dict),
                        ViewsPath(**catalog_path_dict),
                    ]
            case TablesPath(
                scheme=self.instance.scheme,
                instance=self.instance.instance,
                schema_=schema,
            ):
                return [
                    TablePath(table=t, **catalog_path_dict)
                    for t in self.tables(schema)
                ]
            case ViewsPath(
                scheme=self.instance.scheme,
                instance=self.instance.instance,
                schema_=schema,
            ):
                return [
                    ViewPath(view=v, **catalog_path_dict)
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


@contextmanager
def create_browser(
    url: str,
    name: str | None = None,
    engine: dict[str, Any] = {},
    **_,
) -> Generator[DatabaseBrowser, None, None]:
    parsed_url = urlparse(url)
    # Given `posgrestql+psycopg2://foo:bar@baz/some_db`, return `postgresql`.
    scheme = parsed_url.scheme.split('+')[0]
    # Given `posgrestql+psycopg2://foo:bar@baz/some_db`, return `baz`.
    default_instance = parsed_url.netloc.split('@')[-1]
    instance = name or default_instance
    instance = InstancePath(scheme=scheme, instance=instance)
    yield DatabaseBrowser(
        instance=instance,
        engine=sqlalchemy.create_engine(url, **engine),
    )

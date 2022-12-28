import fnmatch
import logging
from recap.crawlers.abstract import AbstractCrawler
from recap.crawlers.db.analyzers import AbstractTableAnalyzer
from .browser import DatabaseBrowser
from pathlib import PurePosixPath
from recap.catalog.abstract import AbstractCatalog
from typing import Any, List


log = logging.getLogger(__name__)


class DatabaseCrawler(AbstractCrawler):
    def __init__(
        self,
        infra: str,
        instance: str,
        catalog: AbstractCatalog,
        browser: DatabaseBrowser,
        analyzers: List[AbstractTableAnalyzer],
        filters: List[str]= []
    ):
        self.infra = infra
        self.instance = instance
        self.catalog = catalog
        self.browser = browser
        self.analyzers = analyzers
        self.filters = filters

    def crawl(self):
        log.info('Beginning crawl for %s://%s', self.infra, self.instance)
        self.catalog.touch(PurePosixPath(
            'databases', self.infra,
            'instances', self.instance,
        ))
        schemas = self.browser.schemas()
        for schema in schemas:
            log.debug('Found schema=%s', schema)
            if self._should_crawl(schema):
                self.catalog.touch(PurePosixPath(
                    'databases', self.infra,
                    'instances', self.instance,
                    'schemas', schema,
                ))
            # Continue crawling schema even if _should_crawl returns False for
            # schema. Have to do this because filter='schema1.tables_*' won't
            # fnmatch 'schema1', but we stil need to crawl 'schema1' to get to
            # 'tables_*'.
            views = self.browser.views(schema)
            tables = self.browser.tables(schema)
            for view in views:
                log.debug('Found view=%s.%s', schema, view)
                if self._should_crawl(schema, view):
                    self._write_table_or_view(
                        schema,
                        view=view
                    )
            for table in tables:
                log.debug('Found table=%s.%s', schema, table)
                if self._should_crawl(schema, table):
                    self._write_table_or_view(
                        schema,
                        table=table
                    )
            self._remove_deleted_tables(schema, tables)
            self._remove_deleted_views(schema, views)
        self._remove_deleted_schemas(schemas)
        log.info('Finished crawl for %s://%s', self.infra, self.instance)

    def _should_crawl(
        self,
        schema: str,
        table_or_view: str | None = None,
    ) -> bool:
        name = f"{schema}.{table_or_view}" if table_or_view else schema
        for filter in self.filters:
            if fnmatch.fnmatch(name, filter):
                return True
        return False if self.filters else True

    # TODO Combine methods using a util that is agnostic to data being removed
    def _remove_deleted_schemas(self, schemas: List[str]):
        catalog_schemas = self.catalog.ls(PurePosixPath(
            'databases', self.infra,
            'instances', self.instance,
            'schemas'
        )) or []
        # Find schemas that are not currently in instance
        schemas_to_remove = [s for s in catalog_schemas if s not in schemas]
        for schema in schemas_to_remove:
            log.info('Removing deleted schema %s', schema)
            self.catalog.rm(PurePosixPath(
                'databases', self.infra,
                'instances', self.instance,
                'schemas', schema,
            ))

    def _remove_deleted_tables(self, schema: str, tables: List[str]):
        catalog_tables = self.catalog.ls(PurePosixPath(
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
            'tables'
        )) or []
        # Find schemas that are not currently in instance
        tables_to_remove = [t for t in catalog_tables if t not in tables]
        for table in tables_to_remove:
            log.info('Removing deleted table %s.%s', schema, table)
            self.catalog.rm(PurePosixPath(
                'databases', self.infra,
                'instances', self.instance,
                'schemas', schema,
                'tables', table,
            ))

    def _remove_deleted_views(self, schema: str, views: List[str]):
        catalog_views = self.catalog.ls(PurePosixPath(
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
            'views'
        )) or []
        # Find schemas that are not currently in instance
        views_to_remove = [v for v in catalog_views if v not in views]
        for view in views_to_remove:
            log.info('Removing deleted view %s.%s', schema, view)
            self.catalog.rm(PurePosixPath(
                'databases', self.infra,
                'instances', self.instance,
                'schemas', schema,
                'views', view,
            ))

    def  _write_table_or_view(
        self,
        schema: str,
        table: str | None = None,
        view: str | None = None
    ):
        path = PurePosixPath(
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
        )

        if table:
            path = PurePosixPath(path, 'tables', table)
        elif view:
            path = PurePosixPath(path, 'views', view)
        else:
            raise ValueError("Must set table or view when writing metadata.")

        location = self._location(
            schema,
            table=table,
            view=view,
        )

        table_or_view = table or view
        analysis_dicts = self._analyze_table_or_view(schema, table_or_view) | { # pyright: ignore [reportGeneralTypeIssues]
            # TODO This seems kind of hacky. Maybe it should be an analyzer?
            'location': location,
        }

        # TODO Should have AbstractCatalog.write allow for multiple type dicts
        for type, metadata in analysis_dicts.items():
            log.debug(
                'Writing metadata path=%s type=%s',
                path,
                type,
            )
            self.catalog.write(path, type, metadata)

    def _location(
        self,
        schema: str,
        table: str | None = None,
        view: str | None = None
    ) -> dict[str, str]:
        assert table or view, \
            "Must specify either 'table' or 'view' for a location dictionary"
        location = {
            'database': self.infra,
            'instance': self.instance,
            'schema': schema,
        }
        if table:
            location['table'] = table
        elif view:
            location['view'] = view
        return location

    def _analyze_table_or_view(
        self,
        schema: str,
        table_or_view: str
    ) -> dict[str, Any]:
        results = {}
        for analyzer in self.analyzers:
            analyzer_type = type(analyzer)
            if issubclass(analyzer_type, AbstractTableAnalyzer):
                log.debug(
                    'Analyzing %s.%s with %s',
                    schema,
                    table_or_view,
                    analyzer_type.__name__,
                )
                results |= analyzer.analyze(schema, table_or_view)
        return results

import fnmatch
import logging
from contextlib import contextmanager
from pathlib import PurePosixPath
from recap.browsers.analyzing import AnalyzingBrowser, create_browser
from recap.catalogs.abstract import AbstractCatalog
from recap.paths import CatalogPath, RootPath
from typing import Any, Generator


log = logging.getLogger(__name__)


class Crawler:
    """
    Crawler does three things:

    1. Browses the configured infrastructure
    2. Analyzes the infrastructure's data to generate metadata
    3. Stores the metadata in a catalog
    """

    def __init__(
        self,
        browser: AnalyzingBrowser,
        catalog: AbstractCatalog,
        recursive: bool = True,
        filters: list[str] = [],
    ):
        """
        :param browser: AnalyzingBrowser to use for listing children and
            analyzing metadata.
        :param catalog: The catalog where the crawler will create directories
            and store metadata.
        :param recursive: Whether the crawler should recurse into
            subdirectories when crawling.
        :param filters: Path filter to include only certain paths. Recap uses
            Unix filename pattern matching as defined in Python's fnmatch
            module. Filtered paths are relative to the browser (excluding the
            browser's root).
        """

        self.browser = browser
        self.catalog = catalog
        self.recursive = recursive
        self.filters = filters
        self.exploded_filters = self._explode_filters(filters)

    def crawl(self):
        log.info('Beginning crawl root=%s', self.browser.root())
        path_stack: list[CatalogPath] = [RootPath()]

        while len(path_stack) > 0:
            relative_path = str(path_stack.pop())
            full_path_posix = PurePosixPath(
                str(self.browser.root()),
                relative_path[1:],
            )

            log.info("Crawling path=%s", relative_path)

            # 1. Read and save metadata for path if filters match.
            if (
                self._matches(relative_path, self.filters)
                and (metadata := self.browser.analyze(relative_path))
            ):
                self._write_metadata(full_path_posix, metadata)

            # 2. Add children (that match filter) to path_stack.
            children = self.browser.children(relative_path) or []
            filtered_children = filter(
                lambda p: self._matches(str(p), self.exploded_filters),
                children,
            )
            if self.recursive:
                path_stack.extend(filtered_children)

            # 3. Remove deleted children from catalog.
            self._remove_deleted(full_path_posix, children)

        log.info('Finished crawl root=%s', self.browser.root())

    def _matches(
        self,
        relative_path: str,
        filters: list[str],
    ) -> bool:
        """
        Check if a path matches any filters.

        :returns: True if path matches a filter or if filters is empty.
        """

        for filter in filters:
            if fnmatch.fnmatch(relative_path, filter):
                return True
        return False if filters else True

    def _write_metadata(
        self,
        full_path_posix: PurePosixPath,
        metadata: dict[str, Any],
    ):
        """
        Write a metadata dictionary to a path in the catalog.
        """

        log.debug(
            'Writing metadata path=%s metadata=%s',
            full_path_posix,
            metadata,
        )
        self.catalog.write(str(full_path_posix), metadata, True)

    def _remove_deleted(
        self,
        full_path_posix: PurePosixPath,
        instance_children: list[CatalogPath],
    ):
        """
        Compares the path's children in the browser vs. what is currently in
        the catalog. Deletes all children that appear in the catalog, but no
        longer appear in the browser. This behavior removes children that used
        to exist in data infrastructure, but have been deleted since the last
        crawl.
        """

        catalog_children = self.catalog.ls(str(full_path_posix)) or []
        instance_children_names = [c.name() for c in instance_children]
        # Find catalog children that are not in the browser's children.
        deleted_children = [
            catalog_child
            for catalog_child in catalog_children
            if catalog_child not in instance_children_names
        ]
        for child in deleted_children:
            path_to_remove = str(PurePosixPath(full_path_posix, child))
            log.debug('Removing deleted path from catalog: %s', path_to_remove)
            self.catalog.rm(path_to_remove)

    def _explode_filters(self, filters: list[str]) -> list[str]:
        """
        Returns a list of paths that bread-crumb from the filter all the way
        back to root. For example:

            filters=[
                '/**/schemas/my_db/tables/foo*'
            ]
            returns=[
                '/**',
                '/**/schemas',
                '/**/schemas/my_db',
                '/**/schemas/my_db/tables',
                '/**/schemas/my_db/tables/foo*',
            ]

        We need to do this so that parents match the filter and crawling
        reaches the wild-carded children.
        """

        exploded_filters = []
        for filter in filters:
            fragments = filter.split('/')
            partial_path = PurePosixPath('/')
            for fragment in fragments:
                partial_path = PurePosixPath(partial_path, fragment)
                exploded_filters.append(str(partial_path))
        return exploded_filters

@contextmanager
def create_crawler(
    url: str,
    catalog: AbstractCatalog,
    **config,
) -> Generator['Crawler', None, None]:
    with create_browser(url=url, **config) as browser:
        yield Crawler(browser, catalog, **config)

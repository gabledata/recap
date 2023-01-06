import fnmatch
import logging
from .plugins import load_analyzer_plugins, load_browser_plugins
from contextlib import contextmanager, ExitStack
from pathlib import PurePosixPath
from recap.plugins.analyzers.abstract import AbstractAnalyzer
from recap.plugins.browsers.abstract import AbstractBrowser
from recap.plugins.catalogs.abstract import AbstractCatalog
from typing import Any, Generator, List


log = logging.getLogger(__name__)


class Crawler:
    """
    Crawler does three things:

    1. Browses the configured infrastructure
    2. Analyzes the infrastructure's data to generate metadata
    3. Stores the metadata in a catalog

    Recap's crawler is very simple right now. The crawler recursively browses
    and analyzes all children starting from an infrastructre's root location.
    """

    def __init__(
        self,
        root: PurePosixPath,
        browser: AbstractBrowser,
        catalog: AbstractCatalog,
        analyzers: List[AbstractAnalyzer],
        filters: List[str] = [],
    ):
        """
        :param root: Root path to use when storing data in the catalog.
        :param browser: The browser to use when listing a path's children.
        :param catalog: The catalog to create directories and store metadata.
        :param analyzers: Analyzers to inspect each path for metadata.
        :param filters: Path filter to include only certain paths. Recap uses
            Unix filename pattern matching as defined in Python's fnmatch
            module. The path that's filtered doesn't include the root.
        """

        self.root = root
        self.browser = browser
        self.catalog = catalog
        self.analyzers = analyzers
        self.filters = filters
        self.exploded_filters = self._explode_filters(filters)

    def crawl(self):
        log.info('Beginning crawl root=%s', self.root)
        self.catalog.touch(self.root)
        # Start crawling from the root ('/')
        path_stack: List[PurePosixPath] = [PurePosixPath('/')]

        while len(path_stack) > 0:
            path = path_stack.pop()
            log.info("Crawling path=%s", path)

            # 1. Read and save metadata for path if filters match.
            if self._matches(path, self.filters):
                metadata = self._get_metadata(path)
                self._write_metadata(path, metadata)

            # 2. Add children (that match filter) to path_stack.
            children = self.browser.children(path)
            children_paths = map(
                lambda c: PurePosixPath(path, c),
                children,
            )
            children_paths = filter(
                lambda p: self._matches(p, self.exploded_filters),
                children_paths,
            )
            path_stack.extend(children_paths)

            # 3. Remove deleted children from catalog.
            self._remove_deleted(path, children)

        log.info('Finished crawl root=%s', self.root)

    def _matches(
        self,
        path: PurePosixPath,
        filters: List[str],
    ) -> bool:
        """
        Check if a path matches any filters.

        :returns: True if path matches a filter or if filters is empty.
        """

        for filter in filters:
            if fnmatch.fnmatch(str(path), filter):
                return True
        return False if filters else True

    def _get_metadata(
        self,
        path: PurePosixPath,
    ) -> dict[str, Any]:
        """
        Run all analyzers on a path.

        :returns: A dictionary of all metadata returned from all analyzers.
        """

        results = {}
        for analyzer in self.analyzers:
            log.debug(
                'Analyzing path=%s analyzer=%s',
                path,
                type(analyzer).__name__,
            )
            results |= analyzer.analyze(path)
        return results

    def _write_metadata(
        self,
        path: PurePosixPath,
        metadata: dict[str, Any],
    ):
        """
        Write a metadata dictionary to a path in the catalog.
        """

        full_path = PurePosixPath(self.root, str(path)[1:])

        # TODO Should have AbstractCatalog.write allow for multiple type dicts
        for type, metadata in metadata.items():
            log.debug(
                'Writing metadata path=%s type=%s',
                full_path,
                type,
            )
            self.catalog.write(full_path, type, metadata)

    def _remove_deleted(
        self,
        path: PurePosixPath,
        instance_children: List[str],
    ):
        """
        Compares the path's children in the browser vs. what is currently in
        the catalog. Deletes all children that appear in the catalog, but no
        longer appear in the browser. This behavior removes children that used
        to exist in data infrastructure, but have been deleted since the last
        crawl.
        """

        full_path = PurePosixPath(self.root, str(path)[1:])
        catalog_children = self.catalog.ls(full_path) or []
        # Find catalog children that are not in the browser's children.
        deleted_children = [s for s in catalog_children if s not in instance_children]
        for child in deleted_children:
            path_to_remove = PurePosixPath(full_path, child)
            log.debug('Removing deleted path from catalog: %s', path_to_remove)
            self.catalog.rm(path_to_remove)

    def _explode_filters(self, filters: List[str]) -> List[str]:
        """
        Returns a list of paths that bread-crumb from the filter all the way
        back to root. For example:

            filters=[
                '/schemas/my_db/tables/foo*'
            ]
            returns=[
                '/schemas',
                '/schemas/my_db',
                '/schemas/my_db/tables',
                '/schemas/my_db/tables/foo*',
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
                exploded_filters.extend(str(partial_path))
        return exploded_filters

    @staticmethod
    @contextmanager
    def open(
        catalog: AbstractCatalog,
        **config,
    ) -> Generator['Crawler', None, None]:
        analyzer_plugins = load_analyzer_plugins()
        browser_plugins = load_browser_plugins()
        url = config.get('url')
        excludes = config.get('excludes', [])
        filters = config.get('filters', [])

        assert url, \
            f"No url defined for instance config={config}"

        analyzers = []
        browser = None

        with ExitStack() as stack:
            for analyzer_name, analyzer_cls in analyzer_plugins.items():
                if (
                    analyzer_cls.analyzable(url)
                    and analyzer_name not in excludes
                ):
                    analyzer_context_manager = analyzer_cls.open(**config)
                    analyzer = stack.enter_context(analyzer_context_manager)
                    analyzers.append(analyzer)

            for browser_cls in browser_plugins.values():
                if browser_cls.browsable(url):
                    browser_context_manager = browser_cls.open(**config)
                    browser = stack.enter_context(browser_context_manager)

            assert analyzers, f"Found no analyzers for url={url}"
            assert browser, f"Found no browser for url={url}"

            root = browser.root(**config)

            yield Crawler(
                root,
                browser,
                catalog,
                analyzers,
                filters,
            )

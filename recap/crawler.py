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
    def __init__(
        self,
        root: PurePosixPath,
        browser: AbstractBrowser,
        catalog: AbstractCatalog,
        analyzers: List[AbstractAnalyzer],
        filters: List[str] = [],
    ):
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
        for filter in filters:
            if fnmatch.fnmatch(str(path), filter):
                return True
        return False if filters else True

    def _get_metadata(
        self,
        path: PurePosixPath,
    ) -> dict[str, Any]:
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
        full_path = PurePosixPath(self.root, str(path)[1:])
        catalog_children = self.catalog.ls(full_path) or []
        # Find catalog children that are not in the browser's children.
        deleted_children = [s for s in catalog_children if s not in instance_children]
        for child in deleted_children:
            path_to_remove = PurePosixPath(full_path, child)
            log.debug('Removing deleted path from catalog: %s', path_to_remove)
            self.catalog.rm(path_to_remove)

    """
    If filters=[
        '/schemas/my_db/tables/foo*'
    ]
    Returns [
        '/schemas',
        '/schemas/my_db',
        '/schemas/my_db/tables',
        '/schemas/my_db/tables/foo*',
    ]

    We need to do this so that parents match the filter and crawling reaches
    the wild-carded children.
    """
    def _explode_filters(self, filters: List[str]) -> List[str]:
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

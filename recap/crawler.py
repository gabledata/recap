import fnmatch
import logging
from .plugins import load_analyzer_plugins, load_browser_plugins
from contextlib import contextmanager, ExitStack
from pathlib import PurePosixPath
from recap.analyzers import create_analyzer
from recap.analyzers.abstract import AbstractAnalyzer
from recap.browsers import create_browser
from recap.browsers.abstract import AbstractBrowser
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

    Recap's crawler is very simple right now. The crawler recursively browses
    and analyzes all children starting from an infrastructre's root location.
    """

    def __init__(
        self,
        browser: AbstractBrowser,
        catalog: AbstractCatalog,
        analyzers: list[AbstractAnalyzer],
        filters: list[str] = [],
    ):
        """
        :param browser: The browser to use when listing a path's children.
        :param catalog: The catalog to create directories and store metadata.
        :param analyzers: Analyzers to inspect each path for metadata.
        :param filters: Path filter to include only certain paths. Recap uses
            Unix filename pattern matching as defined in Python's fnmatch
            module. The path that's filtered doesn't include the root.
        """

        self.browser = browser
        self.catalog = catalog
        self.analyzers = analyzers
        self.filters = filters
        self.exploded_filters = self._explode_filters(filters)

    def crawl(self):
        log.info('Beginning crawl root=%s', self.browser.root())
        path_stack: list[CatalogPath] = [RootPath()]

        while len(path_stack) > 0:
            relative_path = path_stack.pop()
            full_path_posix = PurePosixPath(
                str(self.browser.root()),
                str(relative_path)[1:],
            )

            log.info("Crawling path=%s", relative_path)

            # 1. Read and save metadata for path if filters match.
            if self._matches(relative_path, self.filters):
                metadata = self._get_metadata(relative_path)
                self._write_metadata(full_path_posix, metadata)

            # 2. Add children (that match filter) to path_stack.
            children = self.browser.children(str(relative_path)) or []
            filtered_children = filter(
                lambda p: self._matches(p, self.exploded_filters),
                children,
            )
            path_stack.extend(filtered_children)

            # 3. Remove deleted children from catalog.
            self._remove_deleted(full_path_posix, children)

        log.info('Finished crawl root=%s', self.browser.root())

    def _matches(
        self,
        relative_path: CatalogPath,
        filters: list[str],
    ) -> bool:
        """
        Check if a path matches any filters.

        :returns: True if path matches a filter or if filters is empty.
        """

        for filter in filters:
            if fnmatch.fnmatch(str(relative_path), filter):
                return True
        return False if filters else True

    def _get_metadata(
        self,
        path: CatalogPath,
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
                analyzer.__class__.__name__,
            )
            try: # EAFP
                if metadata := analyzer.analyze(path):
                    metadata_dict = metadata.dict(
                        by_alias=True,
                        exclude_none=True,
                        exclude_unset=True,
                        exclude_defaults=True,
                    )
                    # Have to unpack __root__ if it exists, sigh.
                    # https://github.com/pydantic/pydantic/issues/1193
                    metadata_dict = metadata_dict.get(
                        '__root__',
                        metadata_dict
                    )
                    results |= {metadata.key(): metadata_dict}
            except Exception as e:
                log.debug(
                    'Unable to process path with analyzer path=%s analyzer=%s',
                    path,
                    analyzer.__class__.__name__,
                    exc_info=e,
                )
        return results

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
                exploded_filters.extend(str(partial_path))
        return exploded_filters

@contextmanager
def create_crawler(
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
        for browser_name in browser_plugins.keys():
            try:
                browser_context_manager = create_browser(
                    plugin=browser_name,
                    **config,
                )
                browser = stack.enter_context(browser_context_manager)

                # If we got this far, we found a browser. Stop looking.
                break
            except Exception as e:
                    log.debug(
                        'Skipped browser for url=%s name=%s',
                        url,
                        browser_name,
                        exc_info=e,
                    )

        assert browser, f"Found no browser for url={url}"

        for analyzer_name in analyzer_plugins.keys():
            if (analyzer_name not in excludes):
                try:
                    analyzer_context_manager = create_analyzer(
                        plugin=analyzer_name,
                        **config,
                    )
                    analyzer = stack.enter_context(analyzer_context_manager)
                    analyzers.append(analyzer)
                except Exception as e:
                    log.debug(
                        'Skipped analyzer for url=%s name=%s',
                        url,
                        analyzer_name,
                        exc_info=e,
                    )

        assert analyzers, f"Found no analyzers for url={url}"

        yield Crawler(
            browser,
            catalog,
            analyzers,
            filters,
        )

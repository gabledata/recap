import logging
from contextlib import ExitStack, contextmanager
from typing import Any, Generator

from recap.analyzers import create_analyzer
from recap.analyzers.abstract import AbstractAnalyzer
from recap.browsers import create_browser as create_wrapped_browser
from recap.browsers.abstract import AbstractBrowser
from recap.plugins import load_analyzer_plugins, load_browser_plugins

log = logging.getLogger(__name__)


class AnalyzingBrowser(AbstractBrowser):
    """
    A special browser that ties an AbstractBrowser together with its compatible
    AbstractAnalyzers. Browsers and analyzers are deliberately unaware of one
    another, but something has to tie them together. That's this class.

    AnalyzingBrowser takes in a browser and its compatible analyzers. It
    provides all of the same functionality as the wrapped browser, but it also
    exposes an `analyze()` method, which runs all analyzers on a path and
    returns the analyzed metadata.

    This browser is deliberately not added to Recap's browser plugin
    entrypoint. It's more of a utility class for the CLI and crawler.
    """

    def __init__(
        self,
        browser: AbstractBrowser,
        analyzers: list[AbstractAnalyzer],
    ):
        self.browser = browser
        self.analyzers = analyzers
        self.url = browser.url

    def children(self, path: str) -> list[str] | None:
        return self.browser.children(path)

    def analyze(self, path: str) -> dict[str, Any] | None:
        results = {}
        for analyzer in self.analyzers:
            log.debug(
                "Analyzing path=%s analyzer=%s",
                path,
                analyzer.__class__.__name__,
            )
            try:  # EAFP
                if metadata := analyzer.analyze(path):
                    metadata_dict = metadata.dict(
                        by_alias=True,
                        exclude_none=True,
                        exclude_unset=True,
                        exclude_defaults=True,
                    )
                    # Have to unpack __root__ if it exists, sigh.
                    # https://github.com/pydantic/pydantic/issues/1193
                    metadata_obj = metadata_dict.get("__root__", metadata_dict)
                    results |= {metadata.key(): metadata_obj}
            except Exception as e:
                log.debug(
                    "Unable to process path with analyzer path=%s analyzer=%s",
                    path,
                    analyzer.__class__.__name__,
                    exc_info=e,
                )
        return results or None


@contextmanager
def create_browser(**config) -> Generator["AnalyzingBrowser", None, None]:
    """
    Create an AnalyzingBrowser that wraps an AbstractBrowser and
    AbstractAnalyzers that are compatible with the browser's child
    CatalogPaths.

    Note that the `plugin` config is currently ignored. This method simply
    searches for the first AbstractBrowser that doesn't throw an exception when
    its `create_browser` method is called.

    :param config: A **kwargs config for the browser to wrap.
    """

    analyzer_plugins = load_analyzer_plugins()
    browser_plugins = load_browser_plugins()

    with ExitStack() as stack:
        url = config.get("url")
        excludes = config.get("excludes", [])
        browser = None
        analyzers = []

        # Find a real AbstractBrowser to wrap
        for browser_name in browser_plugins.keys():
            try:
                browser_context_manager = create_wrapped_browser(
                    plugin=browser_name,
                    **config,
                )
                browser = stack.enter_context(browser_context_manager)

                # If we got this far, we found a browser. Stop looking.
                break
            except Exception as e:
                log.debug(
                    "Skipped browser for url=%s name=%s",
                    url,
                    browser_name,
                    exc_info=e,
                )

        assert browser, f"Found no browser for url={url}"

        # Find analyzers compatible with the real AbstractBrowser's config.
        for analyzer_name in analyzer_plugins.keys():
            if analyzer_name not in excludes:
                try:
                    analyzer_context_manager = create_analyzer(
                        plugin=analyzer_name,
                        **config,
                    )
                    analyzer = stack.enter_context(
                        analyzer_context_manager,
                    )
                    analyzers.append(analyzer)
                except Exception as e:
                    log.debug(
                        "Skipped analyzer for url=%s name=%s",
                        url,
                        analyzer_name,
                        exc_info=e,
                    )

        if not analyzers:
            log.warn(
                "Found no metadata analyzers for url=%s. "
                "Crawling will only create directories.",
                url,
            )

        yield AnalyzingBrowser(browser, analyzers)

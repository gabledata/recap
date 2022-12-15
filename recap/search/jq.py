import fsspec
import pyjq
from .abstract import AbstractSearch, AbstractIndexer
from contextlib import contextmanager
from pathlib import PurePosixPath
from recap.storage.fs import FilesystemStorage
from typing import List, Any, Generator
from urllib.parse import urlparse


class JqSearch(AbstractSearch):
    def __init__(
        self,
        path: PurePosixPath,
        fs: fsspec.AbstractFileSystem,
    ):
        self.storage = FilesystemStorage(path, fs)

    def search(self, query: str) -> List[dict[str, Any]]:
        results = []
        path_stack = [PurePosixPath('/')]

        while path_stack:
            path = path_stack.pop()
            doc = self.storage.read(PurePosixPath(path)) or {}

            # If the doc matches the query, add it to the results
            if pyjq.first(query, doc):
                results.append(doc)

            # Now add any children to the stack for processing
            children = self.storage.ls(path) or []
            children = map(lambda c: PurePosixPath(path, c), children)
            path_stack.extend(children)

        return results


class JqIndexer(AbstractIndexer):
    def written(
        self,
        path: PurePosixPath,
        type: str,
        metadata: Any,
    ):
        pass

    def removed(
        self,
        path: PurePosixPath,
        type: str | None = None,
    ):
        pass


@contextmanager
def open_search(**config) -> Generator[JqSearch, None, None]:
    url = urlparse(config['url'])
    fs_options = config.get('fs', {})
    fs = fsspec.filesystem(
        url.scheme,
        **fs_options,
        # TODO This should move to the filesystem storage config
        auto_mkdir=True)
    yield JqSearch(
        PurePosixPath(url.path),
        fs,
    )


@contextmanager
def open_indexer(**config) -> Generator[JqIndexer, None, None]:
    yield JqIndexer()

import httpx
from .abstract import AbstractSearch, AbstractIndexer
from contextlib import contextmanager
from pathlib import PurePosixPath
from typing import Any, List, Generator


class RecapSearch(AbstractSearch):
    def __init__(
        self,
        client: httpx.Client,
    ):
        self.client = client

    def search(
        self,
        query: str,
    ) -> List[dict[str, Any]]:
        return self.client.get('/search', params={'query': query}).json()


class RecapIndexer(AbstractIndexer):
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
def open_search(**config) -> Generator[RecapSearch, None, None]:
    with httpx.Client(base_url=config['url']) as client:
        yield RecapSearch(client)


@contextmanager
def open_indexer(**config) -> Generator[RecapIndexer, None, None]:
    yield RecapIndexer()

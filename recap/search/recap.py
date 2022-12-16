import httpx
from .abstract import AbstractSearchIndex
from contextlib import contextmanager
from pathlib import PurePosixPath
from typing import Any, List, Generator


class RecapSearchIndex(AbstractSearchIndex):
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
def open(**config) -> Generator[RecapSearchIndex, None, None]:
    with httpx.Client(base_url=config['url']) as client:
        yield RecapSearchIndex(client)

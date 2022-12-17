import httpx
from .abstract import AbstractCatalog
from contextlib import contextmanager
from pathlib import PurePosixPath
from recap.api import DEFAULT_URL
from typing import Any, List, Generator


class RecapCatalog(AbstractCatalog):
    def __init__(
        self,
        client: httpx.Client,
    ):
        self.client = client

    def touch(
        self,
        path: PurePosixPath,
    ):
        self.client.put(str(path))

    def write(
        self,
        path: PurePosixPath,
        type: str,
        metadata: Any,
    ):
        params = {'type': type} if type else None
        self.client.put(str(path), params=params, json=metadata)

    def rm(
        self,
        path: PurePosixPath,
        type: str | None = None,
    ):
        params = {'type': type} if type else None
        self.client.delete(str(path), params=params)

    def ls(
        self,
        path: PurePosixPath,
    ) -> List[str] | None:
        return self.client.get(str(path)).json()

    def read(
        self,
        path: PurePosixPath,
    ) -> dict[str, Any] | None:
        return self.client.get(str(path), params={'read': True}).json()

    def search(
        self,
        query: str,
    ) -> List[dict[str, Any]]:
        return self.client.get('/search', params={'query': query}).json()


@contextmanager
def open(**config) -> Generator[RecapCatalog, None, None]:
    url = config.get('url', DEFAULT_URL)
    with httpx.Client(base_url=url) as client:
        yield RecapCatalog(client)

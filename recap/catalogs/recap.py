import httpx
from .abstract import AbstractCatalog
from contextlib import contextmanager
from datetime import datetime
from pathlib import PurePosixPath
from recap.commands.serve import DEFAULT_URL
from typing import Any, List, Generator


class RecapCatalog(AbstractCatalog):
    """
    RecapCatalog makes HTTP requests to a Recap server and returns the results.
    """

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
        as_of: datetime | None = None,
    ) -> List[str] | None:
        params: dict[str, Any] = {}
        if as_of:
            params['as_of'] = as_of.isoformat()
        return self.client.get(str(path), params=params).json()

    def read(
        self,
        path: PurePosixPath,
        as_of: datetime | None = None,
    ) -> dict[str, Any] | None:
        params: dict[str, Any] = {'read': True}
        if as_of:
            params['as_of'] = as_of.isoformat()
        return self.client.get(str(path), params=params).json()

    def search(
        self,
        query: str,
        as_of: datetime | None = None,
    ) -> List[dict[str, Any]]:
        params: dict[str, Any] = {'query': query}
        if as_of:
            params['as_of'] = as_of.isoformat()
        return self.client.get('/search', params=params).json()

    @staticmethod
    @contextmanager
    def open(**config) -> Generator['RecapCatalog', None, None]:
        url = config.get('url', DEFAULT_URL)
        with httpx.Client(base_url=url) as client:
            yield RecapCatalog(client)

import httpx
from .abstract import AbstractCatalog
from contextlib import contextmanager
from datetime import datetime
from pathlib import PurePosixPath
from recap.server import DEFAULT_URL
from typing import Any, Generator



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
        response = self.client.patch(
            f"/metadata{path}",
            params=params,
            json=metadata
        )
        response.raise_for_status()

    def rm(
        self,
        path: PurePosixPath,
        type: str | None = None,
    ):
        if type:
            params = {'type': type}
            self.client.delete(f"/metadata{path}", params=params)
        else:
            self.client.delete(f"/directory{path}")

    def ls(
        self,
        path: PurePosixPath,
        as_of: datetime | None = None,
    ) -> list[str] | None:
        params: dict[str, Any] = {}
        if as_of:
            params['as_of'] = as_of.isoformat()
        response = self.client.get(f"/directory{path}", params=params)
        if response.status_code == httpx.codes.OK:
            return response.json()
        if response.status_code == httpx.codes.NOT_FOUND:
            return None
        response.raise_for_status()

    def read(
        self,
        path: PurePosixPath,
        as_of: datetime | None = None,
    ) -> dict[str, Any] | None:
        params: dict[str, Any] = {}
        if as_of:
            params['as_of'] = as_of.isoformat()
        response = self.client.get(f"/metadata{path}", params=params)
        if response.status_code == httpx.codes.OK:
            return response.json()
        if response.status_code == httpx.codes.NOT_FOUND:
            return None
        response.raise_for_status()

    def search(
        self,
        query: str,
        as_of: datetime | None = None,
    ) -> list[dict[str, Any]]:
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

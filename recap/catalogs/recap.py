import httpx
from .abstract import AbstractCatalog
from contextlib import contextmanager
from datetime import datetime
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
        path: str,
    ):
        self.client.put(path)

    def write(
        self,
        path: str,
        type: str,
        metadata: Any,
    ):
        response = self.client.patch(
            f"/metadata{path}",
            json={type: metadata}
        )
        response.raise_for_status()

    def rm(
        self,
        path: str,
        type: str | None = None,
    ):
        if type:
            params = {'type': type}
            self.client.delete(f"/metadata{path}", params=params)
        else:
            self.client.delete(f"/directory{path}")

    def ls(
        self,
        path: str,
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
        path: str,
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

@contextmanager
def create_catalog(
    url: str | None = None,
    **_,
) -> Generator['RecapCatalog', None, None]:
    with httpx.Client(base_url=url or DEFAULT_URL) as client:
        yield RecapCatalog(client)

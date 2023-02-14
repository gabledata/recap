from contextlib import contextmanager
from datetime import datetime
from typing import Generator

import httpx

from recap.metadata import Metadata, MetadataSubtype
from recap.server import DEFAULT_URL
from recap.url import URL

from .abstract import AbstractCatalog


class RecapCatalog(AbstractCatalog):
    """
    The Recap catalog makes HTTP requests to Recap's REST API. You can enable
    RecapCatalog in your settings.toml with:

    ```toml
    [catalog]
    plugin = "recap"
    url = "http://localhost:8000"
    ```

    The Recap catalog enables different systems to share the same metadata
    when they all talk to the same Recap server.
    """

    def __init__(
        self,
        client: httpx.Client,
    ):
        self.client = client

    def add(
        self,
        url: str,
        metadata: Metadata | None = None,
    ):
        encoded_url = URL(url).safe.encoded
        if metadata:
            response = self.client.put(
                f"/catalog/{metadata.key()}/{encoded_url}",
                json=metadata.to_dict(),
            )
        else:
            response = self.client.put(f"/catalog/urls/{encoded_url}")
        response.raise_for_status()

    def read(
        self,
        url: str,
        type: type[MetadataSubtype],
        id: str | None = None,
        time: datetime | None = None,
    ) -> MetadataSubtype | None:
        encoded_url = URL(url).safe.encoded
        params = {}
        if time:
            params["time"] = time.isoformat()
        if id:
            params["id"] = id
        response = self.client.get(
            f"/catalog/{type.key()}/{encoded_url}", params=params
        )
        if response.status_code == httpx.codes.OK:
            return type.from_dict(response.json())
        if response.status_code == httpx.codes.NOT_FOUND:
            return None
        response.raise_for_status()

    def children(
        self,
        url: str,
        time: datetime | None = None,
    ) -> list[str] | None:
        encoded_url = URL(url).safe.encoded
        params = {}
        if time:
            params["time"] = time.isoformat()
        response = self.client.get(f"/catalog/urls/{encoded_url}", params=params)
        if response.status_code == httpx.codes.OK:
            return response.json()
        if response.status_code == httpx.codes.NOT_FOUND:
            return None
        response.raise_for_status()

    def all(
        self,
        url: str,
        type: type[MetadataSubtype],
        time: datetime | None = None,
    ) -> list[MetadataSubtype] | None:
        raise NotImplementedError

    def remove(
        self,
        url: str,
        type: type[Metadata] | None = None,
        id: str | None = None,
    ):
        encoded_url = URL(url).safe.encoded
        if type:
            params = {"id": id or None}
            self.client.delete(
                f"/catalog/{type.key()}/{encoded_url}",
                params=params,
            ).raise_for_status()
        else:
            self.client.delete(f"/catalog/urls/{encoded_url}").raise_for_status()

    def search(
        self,
        query: str,
        type: type[MetadataSubtype],
        time: datetime | None = None,
    ) -> list[MetadataSubtype]:
        params = {
            "query": query,
        }
        if time:
            params["time"] = time.isoformat()
        response_list = self.client.get(
            f"/catalog/{type.key()}",
            params=params,
        ).json()
        return [type.from_dict(obj) for obj in response_list]


@contextmanager
def create_catalog(
    url: str | None = None,
    **_,
) -> Generator["RecapCatalog", None, None]:
    with httpx.Client(base_url=url or DEFAULT_URL) as client:
        yield RecapCatalog(client)

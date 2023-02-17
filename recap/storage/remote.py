from datetime import datetime
from urllib.parse import quote_plus

from httpx import Client, codes
from pydantic import BaseModel

from recap.storage.abstract import AbstractStorage, Direction, MetadataSubtype


class RemoteStorage(AbstractStorage):
    def __init__(self, base_url: str, **httpx_opts):
        self.client = Client(base_url=base_url, **httpx_opts)

    def metadata(
        self,
        url: str,
        metadata_type: type[MetadataSubtype],
        time: datetime | None = None,
    ) -> MetadataSubtype | None:
        params: dict[str, str] = {}
        if time:
            params["time"] = time.isoformat()
        response = self.client.get(
            f"/{quote_plus(url)}/metadata/{metadata_type.__name__.lower()}",
            params=params,
        )
        if response.status_code == codes.OK:
            match metadata_type:
                case Schema:
                    return Schema.parse_obj(response.json())
        elif response.status_code == codes.NOT_FOUND:
            return None
        response.raise_for_status()

    def links(
        self,
        url: str,
        relationship: str,
        time: datetime | None = None,
        direction: Direction = Direction.FROM,
    ) -> list[str]:  # type: ignore
        params: dict[str, str] = {
            "direction_type": direction.name.lower(),
        }
        if time:
            params["time"] = time.isoformat()
        response = self.client.get(
            f"/{quote_plus(url)}/links/{relationship}", params=params
        )
        match response.status_code:
            case codes.OK:
                return response.json()
            case codes.NOT_FOUND:
                return []
            case _:
                response.raise_for_status()

    def search(
        self,
        query: str,
        metadata_type: type[MetadataSubtype],
        time: datetime | None = None,
    ) -> list[MetadataSubtype]:
        params = {
            "query": query,
        }
        if time:
            params["time"] = time.isoformat()
        response = self.client.get(
            f"/search/{metadata_type.__name__.lower()}", params=params
        )
        if response.status_code == codes.OK:
            match metadata_type:
                case Schema:
                    return [Schema.parse_obj(obj) for obj in response.json()]
        elif response.status_code == codes.NOT_FOUND:
            return []
        response.raise_for_status()
        raise Exception("Unexpected: status not OK, but no exception.")

    def write(
        self,
        url: str,
        metadata: BaseModel,
    ):
        response = self.client.put(
            f"/{quote_plus(url)}/metadata/{type(metadata).__name__.lower()}",
            json=metadata.dict(),
        )
        response.raise_for_status()

    def link(
        self,
        url: str,
        relationship: str,
        other_url: str,
    ):
        params = {
            "other_url": other_url,
        }
        response = self.client.post(
            f"/{quote_plus(url)}/links/{relationship}",
            params=params,
        )
        response.raise_for_status()

    def unlink(
        self,
        url: str,
        relationship: str,
        other_url: str,
    ):
        params = {
            "other_url": other_url,
        }
        response = self.client.delete(
            f"/{quote_plus(url)}/links/{relationship}",
            params=params,
        )
        response.raise_for_status()

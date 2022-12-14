import httpx
from .abstract import AbstractStorage
from contextlib import contextmanager
from pathlib import PurePosixPath
from typing import Any, List, Generator


class RecapStorage(AbstractStorage):
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


@contextmanager
def open(**config) -> Generator[RecapStorage, None, None]:
    with httpx.Client(base_url=config['url']) as client:
        yield RecapStorage(client)

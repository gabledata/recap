import httpx
from .abstract import AbstractSearch
from contextlib import contextmanager
from recap.storage.abstract import AbstractStorage
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

    # TODO Should probably have an /index API to call


@contextmanager
def open(
    storage: AbstractStorage,
    **config
) -> Generator[RecapSearch, None, None]:
    with httpx.Client(base_url=config['url']) as client:
        yield RecapSearch(client)

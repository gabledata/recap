from urllib.parse import quote_plus

import httpx

from recap import types
from recap.converters.recap import RecapConverter


class Client:
    def __init__(
        self,
        url: str,
        converter: RecapConverter | None = None,
        **httpx_opts,
    ):
        self.client = httpx.Client(base_url=url, **httpx_opts)
        self.converter = converter or RecapConverter.get_converter("recap")

    def put_schema(
        self,
        url: str,
        type_: types.Type,
    ):
        response = self.client.put(
            f"/schema/{quote_plus(url)}",
            json=self.converter.from_recap_type(type_),
        )
        response.raise_for_status()


def create_client(
    url: str | None = None,
    converter: RecapConverter | None = None,
    **httpx_opts,
) -> Client:
    from recap.config import settings

    client_settings = settings.client_settings
    combined_opts = client_settings.opts | httpx_opts
    url = url or client_settings.url

    return Client(url, converter, **combined_opts)

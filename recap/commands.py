from recap.clients import create_client, parse_url
from recap.settings import RecapSettings
from recap.types import StructType

settings = RecapSettings()


def ls(url: str | None = None) -> list[str] | None:
    """
    List a URL's children.

    :param url: URL where children are located. If `url` is None, list root URLs.
    :return: List of children. Values are relative to `url`.
    """

    if not url:
        return settings.safe_urls
    connection_url, method_args = parse_url("ls", url)
    with create_client(connection_url) as client:
        return client.ls(*method_args)


def schema(url: str) -> StructType | None:
    """
    Get a URL's schema.

    :param url: URL where schema is located.
    :return: Schema for URL.
    """

    connection_url, method_args = parse_url("schema", url)
    with create_client(connection_url) as client:
        return client.schema(*method_args)

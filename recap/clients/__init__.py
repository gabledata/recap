from __future__ import annotations

from contextlib import contextmanager
from importlib import import_module
from typing import Any, Generator, Protocol
from urllib.parse import parse_qs, urlparse

from recap.settings import RecapSettings
from recap.types import StructType

settings = RecapSettings()
CLIENTS = {
    None: "recap.clients.fs.FilesystemClient",
    "": "recap.clients.fs.FilesystemClient",
    "file": "recap.clients.fs.FilesystemClient",
    "bigquery": "recap.clients.bigquery.BigQueryClient",
    "http+csr": "recap.clients.confluent_registry.ConfluentRegistryClient",
    "https+csr": "recap.clients.confluent_registry.ConfluentRegistryClient",
    "mysql": "recap.clients.mysql.MysqlClient",
    "postgresql": "recap.clients.postgresql.PostgresqlClient",
    "snowflake": "recap.clients.snowflake.SnowflakeClient",
    "sqlite": "recap.clients.sqlite.SQLiteClient",
    "thrift+hms": "recap.clients.hive_metastore.HiveMetastoreClient",
}


@contextmanager
def create_client(url: str) -> Generator[Client, None, None]:
    """
    Create a client from a URL. The supported clients are listed in the
    `recap.clients.CLIENTS` dict.

    :param url: URL to create client from
    :return: Client
    """

    url_args = _url_to_dict(url)
    scheme = url_args["scheme"]

    if client_path := CLIENTS.get(scheme):
        module_path, class_name = client_path.rsplit(".", 1)
        module = import_module(module_path)
        client_class = getattr(module, class_name)
        with client_class.create(**url_args) as client:
            yield client
    else:
        raise ValueError(f"No clients available for scheme: {scheme}")


def parse_url(method: str, url: str, strict: bool = True) -> tuple[str, list[Any]]:
    """
    Parse a URL into a connection URL and a list of method arguments.

    :param method: Either "ls" or "schema".
    :param url: URL to parse
    :param strict: If True, raise an error if the URL is not configured in settings.
    :return: Tuple of connection URL and list of method arguments.
    """

    url_args = _url_to_dict(url)
    scheme = url_args["scheme"]

    if client_path := CLIENTS.get(scheme):
        module_path, class_name = client_path.rsplit(".", 1)
        module = import_module(module_path)
        client_class = getattr(module, class_name)
        connection_url, method_args = client_class.parse(method, **url_args)
        return (settings.unsafe_url(connection_url, strict), method_args)
    else:
        raise ValueError(f"No clients available for scheme: {scheme}")


def _url_to_dict(url: str) -> dict[str, str]:
    """
    Parse a URL into a dict of its components. This is a wrapper around the
    `urllib.parse.urlparse` function that adds some additional fields:

    - `dialect`: The dialect of the URL, e.g. `http`, `https`, `mysql`, etc.
    - `driver`: The driver of the URL, e.g. `csr` for Confluent Schema Registry
    - `user`: The username of the URL
    - `host`: The hostname of the URL
    - `paths`: The path of the URL split into a list
    - `url`: The original URL

    All query parameters are parsed into a dict, as well.

    :param url: URL to parse
    :return: Dict of URL components
    """

    parsed_url = urlparse(url)
    parsed_dict = {
        field: getattr(parsed_url, field)
        for field in [
            "scheme",
            "netloc",
            "path",
            "params",
            "query",
            "fragment",
            "username",
            "password",
            "hostname",
            "port",
        ]
    }

    if "+" in parsed_dict["scheme"]:
        dialect, driver = parsed_dict["scheme"].split("+")
        parsed_dict["dialect"] = dialect
        parsed_dict["driver"] = driver
        url = url.replace(f"{dialect}+{driver}://", f"{dialect}://")

    if username := parsed_url.username:
        parsed_dict["user"] = username

    if hostname := parsed_url.hostname:
        parsed_dict["host"] = hostname

    parsed_dict["path"] = parsed_dict["path"].rstrip("/")
    parsed_dict["paths"] = [path for path in parsed_dict["path"].split("/") if path]
    parsed_dict["url"] = url
    parsed_qs = parse_qs(parsed_dict["query"])
    parsed_qs = {
        key: value[0] if len(value) == 1 else value for key, value in parsed_qs.items()
    }
    parsed_dict |= parsed_qs

    return parsed_dict


class Client(Protocol):
    @staticmethod
    def create(**url_args) -> Client:
        ...

    @staticmethod
    def parse(method: str, **url_args) -> tuple[str, list[Any]]:
        ...

    def ls(self, *vargs) -> list[str]:
        ...

    def schema(self, *vargs) -> StructType:
        ...

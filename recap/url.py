from __future__ import annotations

from functools import cached_property
from pathlib import PurePosixPath
from urllib.parse import urlparse


class URL:
    def __init__(
        self,
        url: str | URL,
        subpath: str | None = None,
    ):
        self.url = URL.insert_subpath(url, subpath) if subpath else str(url)
        self.parsed_url = urlparse(str(self.url))

    @cached_property
    def dialect(self) -> str:
        if scheme := self.parsed_url.scheme:
            return scheme.split("+")[0]
        return "file"

    @cached_property
    def driver(self) -> str | None:
        if scheme := self.parsed_url.scheme:
            return scheme.split("+")[-1]

    @cached_property
    def path(self) -> str | None:
        return self.parsed_url.path or None

    @cached_property
    def path_posix(self) -> PurePosixPath | None:
        if path := self.path:
            return PurePosixPath(path)

    @cached_property
    def host_port(self) -> str | None:
        if netloc := self.parsed_url.netloc:
            return netloc.split("@")[-1]

    @cached_property
    def host_port_path(self) -> PurePosixPath | None:
        host_port = self.host_port or ""
        path = self.parsed_url.path.lstrip("/")
        if host_port or path:
            return PurePosixPath(host_port, path)

    @cached_property
    def dialect_host_port_path(self) -> PurePosixPath:
        host_port_path = self.host_port_path or PurePosixPath("/")
        return PurePosixPath(self.dialect) / str(host_port_path).lstrip("/")

    @staticmethod
    def insert_subpath(url: str | URL, subpath: str) -> str:
        scheme, netloc, path, params, query, fragment = urlparse(str(url))
        path = PurePosixPath("/") / path / subpath.lstrip("/")
        if scheme:
            scheme = f"{scheme}://"
        if params:
            params = f";{params}"
        if query:
            query = f"?{query}"
        if fragment:
            fragment = f"#{fragment}"
        return f"{scheme}{netloc}{path}{params}{query}{fragment}"

    @cached_property
    def safe(self) -> URL:
        scheme = self.dialect
        netloc = self.host_port or ""
        path = self.path or ""
        return URL(f"{scheme}://{netloc}{path}")

    def __str__(self) -> str:
        return self.url

import logging
from dataclasses import dataclass
from re import Pattern
from typing import Any, Callable
from urllib.parse import urlparse

from fsspec import AbstractFileSystem, get_fs_token_paths
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from starlette.routing import compile_path

from recap import types

log = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class FunctionParams:
    pattern: Pattern
    include_engine: bool = False
    include_fs: bool = False
    include_url: bool = False

    def method_args(self, url: str, **kwargs) -> dict[str, Any]:
        method_args = {}
        if self.include_engine:
            method_args["engine"] = self.engine(url, **kwargs)
        if self.include_fs:
            method_args["fs"] = self.fs(url, **kwargs)
        if self.include_url:
            method_args["url"] = url
        return method_args

    def engine(self, url: str, **engine_args) -> Engine:
        parsed_url = urlparse(url)
        match parsed_url.scheme, parsed_url.netloc, parsed_url.path.split("/"):
            case "bigquery", project, _:
                return create_engine(f"bigquery://{project}", **engine_args)
            case str(scheme), str(netloc), path:
                connect_url = f"{scheme}://{netloc}"
                if len(path) > 1:
                    connect_url += f"/{path[1]}"
                return create_engine(connect_url, **engine_args)
        raise ValueError(f"Unable to create engine for url={url}")

    def fs(self, url: str, **storage_options) -> AbstractFileSystem:
        fs, _, _ = get_fs_token_paths(url, storage_options=storage_options)
        return fs


class Registry:
    def __init__(self):
        self.ls_functions: dict[FunctionParams, Callable[..., list[str] | None]] = {}
        self.schema_functions: dict[
            FunctionParams, Callable[..., types.Type | None]
        ] = {}

    def schema(
        self,
        pattern: str,
        include_engine: bool = False,
        include_fs: bool = False,
        include_url: bool = False,
    ):
        def inner(
            callable: Callable[..., types.Type | None],
        ) -> Callable[..., types.Type | None]:
            pattern_regex, _, _ = compile_path(pattern)
            params = FunctionParams(
                pattern=pattern_regex,
                include_fs=include_fs,
                include_engine=include_engine,
                include_url=include_url,
            )
            self.schema_functions[params] = callable
            return callable

        return inner

    def ls(
        self,
        pattern: str,
        include_engine: bool = False,
        include_fs: bool = False,
        include_url: bool = False,
    ):
        pattern_regex, _, _ = compile_path(pattern)
        params = FunctionParams(
            pattern=pattern_regex,
            include_fs=include_fs,
            include_engine=include_engine,
            include_url=include_url,
        )

        def inner(
            callable: Callable[..., list[str] | None],
        ) -> Callable[..., list[str] | None]:
            self.ls_functions[params] = callable
            return callable

        return inner


class Reader:
    def __init__(self, registry: Registry):
        self.registry = registry

    def schema(self, url: str, **kwargs) -> types.Type | None:
        for params, callable in self.registry.schema_functions.items():
            if match := params.pattern.match(url):
                try:
                    if type_ := callable(
                        **params.method_args(url, **kwargs),
                        **match.groupdict(),
                        **kwargs,
                    ):
                        return type_
                except Exception as e:
                    log.warning(e)

    def ls(self, url: str, **kwargs) -> list[str] | None:
        for params, callable in self.registry.ls_functions.items():
            if match := params.pattern.match(url):
                try:
                    if links := callable(
                        **params.method_args(url, **kwargs),
                        **match.groupdict(),
                        **kwargs,
                    ):
                        return list(links)
                except Exception as e:
                    log.warning(e)
        return []


functions = Registry()

# Load known readers into the registry
# Has to happen after `functions` is created to avoid circular import.
# TODO This is ugly.
import recap.readers.fsspec
import recap.readers.sqlalchemy

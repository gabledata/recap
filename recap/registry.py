from dataclasses import dataclass
from pathlib import PurePath
from re import Pattern
from typing import Any, Callable
from urllib.parse import urlparse

from fsspec import AbstractFileSystem, get_fs_token_paths
from pandas import DataFrame, read_csv, read_json, read_parquet, read_sql_table
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from starlette.routing import compile_path

from recap.storage.abstract import Direction


@dataclass(frozen=True, kw_only=True)
class MethodArgsParams:
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


@dataclass(frozen=True)
class MetadataParams(MethodArgsParams):
    include_df: bool = False

    def method_args(self, url: str, **kwargs) -> dict[str, Any]:
        method_args = super().method_args(url, **kwargs)
        if self.include_df:
            method_args["df"] = self.df(url, **kwargs)
        return method_args

    def df(self, url: str, **kwargs) -> DataFrame:
        try:
            engine = self.engine(url, **kwargs)
            with engine.connect() as connection:
                table = PurePath(url).parts[-1]
                read_sql_table(table, connection)
        except:
            pass
        try:
            match PurePath(urlparse(url).path).suffix:
                case ".json" | ".jsonl" | ".ndjson":
                    return read_json(url, **kwargs)
                case ".csv" | ".tsv":
                    return read_csv(url, **kwargs)
                case ".parquet":
                    return read_parquet(url, **kwargs)
        except:
            pass
        raise ValueError(f"Unable to create data frame for url={url}")


@dataclass(frozen=True)
class RelationshipParams(MethodArgsParams):
    relationship: str
    direction: Direction = Direction.FROM


class FunctionRegistry:
    def __init__(self):
        self.relationship_registry: dict[
            RelationshipParams, Callable[..., list[str]]
        ] = {}
        self.metadata_registry: dict[MetadataParams, Callable[..., BaseModel]] = {}

    def metadata(
        self,
        pattern: str,
        include_df: bool = False,
        include_engine: bool = False,
        include_fs: bool = False,
        include_url: bool = False,
    ):
        def inner(
            callable: Callable[..., BaseModel],
        ) -> Callable[..., BaseModel]:
            pattern_regex, _, _ = compile_path(pattern)
            params = MetadataParams(
                pattern=pattern_regex,
                include_df=include_df,
                include_fs=include_fs,
                include_engine=include_engine,
                include_url=include_url,
            )
            self.metadata_registry[params] = callable
            return callable

        return inner

    def relationship(
        self,
        pattern: str,
        type: str,
        direction: Direction = Direction.FROM,
        include_engine: bool = False,
        include_fs: bool = False,
        include_url: bool = False,
    ):
        pattern_regex, _, _ = compile_path(pattern)
        params = RelationshipParams(
            pattern=pattern_regex,
            relationship=type,
            direction=direction,
            include_fs=include_fs,
            include_engine=include_engine,
            include_url=include_url,
        )

        def inner(
            callable: Callable[..., list[str]],
        ) -> Callable[..., list[str]]:
            self.relationship_registry[params] = callable
            return callable

        return inner


registry = FunctionRegistry()

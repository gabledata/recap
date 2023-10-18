from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

from recap.types import StructType


class FilesystemClient:
    def __init__(self, scheme: str):
        self.scheme = scheme

    @staticmethod
    @contextmanager
    def create(scheme: str, **_) -> Generator[FilesystemClient, None, None]:
        yield FilesystemClient(scheme=scheme)

    @staticmethod
    def parse(_: str, **url_args) -> tuple[str, list[Any]]:
        from urllib.parse import urlsplit

        split_url = urlsplit(url_args["url"])
        root_url = f"{split_url.scheme or 'file'}://"
        path = "/"

        if split_url.netloc:
            path += split_url.netloc + "/"
        if split_url.path:
            path += split_url.path[1:] + "/"

        return (root_url, [path])

    def ls(self, path: str = "/") -> list[str]:
        if self.scheme == "file":
            return [p.name for p in Path(path).iterdir()]
        else:
            import fsspec

            fs = fsspec.filesystem(self.scheme)
            return [Path(p).name for p in fs.ls(path)]

    def schema(self, path: str = "/") -> StructType:
        suffix = Path(path).suffix.lower().strip(".")

        match suffix:
            case "avsc" | "avro" | "avdl" | "avpr":
                from recap.converters.avro import AvroConverter

                return AvroConverter().to_recap(self._read_file(path))
            case "json" | "jsonschema" | "jsons":
                from recap.converters.json_schema import JSONSchemaConverter

                return JSONSchemaConverter().to_recap(self._read_file(path))
            case "proto" | "protobuf" | "protos" | "proto3" | "proto2":
                from recap.converters.protobuf import ProtobufConverter

                return ProtobufConverter().to_recap(self._read_file(path))
            case _:
                raise ValueError(f"Unsupported file type {suffix}")

    def _read_file(self, path: str) -> str:
        if self.scheme == "file":
            return Path(path).read_text()
        else:
            import fsspec

            fs = fsspec.filesystem(self.scheme)
            return fs.read_text(path)

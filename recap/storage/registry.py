import json
from pathlib import Path
from urllib.parse import quote_plus, unquote_plus

import fsspec

from recap.types import RecapType, from_dict, to_dict


class RegistryStorage:
    def __init__(self, storage_url: str, **storage_url_args):
        self.fs, self.root_path = fsspec.core.url_to_fs(storage_url, **storage_url_args)
        self.fs.mkdirs(self.root_path, exist_ok=True)

    def ls(self) -> list[str]:
        return sorted(
            [
                unquote_plus(file_path[len(self.root_path) + 1 :])
                for file_path in self.fs.ls(self.root_path)
            ]
        )

    def get(
        self,
        name: str,
        version: int | None = None,
    ) -> tuple[RecapType, int] | None:
        quoted_name = quote_plus(name)

        if version is None:
            versions = self.versions(name)
            if not versions:
                return None
            version = max(versions)

        try:
            with self.fs.open(f"{self.root_path}/{quoted_name}/{version}.json") as f:
                type_json = json.load(f)
                type_ = from_dict(type_json)
                return (type_, version)
        except FileNotFoundError:
            return None

    def put(
        self,
        name: str,
        type_: RecapType,
        version: int | None = None,
    ) -> int:
        quoted_name = quote_plus(name)

        if version is None:
            version = (self.latest(name) or 0) + 1

        path_without_version = f"{self.root_path}/{quoted_name}"
        type_dict = to_dict(type_)

        self.fs.mkdirs(path_without_version, exist_ok=True)

        with self.fs.open(f"{path_without_version}/{version}.json", "w") as f:
            json.dump(type_dict, f)

        return version

    def versions(self, name: str) -> list[int] | None:
        quoted_name = quote_plus(name)
        path_without_version = f"{self.root_path}/{quoted_name}"

        try:
            return sorted(
                [
                    int(Path(file_path).stem)
                    for file_path in self.fs.ls(path_without_version)
                ]
            )
        except FileNotFoundError:
            return None

    def latest(self, name: str) -> int | None:
        versions = self.versions(name)
        if not versions:
            return None
        return max(versions)

from .abstract import AbstractStorage
from pathlib import PurePosixPath
from recap.search.abstract import AbstractIndexer
from typing import Any, List


class StorageNotifier(AbstractStorage):
    def __init__(
        self,
        storage: AbstractStorage,
        indexer: AbstractIndexer,
    ):
        self.storage = storage
        self.indexer = indexer

    def touch(
        self,
        path: PurePosixPath,
    ):
        self.storage.touch(path)
        self.indexer.touched(path)

    def write(
        self,
        path: PurePosixPath,
        type: str,
        metadata: Any,
    ):
        self.storage.write(
            path,
            type,
            metadata,
        )
        self.indexer.written(path, type, metadata)

    def rm(
        self,
        path: PurePosixPath,
        type: str | None = None,
    ):
        self.storage.rm(path, type)
        self.indexer.removed(path, type)

    def ls(
        self,
        path: PurePosixPath,
    ) -> List[str] | None:
        return self.storage.ls(path)

    def read(
        self,
        path: PurePosixPath,
    ) -> dict[str, Any] | None:
        return self.storage.read(path)

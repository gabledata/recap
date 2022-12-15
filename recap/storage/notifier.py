from .abstract import AbstractStorage
from abc import ABC
from pathlib import PurePosixPath
from typing import Any, List


class StorageListener:
    def touched(
        self,
        path: PurePosixPath,
    ):
        pass

    def written(
        self,
        path: PurePosixPath,
        type: str,
        metadata: Any,
    ):
        pass

    def removed(
        self,
        path: PurePosixPath,
        type: str | None = None,
    ):
        pass


class StorageNotifier(AbstractStorage):
    def __init__(
        self,
        storage: AbstractStorage,
        listeners: List[StorageListener] = []
    ):
        self.storage = storage
        self.listeners = listeners

    def touch(
        self,
        path: PurePosixPath,
    ):
        self.storage.touch(path)
        for listener in self.listeners:
            listener.touched(path)

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
        for listener in self.listeners:
            listener.written(path, type, metadata)

    def rm(
        self,
        path: PurePosixPath,
        type: str | None = None,
    ):
        self.storage.rm(path, type)
        for listener in self.listeners:
            listener.removed(path, type)

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

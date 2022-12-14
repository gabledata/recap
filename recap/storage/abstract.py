from abc import ABC, abstractmethod
from pathlib import PurePosixPath
from typing import Any, List


class AbstractStorage(ABC):
    @abstractmethod
    def touch(
        self,
        path: PurePosixPath,
    ):
        raise NotImplementedError

    @abstractmethod
    def write(
        self,
        path: PurePosixPath,
        type: str,
        metadata: Any,
    ):
        raise NotImplementedError

    @abstractmethod
    def rm(
        self,
        path: PurePosixPath,
        type: str | None = None,
    ):
        raise NotImplementedError

    @abstractmethod
    def ls(
        self,
        path: PurePosixPath,
    ) -> List[str] | None:
        raise NotImplementedError

    @abstractmethod
    def read(
        self,
        path: PurePosixPath,
    ) -> dict[str, Any] | None:
        raise NotImplementedError

    # TODO Not sure this really belongs here...
    def _path_components_dict(self, path: PurePosixPath) -> dict[str, str]:
        components_doc = {}
        path_parts = list(path.parts)

        # Get rid of leading separator if it's there
        if path_parts[0] == '/':
            path_parts = path_parts[1:]

        # Path format is always something like:
        #     /databases/<infra>/instances/<myinstance>/etc
        # So split the path up and pop off values and keys to add to the doc.
        # TODO singularize plural keys for user convenience
        while path_parts:
            v = path_parts.pop()
            k = path_parts.pop()
            components_doc[k] = v

        return components_doc

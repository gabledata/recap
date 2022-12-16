from abc import ABC, abstractmethod
from pathlib import PurePosixPath
from typing import List, Any


class AbstractSearchIndex(ABC):
    @abstractmethod
    def search(
        self,
        query: str,
    ) -> List[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def written(
        self,
        path: PurePosixPath,
        type: str,
        metadata: Any,
    ):
        raise NotImplementedError

    @abstractmethod
    def removed(
        self,
        path: PurePosixPath,
        type: str | None = None,
    ):
        raise NotImplementedError

    def touched(
        self,
        path: PurePosixPath,
    ):
        # Allow indexers to ignore touch events if it pleases them.
        pass

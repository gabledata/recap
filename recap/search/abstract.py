from recap.storage.abstract import AbstractStorage
from abc import ABC, abstractmethod
from pathlib import PurePosixPath
from typing import List, Any


class AbstractSearch(ABC):
    @abstractmethod
    def search(
        self,
        query: str,
    ) -> List[dict[str, Any]]:
        raise NotImplementedError

    def index(
        self,
        metadata: dict[str, Any],
    ):
        pass

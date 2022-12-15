from recap.storage.abstract import AbstractStorage
from recap.storage.notifier import StorageListener
from abc import ABC, abstractmethod
from pathlib import PurePosixPath
from typing import List, Any


class AbstractSearch(ABC, StorageListener):
    @abstractmethod
    def search(
        self,
        query: str,
    ) -> List[dict[str, Any]]:
        raise NotImplementedError


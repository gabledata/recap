from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import PurePosixPath
from typing import Generator, List


class AbstractBrowser(ABC):
    url: str = NotImplemented

    @staticmethod
    @abstractmethod
    def browsable(url: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def children(self, path: PurePosixPath) -> List[str]:
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def root(**config) -> PurePosixPath:
        raise NotImplementedError

    @staticmethod
    @contextmanager
    @abstractmethod
    def open(**config) -> Generator['AbstractBrowser', None, None]:
        raise NotImplementedError

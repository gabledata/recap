from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import PurePosixPath
from typing import Any, Generator


class AbstractAnalyzer(ABC):
    @staticmethod
    @abstractmethod
    def analyzable(url: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def analyze(self, path: PurePosixPath) -> dict[str, Any]:
        raise NotImplementedError

    @staticmethod
    @contextmanager
    @abstractmethod
    def open(**config) -> Generator['AbstractAnalyzer', None, None]:
        raise NotImplementedError

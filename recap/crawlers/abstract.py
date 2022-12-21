from abc import ABC, abstractmethod
from pathlib import PurePosixPath
from typing import List


class AbstractBrowser(ABC):
    @abstractmethod
    def children(self, path: PurePosixPath) -> List[str]:
        raise NotImplementedError


class AbstractCrawler(ABC):
    @abstractmethod
    def crawl(self):
        raise NotImplementedError
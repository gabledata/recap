from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import PurePosixPath
from pydantic import BaseModel
from stringcase import snakecase
from typing import Generator


class BaseMetadataModel(BaseModel):
    def key(self) -> str:
        return snakecase(self.__class__.__name__)


class AbstractAnalyzer(ABC):
    """
    The abstract class for all analyzers. Analyzers are responsible for
    inspecting data and returning metadata.
    """

    @abstractmethod
    def analyze(self, **kwargs) -> BaseMetadataModel | None:
        """
        Analyze a path for an infrastructure instance. Only the path is
        specified because the URL for the instance is passed in via the config
        in `open()`.

        :returns: BaseMetadataModel Pydantic BaseModel, which represents
            discovered metadata. This data gets serialized as JSON in the
            catalog.
        """

        raise NotImplementedError

    @staticmethod
    @contextmanager
    @abstractmethod
    def open(**config) -> Generator['AbstractAnalyzer', None, None]:
        """
        Creates and returns an analyzer using the supplied config.
        """

        raise NotImplementedError

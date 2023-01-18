from abc import ABC, abstractmethod
from pydantic import BaseModel
from recap.paths import CatalogPath
from stringcase import snakecase


class BaseMetadataModel(BaseModel):
    @classmethod
    def key(cls: type['BaseMetadataModel']) -> str:
        return snakecase(cls.__name__)


class AbstractAnalyzer(ABC):
    """
    The abstract class for all analyzers. Analyzers are responsible for
    inspecting data and returning metadata.
    """

    @abstractmethod
    def analyze(self, path: CatalogPath) -> BaseMetadataModel | None:
        """
        Analyze a path for an infrastructure instance. Only the path is
        specified because the URL for the instance is passed in via the config
        in `open()`.

        :returns: BaseMetadataModel Pydantic BaseModel, which represents
            discovered metadata. This data gets serialized as JSON in the
            catalog.
        """

        raise NotImplementedError

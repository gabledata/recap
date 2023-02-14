from abc import ABC, abstractmethod

from recap.metadata import Metadata


class AbstractAnalyzer(ABC):
    """
    The abstract class for all analyzers. Analyzers are responsible for
    inspecting data and returning metadata.
    """

    @abstractmethod
    def analyze(self, path: str) -> Metadata | None:
        """
        Analyze a path for an infrastructure instance. Only the path is
        specified because the URL for the instance is passed in via the config
        in `create_analyzer()`.

        :returns: Pydantic BaseModel that represents discovered metadata. This
            data gets serialized as JSON in the catalog.
        """

        raise NotImplementedError

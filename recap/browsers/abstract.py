from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import PurePosixPath
from typing import Generator, List


class AbstractBrowser(ABC):
    """
    The abstract class for all browsers. Recap uses a browser abstraction to
    deal with different data infrastructure in a standard way.

    Browsers map infrastructure objects into a standard directory format. A
    different browser is used for each type of infrastructure.

    A KafkaBrowser might list directories like this:

        /
        /streaming
        /streaming/kafka
        /streaming/kafka/instances
        /streaming/kafka/instances/log-cluster
        /streaming/kafka/instances/log-cluster/streams
        /streaming/kafka/instances/log-cluster/streams/access-logs
        /streaming/kafka/instances/log-cluster/streams/error-logs
    """

    url: str = NotImplemented
    """
    The base URL for the infrastructure that this browser is browsing.
    Something like 'postgresql://user:pass@localhost/my_db`.
    """

    @abstractmethod
    def children(self, path: PurePosixPath) -> List[str]:
        """
        Given a path, returns its children. Using the example above,
        path="/streaming/kafka/instances/log-cluster/streams" would return:

            ["access-logs", "error-logs"]
        """

        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def root(**config) -> PurePosixPath:
        """
        Returns the root directory for this browser. Using the example above,
        root() would return:

            /streaming/kafka/instances/log-cluster
        """

        raise NotImplementedError

    @staticmethod
    @contextmanager
    @abstractmethod
    def open(**config) -> Generator['AbstractBrowser', None, None]:
        """
        Creates and returns a browser using the supplied config.
        """

        raise NotImplementedError

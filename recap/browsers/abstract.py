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

    @staticmethod
    @abstractmethod
    def browsable(url: str) -> bool:
        """
        Determine if this browser can browse a given infrastructure instance.
        In plain english, `browsable` answers the question "Can FooBrowser
        return directory lists for 'postgresql://user:pass@localhost/some_db'"?

        `browsable` is called before the browser is instantiated. If it
        returns False, the browser is skipped.

        :param url: The instance URL that will be browsed. This URL points to
            the base of the instance; it doesn't include a path to specific
            data. For example, it would look like
            "postgresql://user:pass@localhost/some_db", rather than
            "postgresql://user:pass@localhost/some_db/some_schema/some_table".

            `browsable` will be called once, then `children()` will be called
            multiple times, once for each path to data.
        :returns: True if the browser is compatible with the given URL, else
            False.
        """

        raise NotImplementedError

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

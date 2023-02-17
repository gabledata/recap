from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import TypeVar

from pydantic import BaseModel

MetadataSubtype = TypeVar("MetadataSubtype", bound=BaseModel)
"""
A templated type that extends Pydantic's `BaseModel` class.
"""


class Direction(Enum):
    """
    Edge direction between two nodes.
    """

    FROM = 1
    TO = 2


class AbstractStorage(ABC):
    """
    An abstract representation of Recap's storage layer.

    Recap's storage layer provides graph, search, and time travel capabilities.
    Nodes are represented by URLs. A URL can be linked to another with a
    relationship type. Metadata can be attached to a node.
    """

    @abstractmethod
    def metadata(
        self,
        url: str,
        metadata_type: type[MetadataSubtype],
        time: datetime | None = None,
    ) -> MetadataSubtype | None:
        """
        Read a type of metadata for a URL.

        :param url:
        :param metadata_type:
        :param time:
        :returns:
        """

        raise NotImplementedError

    @abstractmethod
    def links(
        self,
        url: str,
        relationship: str,
        time: datetime | None = None,
        direction: Direction = Direction.FROM,
    ) -> list[str]:
        """
        Read graph edges (links) for a URL.

        :param url:
        :param relationship:
        :param time:
        :param direction:
        :returns:
        """

        raise NotImplementedError

    @abstractmethod
    def search(
        self,
        query: str,
        metadata_type: type[MetadataSubtype],
        time: datetime | None = None,
    ) -> list[MetadataSubtype]:
        """
        Search for metadata.

        :param query:
        :param metadata_type:
        :param time:
        :returns:
        """

        raise NotImplementedError

    @abstractmethod
    def write(
        self,
        url: str,
        metadata: BaseModel,
    ):
        """
        (Over)write a type of metadata for a URL.

        :param url:
        :param metadata:
        """

        raise NotImplementedError

    @abstractmethod
    def link(
        self,
        url: str,
        relationship: str,
        other_url: str,
    ):
        """
        Connect two URLs in the graph with relationship type.

        :param url:
        :param relationship:
        :param other_url:
        """

        raise NotImplementedError

    @abstractmethod
    def unlink(
        self,
        url: str,
        relationship: str,
        other_url: str,
    ):
        """
        Disconnect two URLs in the graph with relationship type.

        :param url:
        :param relationship:
        :param other_url:
        :returns:
        """

        raise NotImplementedError

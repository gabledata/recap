from abc import ABC, abstractmethod
from datetime import datetime

from recap.metadata import Metadata, MetadataSubtype


class AbstractCatalog(ABC):
    """
    The abstract class for all catalogs. Recap catalogs store metadata and
    expose read and search APIs. Catalogs follow the same directory structure
    as AbstractBrowsers.
    """

    @abstractmethod
    def add(
        self,
        url: str,
        metadata: Metadata | None = None,
    ):
        """
        Add metadata at a URL.
        """

        raise NotImplementedError

    @abstractmethod
    def read(
        self,
        url: str,
        type: type[MetadataSubtype],
        id: str | None = None,
        time: datetime | None = None,
    ) -> MetadataSubtype | None:
        """
        Read metadata for a URL.

        :returns: Metadata dictionary of the format {"metadata_type": Any}.
        """

        raise NotImplementedError

    @abstractmethod
    def children(
        self,
        url: str,
        time: datetime | None = None,
    ) -> list[str] | None:
        raise NotImplementedError

    @abstractmethod
    def all(
        self,
        url: str,
        type: type[MetadataSubtype],
        time: datetime | None = None,
    ) -> list[MetadataSubtype] | None:
        """
        Returns all children in a directory. This method does not signal
        whether or not a directory has metadata, since metadata is not a child
        of a directory. To check if a path has metadata, call `read`.

        :returns: A list of child names. Does not include absolute path.
        """

        raise NotImplementedError

    @abstractmethod
    def remove(
        self,
        url: str,
        type: type[Metadata] | None = None,
        id: str | None = None,
    ):
        """
        Remove a directory or metadata entry. If type is note set, the whole
        directory (including all children and metadata) is removed.
        """

        raise NotImplementedError

    @abstractmethod
    def search(
        self,
        query: str,
        type: type[MetadataSubtype],
        time: datetime | None = None,
    ) -> list[MetadataSubtype]:
        """
        Searches an entire catalog for metadata. The query syntax is dependent
        on the catalog implementation.

        :param query: A query string to match against metadata in a catalog.
        """

        raise NotImplementedError

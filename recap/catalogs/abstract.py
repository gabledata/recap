from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any


class AbstractCatalog(ABC):
    """
    The abstract class for all catalogs. Recap catalogs store metadata and
    expose read and search APIs. Catalogs follow the same directory structure
    as AbstractBrowsers.
    """

    @abstractmethod
    def touch(
        self,
        path: str,
    ):
        """
        Creates an empty directory.
        """

        raise NotImplementedError

    @abstractmethod
    def write(
        self,
        path: str,
        metadata: dict[str, Any],
        patch: bool = True,
    ):
        """
        Writes metadata to a directory location.

        :param type: Metadata types can be any string. Common examples are
            "schema", "pii", "indexes", "profile", and so on. Types are defined
            based on the data that AbstractAnalyzers return.
        """

        raise NotImplementedError

    @abstractmethod
    def rm(
        self,
        path: str,
    ):
        """
        Remove a directory or metadata entry. If type is note set, the whole
        directory (including all children and metadata) is removed.

        :param type: If specified, the type of metadata to delete.
        """

        raise NotImplementedError

    @abstractmethod
    def ls(
        self,
        path: str,
        as_of: datetime | None = None,
    ) -> list[str] | None:
        """
        Returns all children in a directory. This method does not signal
        whether or not a directory has metadata, since metadata is not a child
        of a directory. To check if a path has metadata, call `read`.

        :returns: A list of child names. Does not include absolute path.
        """

        raise NotImplementedError

    @abstractmethod
    def read(
        self,
        path: str,
        as_of: datetime | None = None,
    ) -> dict[str, Any] | None:
        """
        Read all metadata in a directory.

        :returns: Metadata dictionary of the format {"metadata_type": Any}.
        """

        raise NotImplementedError

    @abstractmethod
    def search(
        self,
        query: str,
        as_of: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """
        Searches an entire catalog for metadata. The query syntax is dependent
        on the catalog implementation.

        :param query: A query string to match against metadata in a catalog.
        """

        raise NotImplementedError

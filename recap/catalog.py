from __future__ import annotations

from datetime import datetime
from enum import Enum
from urllib.parse import urlparse

from recap.metadata import Schema
from recap.registry import FunctionRegistry
from recap.registry import registry as global_registry
from recap.storage.abstract import AbstractStorage, Direction, MetadataSubtype


def safe(url: str) -> str:
    """
    Strips the `username:password` from a URL.

    Given:

        scheme://user:pass@host:port/some/path;param?some=query#fragment

    Returns:

        scheme://host:port/some/path;param?some=query#fragment

    :param url: URL to make safe.
    :returns: A URL with the `username:password` removed.
    """

    parsed_url = urlparse(url)
    safe_url = f"{parsed_url.scheme or 'file'}://"
    safe_url += parsed_url.netloc.split("@", 1)[-1]
    safe_url += parsed_url.path
    if params := parsed_url.params:
        safe_url += f";{params}"
    if query := parsed_url.query:
        safe_url += f"?{query}"
    if fragment := parsed_url.fragment:
        safe_url += f"#{fragment}"
    return safe_url


class Relationship(Enum):
    """
    A relationship between two URLs in the catalog.
    """

    CONTAINS = 1
    """
    The source URL contains the destination URL. This is used to model data
    hierarchy. Folders contain files, databases contain schemas, and so on.
    """

    RUNS = 2
    """
    The source URL runs the destination URL. RUNS is usually used to mode job
    executation: an account runs a job.
    """

    READS = 3
    """
    The source URL reads the destination URL. READS is used to model both
    account access and job lineage. When the source URL is an account, READS
    signals that the account has read-access to the destination URL. When the
    source URL is a job, READS signals that the job reads the destination URL's
    data.
    """

    WRITES = 4
    """
    The source URL writes to the destination URL. WRITES is used to model both
    account access and job lineage. When the source URL is an account, WRITES
    signals that the account has write-access to the destination URL. When the
    source URL is a job, WRITES signals that the job writes to the destination
    URL's data.
    """


class Catalog:
    """
    Recap's catalog defines standard metadata and relationships between URLs.
    The only metadata the catalog models is `recap.metadata.Schema`.

    Relationships are modeled as:

    * `account` runs `job`
    * `account` reads `data`
    * `account` writes `data`
    * `job` reads `data`
    * `job` writes `data`
    * `data` contains `data`

    `data` contains `data` might seem odd at first, but it simply models how
    object stores like S3 behave. Databases and tables are all treated as
    `data`.
    """

    def __init__(
        self,
        storage: AbstractStorage,
        registry: FunctionRegistry | None = None,
    ):
        self.storage = storage
        self.registry = registry or global_registry

    def ls(
        self,
        url: str,
        time: datetime | None = None,
        refresh: bool = False,
        **kwargs,
    ) -> list[str] | None:
        """
        Lists all URLs that the provided URL contains. In a filesystem, `ls`
        would return a path's children.

        :param url: Fetch the contained children for this URL.
        :param time: Time travel to a point in time. Causes the method to
            return children from the last crawl before the provided timestamp.
        :param refresh: Bypass the catalog's cache, fetching the data directly
            from the URL. Cache the results in the catalog.
        :returns: A list of URLs contained by the input URL. None if the input
            URL doesn't exist.
        """

        return self._links(
            url,
            Relationship.CONTAINS,
            time,
            refresh,
            **kwargs,
        )

    def readers(
        self,
        url: str,
        time: datetime | None = None,
        refresh: bool = False,
        **kwargs,
    ) -> list[str] | None:
        """
        Lists all URLs that read from the input URL.

        :param url: Fetch readers for this this URL.
        :param time: Time travel to a point in time. Causes the method to
            return readers from the last crawl before the provided timestamp.
        :param refresh: Bypass the catalog's cache, fetching the data directly
            from the URL. Cache the results in the catalog.
        :returns: A list of URLs that read from the input URL. If the return
            URL represents an account, then the account has read-access to the
            input URL. If the return URL represents a job, then the account
            reads the input URL's data.
        """

        return self._links(
            url,
            Relationship.READS,
            time,
            refresh,
            Direction.TO,
            **kwargs,
        )

    def reads(
        self,
        url: str,
        time: datetime | None = None,
        refresh: bool = False,
        **kwargs,
    ) -> list[str] | None:
        """
        Lists all URLs that the input URL reads.

        :param url: Fetch URLs that this URL reads.
        :param time: Time travel to a point in time. Causes the method to
            return URLs from the last crawl before the provided timestamp.
        :param refresh: Bypass the catalog's cache, fetching the data directly
            from the URL. Cache the results in the catalog.
        :returns: A list of URLs that the input URL has read from. If the input
            URL is an account, the account will have read-access to the
            returned URLs. If the input URL is a job, the job has read from
            the return URLs.
        """

        return self._links(
            url,
            Relationship.READS,
            time,
            refresh,
            Direction.FROM,
            **kwargs,
        )

    def schema(
        self,
        url: str,
        time: datetime | None = None,
        refresh: bool = False,
        **kwargs,
    ) -> Schema | None:
        """
        Returns a schema for the URL.

        :param url: URL to fetch schema for.
        :param time: Time travel to a point in time. Causes the method to
            return a scheam from the last crawl before the provided timestamp.
        :param refresh: Bypass the catalog's cache, fetching the schema directly
            from the URL. Cache the results in the catalog.
        :returns: A schema for the URL or `None` if the URL doesn't exist.
        """

        if time and refresh:
            raise ValueError("Unsupported: `refresh` and `time` are both set.")
        safe_url = safe(url)
        if not refresh and (schema := self.storage.metadata(safe_url, Schema, time)):
            return schema
        elif not time:
            for params, callable in self.registry.metadata_registry.items():
                if match := params.pattern.match(url):
                    metadata = callable(
                        **params.method_args(url, **kwargs),
                        **match.groupdict(),
                        **kwargs,
                    )
                    match metadata:
                        case Schema():
                            self.storage.write(safe_url, metadata)
                            return metadata

    def search(
        self,
        query: str,
        metadata_type: type[MetadataSubtype],
        time: datetime | None = None,
    ) -> list[MetadataSubtype]:
        """
        Search for metadata in the catallog.

        :param query: Query to apply to the storage layer. This is usually a
            `WHERE` clause, for example:
                `"url='file:///tmp/recap-test/foo/foo.json'"`
            or
                `"json_extract(metadata_obj, '$.fields.name') = 'email'"`
        :param metadata_type: The type of metadata to search.
        :param time: Time travel to a point in time. Causes the method to
            return URLs from the last crawl before the provided timestamp.
        :returns: A list of Pydantic `BaseModel` metadata objects that match
            the search query.
        """

        return self.storage.search(query, metadata_type, time)

    def writers(
        self,
        url: str,
        time: datetime | None = None,
        refresh: bool = False,
        **kwargs,
    ) -> list[str] | None:
        """
        List all URLs that write to the input URL.

        :param url: URL that is written to.
        :param time: Time travel to a point in time. Causes the method to
            return URLs from the last crawl before the provided timestamp.
        :param refresh: Bypass the catalog's cache, fetching the data directly
            from the URL. Cache the results in the catalog.
        :returns: A list of URLs that have written to the input URL. If the
            return URL is an account, the account will have write-access to the
            input URL. If the return URL is a job, the job has written to
            the input URL.
        """

        return self._links(
            url,
            Relationship.WRITES,
            time,
            refresh,
            Direction.TO,
            **kwargs,
        )

    def writes(
        self,
        url: str,
        time: datetime | None = None,
        refresh: bool = False,
        **kwargs,
    ) -> list[str] | None:
        """
        List all URLs that the input URL writes to.

        :param url: Fetch output URLs written to by this input URL.
        :param time: Time travel to a point in time. Causes the method to
            return URLs from the last crawl before the provided timestamp.
        :param refresh: Bypass the catalog's cache, fetching the data directly
            from the URL. Cache the results in the catalog.
        :returns: A list of URLs that the input URL writes to. If the input URL
            is an account, the account will have write-access to the return
            URLs. If the input URL is a job, the job has written to the return
            URLs.
        """

        return self._links(
            url,
            Relationship.WRITES,
            time,
            refresh,
            Direction.FROM,
            **kwargs,
        )

    def _links(
        self,
        url: str,
        relationship: Relationship,
        time: datetime | None = None,
        refresh: bool = False,
        direction: Direction = Direction.FROM,
        **kwargs,
    ) -> list[str] | None:
        """
        A helper method that fetches links for a given URL. The method manages
        fetching from the storage layer. If there's a cache miss (or `refresh`
        is set), the method will try to use the function registry to fetch
        links straignt from the URL. If new data is fetched, the resulting
        links are merged into the storage layer (new links are added and old
        links are removed).

        :param url: URL to fetch links for.
        :param relationship: The type of relationship to fetch.
        :param time: Time travel to a point in time. Causes the method to
            return URLs from the last crawl before the provided timestamp.
        :param refresh: Bypass the catalog's cache, fetching the data directly
            from the URL. Cache the results in the catalog.
        :param direction: Determines whether the URL is the source or
            destination of the relationship.
        :returns: A list of URLs that have a relationship with the input URL.
        """
        if time and refresh:
            raise ValueError("Unsupported: `refresh` and `time` are both set.")
        safe_url = safe(url)
        storage_links = self.storage.links(
            url=safe_url,
            relationship=relationship.name.lower(),
            time=time,
            direction=direction,
        )
        if not refresh and storage_links:
            return storage_links
        elif not time:
            for params, callable in self.registry.relationship_registry.items():
                if (
                    relationship.name.lower() == params.relationship
                    and direction == params.direction
                    and (match := params.pattern.match(url))
                ):
                    storage_links_set = set(storage_links)
                    relationship_type = relationship.name.lower()
                    links = set(
                        callable(
                            **params.method_args(url, **kwargs),
                            **match.groupdict(),
                            **kwargs,
                        )
                    )
                    # Add new links to storage
                    for link in links - storage_links_set:
                        url_from, url_to = safe_url, link
                        if direction == Direction.TO:
                            url_from, url_to = url_to, url_from
                        self.storage.link(
                            url_from,
                            relationship_type,
                            url_to,
                        )
                    # Remove old links from storage
                    for deleted_link in storage_links_set - links:
                        url_from, url_to = safe_url, deleted_link
                        if direction == Direction.TO:
                            url_from, url_to = url_to, url_from
                        self.storage.unlink(
                            url_from,
                            relationship_type,
                            url_to,
                        )
                    return list(links)


# Force load all recap-core integrations
# This has to be below `functions` to prevent a circular import.
# This can go away once all integrations are entry-point plugins.
import recap.integrations


def create_catalog(
    url: str | None = None,
    registry: FunctionRegistry | None = None,
    **storage_opts,
) -> Catalog:
    """
    A helper that creates a catalog and its underlying storage layer.

    :param url: URL for the storage layer. If unset, the storage layer uses its
        default (A SQLite DB in `$RECAP_HOME/recap.db`).
    :param registry: A function registry containting metadata and relationship
        functions. If unset, the global registry is used.
    :param storage_opts: Extra options passed through to the storage layer.
    """

    from recap.storage import create_storage

    storage = create_storage(url, **storage_opts)
    return Catalog(storage, registry or global_registry)

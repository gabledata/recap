"""
Recap's REPL functions are a convenient way to interact with Recap in Python.
The functions behave similarly to Recap's [CLI](cli.md), but return Python
objects.
"""


import logging
from datetime import datetime

from recap.catalog import create_catalog
from recap.crawler import create_crawler
from recap.schema.model import Schema
from recap.storage.abstract import MetadataSubtype

catalog = create_catalog()
log = logging.getLogger(__name__)


def crawl(url: str, **kwargs):
    """
    Recursively crawl a URL and its children, storing metadata and
    relationships in Recap's data catalog.
    """

    create_crawler().crawl(url, **kwargs)


def ls(
    url: str,
    time: datetime | None = None,
    refresh: bool = False,
    **kwargs,
) -> list[str] | None:
    """
    List a URL's child URLs.

    :param url: URL to list children for.
    :param time: Time travel to see what a URL's children used to be.
    :param refresh: Skip Recap's catalog and read the latest data directly from
        the URL.
    :param kwargs: Arbitrary options passed to the crawler.
    :returns: A list of the input URL's children.
    """

    return catalog.ls(url, time, refresh, **kwargs)


def readers(
    url: str,
    time: datetime | None = None,
    refresh: bool = False,
    **kwargs,
) -> list[str] | None:
    """
    See what reads from a URL.

    :param url: URL to fetch readers for.
    :param time: Time travel to see what a URL's readers used to be.
    :param refresh: Skip Recap's catalog and read the latest data directly from
        the URL.
    :param kwargs: Arbitrary options passed to the catalog.
    :returns: A list URLs that read the input URL.
    """

    return catalog.readers(url, time, refresh, **kwargs)


def reads(
    url: str,
    time: datetime | None = None,
    refresh: bool = False,
    **kwargs,
) -> list[str] | None:
    """
    See what a URL reads. URLs must be accounts or jobs.

    :param url: URL to fetch readers for.
    :param time: Time travel to see what a URL used to read.
    :param refresh: Skip Recap's catalog and read the latest data directly from
        the URL.
    :param kwargs: Arbitrary options passed to the catalog.
    :returns: A list URLs that the input URL reads.
    """

    return catalog.reads(url, time, refresh, **kwargs)


def schema(
    url: str,
    time: datetime | None = None,
    refresh: bool = False,
    **kwargs,
) -> Schema | None:
    """
    Get a Recap schema for a URL.

    :param url: URL to fetch a schema for.
    :param time: Time travel to see what a URL's schema used to be.
    :param refresh: Skip Recap's catalog and read the latest data directly from
        the URL.
    :param kwargs: Arbitrary options passed to the catalog.
    :returns: A list URLs that the input URL reads.
    """

    return catalog.schema(url, time, refresh, **kwargs)


def search(
    query: str,
    metadata_type: type[MetadataSubtype],
    time: datetime | None = None,
) -> list[MetadataSubtype]:
    """
    Get a Recap schema for a URL.

    :param query: Query to apply to the storage layer. This is usually a
        `WHERE` clause, for example:
            `"url='file:///tmp/recap-test/foo/foo.json'"`
        or
            `"json_extract(metadata_obj, '$.fields.name') = 'email'"`
    :param metadata_type: The type of metadata to search for.
    :param time: Time travel to see what a URL's metadata used to be.
    :returns: A list metadata documents that match the search.
    """

    return catalog.search(query, metadata_type, time)


def writers(
    url: str,
    time: datetime | None = None,
    refresh: bool = False,
    **kwargs,
) -> list[str] | None:
    """
    See what writes to a URL.

    :param url: URL to fetch writers for.
    :param time: Time travel to see what a URL's writers used to be.
    :param refresh: Skip Recap's catalog and read the latest data directly from
        the URL.
    :param kwargs: Arbitrary options passed to the catalog.
    :returns: A list URLs that write to the input URL.
    """

    return catalog.writers(url, time, refresh, **kwargs)


def writes(
    url: str,
    time: datetime | None = None,
    refresh: bool = False,
    **kwargs,
) -> list[str] | None:
    """
    See what a URL writes. URLs must be accounts or jobs.

    :param url: URL to fetch writers for.
    :param time: Time travel to see where a URL's used to write.
    :param refresh: Skip Recap's catalog and read the latest data directly from
        the URL.
    :param kwargs: Arbitrary options passed to the catalog.
    :returns: A list URLs that the input URL writes to.
    """

    return catalog.writes(url, time, refresh, **kwargs)

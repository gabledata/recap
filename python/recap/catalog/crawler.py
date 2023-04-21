import logging

from recap.catalog.client import Client, create_client
from recap.converters.recap import RecapConverter
from recap.readers.readers import Reader, Registry, functions

log = logging.getLogger(__name__)


class Crawler:
    def __init__(
        self,
        client: Client,
        reader: Reader,
    ):
        self.client = client
        self.reader = reader

    def crawl(self, url: str, **kwargs):
        log.info("Beginning crawl root='%s'", url)
        url_stack = [url]

        while url_stack:
            url_to_crawl = url_stack.pop()
            log.info("Crawling path='%s'", url_to_crawl)

            if type_ := self.reader.schema(url_to_crawl):
                self.client.put_schema(url_to_crawl, type_)

            if links := self.reader.ls(url_to_crawl):
                url_stack.extend(links)

        log.info("Finished crawl root='%s'", url)


def create_crawler(
    url: str | None = None,
    converter: RecapConverter | None = None,
    registry: Registry | None = None,
    **httpx_opts,
) -> Crawler:
    client = create_client(url, converter, **httpx_opts)
    commands = Reader(registry or functions)
    return Crawler(client, commands)

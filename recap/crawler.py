from recap.registry import FunctionRegistry
from recap.registry import registry as global_registry
from recap.storage.abstract import AbstractStorage, Direction


class Crawler:
    def __init__(
        self,
        storage: AbstractStorage,
        registry: FunctionRegistry,
    ):
        self.storage = storage
        self.registry = registry or global_registry

    def crawl(self, url: str, **kwargs):
        url_stack = [url]

        while url_stack:
            url_to_crawl = url_stack.pop()

            for params, callable in self.registry.metadata_registry.items():
                if match := params.pattern.match(url_to_crawl):
                    try:
                        metadata = callable(
                            **params.method_args(url, **kwargs),
                            **match.groupdict(),
                            **kwargs,
                        )
                        self.storage.write(url_to_crawl, metadata)
                    except:
                        pass

            for params, callable in self.registry.relationship_registry.items():
                if match := params.pattern.match(url_to_crawl):
                    try:
                        links = set(
                            callable(
                                **params.method_args(url, **kwargs),
                                **match.groupdict(),
                                **kwargs,
                            )
                        )
                        storage_links = set(
                            self.storage.links(
                                url_to_crawl,
                                params.relationship,
                                direction=params.direction,
                            )
                        )

                        # Add new links
                        for new_url in links - storage_links:
                            url_from = url_to_crawl
                            url_to = new_url
                            if params.direction == Direction.TO:
                                url_from, url_to = url_to, url_from
                            self.storage.link(
                                url_from,
                                params.relationship,
                                url_to,
                            )

                        # Delete old links
                        for deleted_url in storage_links - links:
                            url_from = url_to_crawl
                            url_to = deleted_url
                            if params.direction == Direction.TO:
                                url_from, url_to = url_to, url_from
                            self.storage.unlink(url_from, params.relationship, url_to)

                        url_stack.extend(links)
                    except:
                        pass


# Force load all recap-core integrations
# This has to be below `functions` to prevent a circular import.
# This can go away once all integrations are entry-point plugins.
import recap.integrations


def create_crawler(
    url: str | None = None, registry: FunctionRegistry | None = None, **storage_opts
) -> Crawler:
    from recap.storage import create_storage

    storage = create_storage(url, **storage_opts)
    return Crawler(storage, registry or global_registry)

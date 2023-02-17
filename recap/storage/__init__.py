from recap.storage.abstract import AbstractStorage


def create_storage(url: str | None = None, **storage_opts) -> AbstractStorage:
    from recap.config import settings
    from recap.storage.db import DatabaseStorage
    from recap.storage.remote import RemoteStorage

    storage_settings = settings.storage_settings
    combined_opts = storage_settings.opts | storage_opts
    url = url or storage_settings.url

    if url.startswith("http"):
        return RemoteStorage(url, **combined_opts)

    return DatabaseStorage(url, **combined_opts)

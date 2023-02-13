import logging
from contextlib import contextmanager
from typing import Any, Generator

from fsspec import AbstractFileSystem, get_fs_token_paths

from recap.url import URL

from .abstract import AbstractBrowser

log = logging.getLogger(__name__)


class FilesystemBrowser(AbstractBrowser):
    """
    A browser that lists filesystem objects. FilesystemBrowser uses fsspec and
    its supported implementations
    (https://filesystem-spec.readthedocs.io/en/latest/api.html#built-in-implementations).

    FilesystemBrowser mirrors the directory structure in the filesystem.
    """

    def __init__(
        self,
        fs: AbstractFileSystem,
        base_path: str,
    ):
        self.fs = fs
        self.base_path = base_path
        self.url = URL(f"{fs.protocol}://{base_path}")

    def children(self, path: str) -> list[str] | None:
        absolute_path = self.base_path + path
        if self.fs.exists(absolute_path):
            if self.fs.isdir(absolute_path):
                return [
                    child["name"]
                    # Force detail=True because gcsfs doesn't honor defaults.
                    for child in self.fs.ls(absolute_path, detail=True)
                ]
            # Return empty since file exists but isn't a directory.
            return []


@contextmanager
def create_browser(
    url: str,
    storage_options: dict[str, Any] = {},
    **_,
) -> Generator[FilesystemBrowser, None, None]:
    """
    :param url: The URL to use for the filesystem. If the URL contains a path,
        the FilesystemBrowser will treat all paths relative to the URL path.
    :param storage_options: Storage options **kwargs to pass on to the fsspec
        filesystem constructor.
    """

    fs, _, paths = get_fs_token_paths(url, storage_options=storage_options)

    assert (
        len(paths) == 1
    ), f"Expected to get exactly 1 path from URL, but got paths={paths}"

    # Don't use DirFileSystem because it doesn't work properly with gcsfs.
    yield FilesystemBrowser(
        fs=fs,
        base_path=paths[0],
    )

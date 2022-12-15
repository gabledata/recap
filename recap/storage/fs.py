import fsspec
import json
from .abstract import AbstractStorage
from contextlib import contextmanager
from os.path import basename, join, normpath, relpath
from pathlib import PurePosixPath
from typing import Any, List, Generator
from urllib.parse import urlparse


class FilesystemStorage(AbstractStorage):
    def __init__(
        self,
        root: str,
        fs: fsspec.AbstractFileSystem,
    ):
        self.root = root
        self.fs = fs

    def touch(
        self,
        path: PurePosixPath,
    ):
        full_path = PurePosixPath(self.root, str(path).strip('/'))
        self.fs.mkdirs(full_path, exist_ok=True)

    def write(
        self,
        path: PurePosixPath,
        type: str,
        metadata: Any,
    ):
        full_path = PurePosixPath(self.root, str(path).strip('/'), f'{type}.json')
        if not self.fs.exists(full_path.parent):
            self.fs.mkdirs(full_path.parent, exist_ok=True)
        with self.fs.open(full_path, 'w+') as f:
            json.dump(metadata, f) # pyright: ignore [reportGeneralTypeIssues]

    def rm(
        self,
        path: PurePosixPath,
        type: str | None = None,
    ):
        full_path = PurePosixPath(self.root, str(path).strip('/'))
        if type:
            full_path = PurePosixPath(full_path, f"{type}.json")
        try:
            self.fs.rm(full_path, recursive=True)
        except FileNotFoundError:
            # File is already deleted
            # TODO Maybe we should raise a StorageException here?
            pass

    def ls(
        self,
        path: PurePosixPath,
    ) -> List[str] | None:
        try:
            # I thought there might be a vulnerability where users could
            # specify '/..' to read arbitrary parts of the filesystem, but
            # PurePosixPath seems to handle that properly, and stops at
            # self.root. So, just strip leading and trailing slashes to prevent
            # overriding self.root.
            dir = PurePosixPath(self.root, str(path).strip('/'))
            children = self.fs.ls(dir, detail=True) if self.fs.exists(dir) else []
            # Remove files since we're listing.
            children = filter(lambda c: c['type'] == 'directory', children)
            # Remove full path and only show immediate child names.
            children = map(lambda c: PurePosixPath(c['name']).name, children)
            return list(children)
        except FileNotFoundError:
            return None

    def read(
        self,
        path: PurePosixPath,
    ) -> dict[str, Any] | None:
        dir = PurePosixPath(self.root, str(path).strip('/'))
        doc = {}
        try:
            for child in self.fs.ls(dir, detail=True):
                if child['type'] == 'file':
                    # Strip .json from the filename and remove prefix path
                    type = PurePosixPath(child['name']).with_suffix('').name
                    with self.fs.open(child['name'], 'r') as f:
                        doc[type] = json.load(f)
            return doc
        except FileNotFoundError:
            # File is already deleted
            # TODO Maybe we should raise a StorageException here?
            return None


@contextmanager
def open(**config) -> Generator[FilesystemStorage, None, None]:
        url = urlparse(config['url'])
        storage_options = config.get('options', {})
        fs = fsspec.filesystem(
            url.scheme,
            **storage_options,
            # TODO This should move to the filesystem storage config
            auto_mkdir=True)
        yield FilesystemStorage(
            url.path,
            fs,
        )

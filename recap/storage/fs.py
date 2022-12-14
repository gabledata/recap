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

    def put_instance(self, infra: str, instance: str):
        dirname = join(
            self.root,
            'databases', infra,
            'instances', instance,
        )
        self.fs.mkdirs(dirname, exist_ok=True)

    def put_schema(self, infra: str, instance: str, schema: str):
        dirname = join(
            self.root,
            'databases', infra,
            'instances', instance,
            'schemas', schema,
        )
        self.fs.mkdirs(dirname, exist_ok=True)

    def put_table(self, infra: str, instance: str, schema: str, table: str):
        dirname = join(
            self.root,
            'databases', infra,
            'instances', instance,
            'schemas', schema,
            'tables', table,
        )
        self.fs.mkdirs(dirname, exist_ok=True)

    def put_view(self, infra: str, instance: str, schema: str, view: str):
        dirname = join(
            self.root,
            'databases', infra,
            'instances', instance,
            'schemas', schema,
            'views', view,
        )
        self.fs.mkdirs(dirname, exist_ok=True)

    def put_metadata(
        self,
        infra: str,
        instance: str,
        type: str,
        metadata: dict[str, Any],
        schema: str | None = None,
        table: str | None = None,
        view: str | None = None,
    ):
        # TODO this code is dupe'd all over
        dirname = join(
            self.root,
            'databases', infra,
            'instances', instance,
        )
        if schema:
            dirname = join(dirname, 'schemas', schema)
        if table:
            assert schema is not None, \
                "Schema must be set if putting table metadata"
            dirname = join(dirname, 'tables', table)
        elif view:
            assert schema is not None, \
                "Schema must be set if putting view metadata"
            dirname = join(dirname, 'views', view)
        dirname = join(dirname, 'metadata')
        filepath = join(dirname, f"{type}.json")
        if not self.fs.exists(dirname):
            self.fs.mkdirs(dirname, exist_ok=True)
        with self.fs.open(filepath, 'w') as f:
            json.dump(metadata, f) # pyright: ignore [reportGeneralTypeIssues]

    def remove_instance(self, infra: str, instance: str):
        dirname = join(
            self.root,
            'databases', infra,
            'instances', instance,
        )
        try:
            self.fs.rm(dirname, recursive=True)
        except FileNotFoundError:
            # File is already deleted
            # TODO Maybe we should raise a StorageException here?
            pass

    def remove_schema(self, infra: str, instance: str, schema: str):
        dirname = join(
            self.root,
            'databases', infra,
            'instances', instance,
            'schemas', schema,
        )
        try:
            self.fs.rm(dirname, recursive=True)
        except FileNotFoundError:
            # File is already deleted
            # TODO Maybe we should raise a StorageException here?
            pass

    def remove_table(self, infra: str, instance: str, schema: str, table: str):
        dirname = join(
            self.root,
            'databases', infra,
            'instances', instance,
            'schemas', schema,
            'tables', table,
        )
        try:
            self.fs.rm(dirname, recursive=True)
        except FileNotFoundError:
            # File is already deleted
            # TODO Maybe we should raise a StorageException here?
            pass

    def remove_view(self, infra: str, instance: str, schema: str, view: str):
        dirname = join(
            self.root,
            'databases', infra,
            'instances', instance,
            'schemas', schema,
            'views', view,
        )
        try:
            self.fs.rm(dirname, recursive=True)
        except FileNotFoundError:
            # File is already deleted
            # TODO Maybe we should raise a StorageException here?
            pass

    def remove_metadata(
        self,
        infra: str,
        instance: str,
        type: str,
        schema: str | None = None,
        table: str | None = None,
        view: str | None = None,
    ):
        # TODO this code is dupe'd all over
        dirname = join(
            self.root,
            'databases', infra,
            'instances', instance,
        )
        if schema:
            dirname = join(dirname, 'schemas', schema)
        if table:
            assert schema is not None, \
                "Schema must be set if putting table metadata"
            dirname = join(dirname, 'tables', table)
        elif view:
            assert schema is not None, \
                "Schema must be set if putting view metadata"
            dirname = join(dirname, 'views', view)
        dirname = join(dirname, 'metadata')
        filepath = join(dirname, f"{type}.json")
        try:
            self.fs.rm(filepath)
        except FileNotFoundError:
            # File is already deleted
            # TODO Maybe we should raise a StorageException here?
            pass

    def list(
        self,
        path: str
    ) -> List[str] | None:
        try:
            # I thought there might be a vulnerability where users could
            # specify '/..' to read arbitrary parts of the filesystem, but
            # PurePosixPath seems to handle that properly, and stops at
            # self.root. So, just strip leading and trailing slashes to prevent
            # overriding self.root.
            dir = PurePosixPath(self.root, path.strip("/"))
            children = self.fs.ls(dir, detail=False) if self.fs.exists(dir) else []
            # Remove full path and only show immediate child names.
            # Remove .json from the metadata JSON filenames.
            children = map(lambda m: basename(m.removesuffix('.json')), children)
            return list(children)
        except FileNotFoundError:
            return None

    def get_metadata(
        self,
        path: str,
        type: str,
    ) -> dict[str, str] | None:
        filepath = PurePosixPath(self.root, path, 'metadata', f"{type}.json")
        try:
            with self.fs.open(filepath, 'r') as f:
                return json.load(f)
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
            # TODO This should move to the filesyste storage config
            auto_mkdir=True)
        yield FilesystemStorage(
            url.path,
            fs,
        )

import fsspec
import json
from .abstract import TableStorage
from contextlib import contextmanager
from os.path import join
from typing import Any, List, Generator
from urllib.parse import urlparse


class FilesystemTableStorage(TableStorage):
    def __init__(
        self,
        infra: str,
        instance: str,
        root: str,
        fs: fsspec.AbstractFileSystem,
    ):
        super().__init__(infra, instance)
        self.root = root
        self.fs = fs

    def put_instance(self):
        dirname = join(
            self.root,
            'databases',self.infra,
            'instances', self.instance,
        )
        self.fs.mkdirs(dirname, exist_ok=True)

    def put_schema(self, schema: str):
        dirname = join(
            self.root,
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
        )
        self.fs.mkdirs(dirname, exist_ok=True)

    def put_table(self, schema: str, table: str):
        dirname = join(
            self.root,
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
            'tables', table,
        )
        self.fs.mkdirs(dirname, exist_ok=True)

    def put_view(self, schema: str, view: str):
        dirname = join(
            self.root,
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
            'views', view,
        )
        self.fs.mkdirs(dirname, exist_ok=True)

    def put_metadata(
        self,
        type: str,
        metadata: dict[str, Any],
        schema: str | None = None,
        table: str | None = None,
        view: str | None = None,
    ):
        # TODO this code is dupe'd all over
        dirname = join(
            self.root,
            'databases', self.infra,
            'instances', self.instance,
        )
        if schema:
            dirname = join(dirname, 'schemas', schema)
        if table:
            assert schema is not None, "Schema must be set if putting table metadata"
            dirname = join(dirname, 'tables', table)
        elif view:
            assert schema is not None, "Schema must be set if putting view metadata"
            dirname = join(dirname, 'views', view)
        dirname = join(dirname, 'metadata')
        filename = join(dirname, f"{type}.json")
        if not self.fs.exists(dirname):
            self.fs.mkdirs(dirname, exist_ok=True)
        with self.fs.open(filename, 'w') as f:
            json.dump(metadata, f) # pyright: ignore [reportGeneralTypeIssues]

    def remove_instance(self):
        dirname = join(
            self.root,
            'databases',self.infra,
            'instances', self.instance,
        )
        try:
            self.fs.rm(dirname, recursive=True)
        except FileNotFoundError:
            # File is already deleted
            # TODO Maybe we should raise a StorageException here?
            pass

    def remove_schema(self, schema: str):
        dirname = join(
            self.root,
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
        )
        try:
            self.fs.rm(dirname, recursive=True)
        except FileNotFoundError:
            # File is already deleted
            # TODO Maybe we should raise a StorageException here?
            pass

    def remove_table(self, schema: str, table: str):
        dirname = join(
            self.root,
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
            'tables', table,
        )
        try:
            self.fs.rm(dirname, recursive=True)
        except FileNotFoundError:
            # File is already deleted
            # TODO Maybe we should raise a StorageException here?
            pass

    def remove_view(self, schema: str, view: str):
        dirname = join(
            self.root,
            'databases', self.infra,
            'instances', self.instance,
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
        type: str,
        schema: str | None = None,
        table: str | None = None,
        view: str | None = None,
    ):
        # TODO this code is dupe'd all over
        dirname = join(
            self.root,
            'databases', self.infra,
            'instances', self.instance,
        )
        if schema:
            dirname = join(dirname, 'schemas', schema)
        if table:
            assert schema is not None, "Schema must be set if putting table metadata"
            dirname = join(dirname, 'tables', table)
        elif view:
            assert schema is not None, "Schema must be set if putting view metadata"
            dirname = join(dirname, 'views', view)
        dirname = join(dirname, 'metadata')
        filename = join(dirname, f"{type}.json")
        try:
            self.fs.rm(filename)
        except FileNotFoundError:
            # File is already deleted
            # TODO Maybe we should raise a StorageException here?
            pass

    def list_schemas(self) -> List[str]:
        dirname = join(
            self.root,
            'databases', self.infra,
            'instances', self.instance,
        )
        schemas = self.fs.ls(dirname, detail=False) if self.fs.exists(dirname) else []
        return list(filter(lambda p: p == 'metadata', schemas))

    def list_tables(self, schema: str) -> List[str]:
        dirname = join(
            self.root,
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
            'tables',
        )
        tables = self.fs.ls(dirname, detail=False) if self.fs.exists(dirname) else []
        return list(filter(lambda p: p == 'metadata', tables))

    def list_views(self, schema: str) -> List[str]:
        dirname = join(
            self.root,
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
            'views',
        )
        views = self.fs.ls(dirname, detail=False) if self.fs.exists(dirname) else []
        return list(filter(lambda p: p == 'metadata', views))

    def list_metadata(
        self,
        schema: str | None = None,
        table: str | None = None,
        view: str | None = None,
    ) -> List[str] | None:
        # TODO implement list_metadata
        raise NotImplementedError

    def get_metadata(
        self,
        type: str,
        schema: str | None = None,
        table: str | None = None,
        view: str | None = None,
    ) -> dict[str, str] | None:
        # TODO implement get_metadata
        raise NotImplementedError


@contextmanager
def open(**config) -> Generator[FilesystemTableStorage, None, None]:
        url = urlparse(config['url'])
        storage_options = config.get('options', {})
        yield FilesystemTableStorage(
            # TODO Remove hardcoding
            'postgresql',
            'sticker_space_dev',
            url.path,
            fsspec.filesystem(url.scheme, **storage_options, auto_mkdir=True),
        )

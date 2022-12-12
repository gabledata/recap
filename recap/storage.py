import fsspec
import httpx
import json
from abc import ABC, abstractmethod
from os.path import join
from typing import Any, List


class TableStorage(ABC):
    def __init__(self, infra, instance):
        self.infra = infra
        self.instance = instance

    @abstractmethod
    def put_instance(self):
        raise NotImplementedError

    @abstractmethod
    def put_schema(self, schema: str):
        raise NotImplementedError

    @abstractmethod
    def put_table(self, schema: str, table: str):
        raise NotImplementedError

    @abstractmethod
    def put_view(self, schema: str, view: str):
        raise NotImplementedError

    @abstractmethod
    def put_metadata(
        self,
        type: str,
        metadata: dict[str, Any],
        schema: str | None = None,
        table: str | None = None,
        view: str | None = None,
    ):
        raise NotImplementedError

    @abstractmethod
    def list_schemas(self) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    def list_tables(self, schema: str) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    def list_views(self, schema: str) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    def list_metadata(
        self,
        schema: str | None = None,
        table: str | None = None,
        view: str | None = None,
    ) -> List[str] | None:
        raise NotImplementedError

    @abstractmethod
    def get_metadata(
        self,
        type: str,
        schema: str | None = None,
        table: str | None = None,
        view: str | None = None,
    ) -> dict[str, str] | None:
        raise NotImplementedError


class RecapTableStorage(TableStorage):
    def __init__(
        self,
        infra: str,
        instance: str,
        client: httpx.Client,
    ):
        super().__init__(infra, instance)
        self.client = client

    def put_instance(self):
        self.client.put(join(
            'databases', self.infra,
            'instances', self.instance
        ))

    def put_schema(self, schema: str):
        self.client.put(join(
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema
        ))

    def put_table(self, schema: str, table: str):
        self.client.put(join(
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
            'tables', table
        ))

    def put_view(self, schema: str, view: str):
        self.client.put(join(
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
            'views', view
        ))

    def put_metadata(
        self,
        type: str,
        metadata: dict[str, Any],
        schema: str | None = None,
        table: str | None = None,
        view: str | None = None,
    ):
        # TODO this code is dupe'd all over
        path = join('databases', self.infra, 'instances', self.instance)
        if schema:
            path = join(path, 'schemas', schema)
        if table:
            assert schema is not None, "Schema must be set if putting table metadata"
            path = join(path, 'tables', table)
        elif view:
            assert schema is not None, "Schema must be set if putting view metadata"
            path = join(path, 'views', view)
        path = join(path, 'metadata', type)
        self.client.put(path, json=metadata)

    def list_schemas(self) -> List[str]:
        return self.client.get(join(
            'databases', self.infra,
            'instances', self.instance,
            'schemas'
        )).json()

    def list_tables(self, schema: str) -> List[str]:
        return self.client.get(join(
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
            'tables'
        )).json()

    def list_views(self, schema: str) -> List[str]:
        return self.client.get(join(
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
            'views'
        )).json()

    def list_metadata(
        self,
        schema: str | None = None,
        table: str | None = None,
        view: str | None = None,
    ) -> List[str] | None:
        # TODO this code is dupe'd all over
        path = join('databases', self.infra, 'instances', self.instance)
        if schema:
            path = join(path, 'schemas', schema)
        if table:
            assert schema is not None, "Schema must be set if putting table metadata"
            path = join(path, 'tables', table)
        elif view:
            assert schema is not None, "Schema must be set if putting view metadata"
            path = join(path, 'views', view)
        path = join(path, 'metadata')
        return self.client.get(path).json()

    def get_metadata(
        self,
        type: str,
        schema: str | None = None,
        table: str | None = None,
        view: str | None = None,
    ) -> dict[str, str] | None:
        # TODO this code is dupe'd all over
        path = join('databases', self.infra, 'instances', self.instance)
        if schema:
            path = join(path, 'schemas', schema)
        if table:
            assert schema is not None, "Schema must be set if putting table metadata"
            path = join(path, 'tables', table)
        elif view:
            assert schema is not None, "Schema must be set if putting view metadata"
            path = join(path, 'views', view)
        path = join(path, 'metadata', type)
        self.client.get(path).json()


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

    def list_schemas(self) -> List[str]:
        dirname = join(
            self.root,
            'databases', self.infra,
            'instances', self.instance,
        )
        schemas = self.fs.ls(dirname, detail=False)
        return list(filter(lambda p: p == 'metadata', schemas))

    def list_tables(self, schema: str) -> List[str]:
        dirname = join(
            self.root,
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
            'tables',
        )
        tables = self.fs.ls(dirname, detail=False)
        return list(filter(lambda p: p == 'metadata', tables))

    def list_views(self, schema: str) -> List[str]:
        dirname = join(
            self.root,
            'databases', self.infra,
            'instances', self.instance,
            'schemas', schema,
            'views',
        )
        views = self.fs.ls(dirname, detail=False)
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

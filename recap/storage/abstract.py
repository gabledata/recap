from abc import ABC, abstractmethod
from typing import Any, List


# TODO rename this to Storage, and make infra/instance method params
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
    def remove_instance(self):
        raise NotImplementedError

    @abstractmethod
    def remove_schema(self, schema: str):
        raise NotImplementedError

    @abstractmethod
    def remove_table(self, schema: str, table: str):
        raise NotImplementedError

    @abstractmethod
    def remove_view(self, schema: str, view: str):
        raise NotImplementedError

    @abstractmethod
    def remove_metadata(
        self,
        type: str,
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

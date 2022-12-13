from abc import ABC, abstractmethod
from typing import Any, List


class AbstractStorage(ABC):
    @abstractmethod
    def put_instance(self, infra: str, instance: str):
        raise NotImplementedError

    @abstractmethod
    def put_schema(self, infra: str, instance: str, schema: str):
        raise NotImplementedError

    @abstractmethod
    def put_table(self, infra: str, instance: str, schema: str, table: str):
        raise NotImplementedError

    @abstractmethod
    def put_view(self, infra: str, instance: str, schema: str, view: str):
        raise NotImplementedError

    @abstractmethod
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
        raise NotImplementedError

    @abstractmethod
    def remove_instance(self, infra: str, instance: str):
        raise NotImplementedError

    @abstractmethod
    def remove_schema(self, infra: str, instance: str, schema: str):
        raise NotImplementedError

    @abstractmethod
    def remove_table(self, infra: str, instance: str, schema: str, table: str):
        raise NotImplementedError

    @abstractmethod
    def remove_view(self, infra: str, instance: str, schema: str, view: str):
        raise NotImplementedError

    @abstractmethod
    def remove_metadata(
        self,
        infra: str,
        instance: str,
        type: str,
        schema: str | None = None,
        table: str | None = None,
        view: str | None = None,
    ):
        raise NotImplementedError

    @abstractmethod
    def list_schemas(self, infra: str, instance: str) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    def list_tables(self, infra: str, instance: str, schema: str) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    def list_views(self, infra: str, instance: str, schema: str) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    def list_metadata(
        self,
        infra: str,
        instance: str,
        schema: str | None = None,
        table: str | None = None,
        view: str | None = None,
    ) -> List[str] | None:
        raise NotImplementedError

    @abstractmethod
    def get_metadata(
        self,
        infra: str,
        instance: str,
        type: str,
        schema: str | None = None,
        table: str | None = None,
        view: str | None = None,
    ) -> dict[str, str] | None:
        raise NotImplementedError

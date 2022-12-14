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

    # TODO Maybe we should just take path: str here?
    @abstractmethod
    def list(
        self,
        path: str
    ) -> List[str] | None:
        raise NotImplementedError

    @abstractmethod
    def get_metadata(
        self,
        path: str,
        type: str,
    ) -> dict[str, str] | None:
        raise NotImplementedError

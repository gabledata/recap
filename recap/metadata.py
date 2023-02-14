from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from json import dumps, loads
from typing import Any, TypeVar

import dacite


@dataclass
class Metadata(ABC):
    def id(self) -> str | None:
        return None

    @classmethod
    @abstractmethod
    def key(cls) -> str:
        raise NotImplementedError

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return dumps(asdict(self))

    @classmethod
    def from_dict(
        cls: type[MetadataSubtype],
        obj: dict[str, Any],
    ) -> MetadataSubtype:
        return dacite.from_dict(data_class=cls, data=obj)

    @classmethod
    def from_json(
        cls: type[MetadataSubtype],
        json_str: str,
    ) -> MetadataSubtype:
        return cls.from_dict(loads(json_str))


MetadataSubtype = TypeVar("MetadataSubtype", bound=Metadata)

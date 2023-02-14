from __future__ import annotations

from dataclasses import dataclass

from recap.metadata import Metadata


@dataclass
class Schema(Metadata):
    fields: list[Field]

    @classmethod
    def key(cls) -> str:
        return "schemas"


@dataclass
class Field:
    name: str
    type: str | None = None
    default: str | None = None
    nullable: bool | None = None
    comment: str | None = None

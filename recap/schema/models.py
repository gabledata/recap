from __future__ import annotations

from pydantic import BaseModel, Extra, root_validator

from recap.schema.types import Parser


class Type(BaseModel, extra=Extra.allow):
    type: str | Type | list[str | Type]
    alias: str | None = None
    doc: str | None = None

    @root_validator
    def validate_recap_obj(cls, obj):
        # Parser throws exception if schema object is unparsable
        Parser().parse_obj(obj)
        return obj

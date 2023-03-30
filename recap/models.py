from __future__ import annotations

from pydantic import BaseModel, Extra, root_validator

from recap.converters.recap import CONVERTER


class Schema(BaseModel, extra=Extra.allow):
    type: str | Schema | list[str | Schema]
    alias: str | None = None
    doc: str | None = None

    @root_validator
    def validate_recap_obj(cls, obj):
        # Parser throws exception if schema object is unparsable
        CONVERTER.to_recap_type(obj)
        return obj


class Diff(BaseModel):
    """
    A difference for a Recap type attribute before and after it was changed.
    """

    path: list[str] = []
    before: Schema | None = None
    after: Schema | None = None

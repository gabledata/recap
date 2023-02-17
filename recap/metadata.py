"""
This module contains the core metadata models that Recap understands. All
models extend Pydantic's `BaseModel` class.

Right now, Recap's only metadata model is a Schema. Other entities, such as
accounts and jobs, are represented by URLs, but have no associated metadata.
"""

from __future__ import annotations

from pydantic import BaseModel


class Field(BaseModel):
    name: str
    """
    The name of a field.
    """

    type: str | None = None
    """
    A field's type.
    """

    default: str | None = None
    """
    A field's default value (represented as a string).
    """

    nullable: bool | None = None
    """
    Whether the field is nullable or not. If `False`, the field is required.
    """

    comment: str | None = None
    """
    A documentation comment for the field.
    """


class Schema(BaseModel):
    """
    Recap's representation of a Schema.
    """

    fields: list[Field]
    """
    Fields in the schema.
    """

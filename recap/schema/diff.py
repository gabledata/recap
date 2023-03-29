from __future__ import annotations

from dataclasses import dataclass, field, fields
from typing import Any

from recap.schema import types


@dataclass(kw_only=True)
class Diff:
    """
    A difference for a Recap type attribute before and after it was changed.
    """

    path: list[str] = field(default_factory=list)
    before: types.Type | None = None
    after: types.Type | None = None


def diff(
    type_a: types.Type | None, type_b: types.Type | None, path: list[str] = []
) -> list[Diff]:
    if type(type_a) != type(type_b) or not type_a or not type_b:
        return [Diff(path=path, before=type_a, after=type_b)]
    elif type_a != type_b:
        # Types are the same and both are non-null. Compare type fields.
        diffs = []
        for type_a_field in fields(type_a):
            type_a_val = getattr(type_a, type_a_field.name)
            type_b_val = getattr(type_b, type_a_field.name)
            match type_a_field.metadata.get(types.FIELD_METADATA_TYPE):
                case "type":
                    diffs.extend(
                        diff(type_a_val, type_b_val, path + [type_a_field.name])
                    )
                case "list[type]":
                    list_a_len = len(type_a_val)
                    list_b_len = len(type_b_val)
                    max_len = max(list_a_len, list_b_len)
                    for i in range(0, max_len):
                        list_a_subtype = type_a_val[i] if i < list_a_len else None
                        list_b_subtype = type_b_val[i] if i < list_b_len else None
                        diffs.extend(
                            diff(
                                list_a_subtype,
                                list_b_subtype,
                                path + [type_a_field.name, f"[{i}]"],
                            )
                        )
                case _ if type_a_val != type_b_val:
                    # Found a non-matching primitive attribute, so treat the entire type as different.
                    return [Diff(path=path, before=type_a, after=type_b)]
        return diffs
    return []

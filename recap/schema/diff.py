from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class Diff:
    """
    A difference for a Recap type attribute before and after it was changed.
    """

    before: Any = None
    after: Any = None


Diffs = dict[str | int, "Diff | Diffs"]
"""
A dictionary of diffs representing a diff between two Recap types.

String keys represent attribute names for a Recap type (e.g. "type", "alias",
etc). Integer keys represent indexes in a list if two lists of types are being
compared.
"""


def diff(obj_a: Any, obj_b: Any) -> Diffs | Diff:
    """
    Compare schema object A to schema object B and return the differences. For
    example, given obj_a and obj_b as:

    ```
    obj_a = {
        "type": "struct",
        "fields": [
            {
                "name": "field1",
                "type": "int32",
            },
            {
                "name": "field2",
                "type": "int64",
            },
        ],
    }

    obj_b = {
        "type": "struct",
        "fields": [
            {
                "name": "field1",
                "type": "int64",
            },
            {
                "name": "field2",
                "type": "int64",
            },
            {
                "name": "field3",
                "type": "list",
                "values": "string32",
            },
        ],
    }
    ```

    The returned diff would be:

    ```
    {'fields': {0: {'type': Diff(before='int32', after='int64')},
                2: {'name': Diff(before=None, after='field3'),
                    'type': Diff(before=None, after='list'),
                    'values': Diff(before=None, after='string32')}}}
    ```

    :param obj_a: A Recap type in object form.
    :param obj_a: A second Recap type in object form to compare to the first.
    :returns: The fields that were added, removed, or changed to get from obj_a
        to obj_b.
    """

    match obj_a, obj_b:
        case (dict(obj), None) | (None, dict(obj)):
            is_from = obj == obj_a
            return {
                attr_name: diff(obj[attr_name], None)
                if is_from
                else diff(None, obj[attr_name])
                for attr_name, attr in obj.items()
            }
        case (list(obj), None) | (None, list(obj)):
            is_from = obj == obj_a
            return {
                i: diff(obj[i], None) if is_from else diff(None, obj[i])
                for i in range(0, len(obj))
            }
        case type(type_a), type(type_b) if type_a != type_b:
            # If types don't match, just return the two objects.
            return Diff(before=obj_a, after=obj_b)
        case list(obj_a_list), list(obj_b_list):
            diffs = {}
            max_len = max(len(obj_a_list), len(obj_b_list))
            for i in range(0, max_len):
                list_a_attr = obj_a_list[i] if i < len(obj_a_list) else None
                list_b_attr = obj_b_list[i] if i < len(obj_b_list) else None
                if attr_diff := diff(list_a_attr, list_b_attr):
                    diffs[i] = attr_diff
            return diffs
        case dict(attr_a_dict), dict(attr_b_dict):
            diffs = {}
            for attr_name in attr_a_dict.keys() | attr_b_dict.keys():
                if attr_diff := diff(
                    attr_a_dict.get(attr_name),
                    attr_b_dict.get(attr_name),
                ):
                    diffs[attr_name] = attr_diff
            return diffs
        case attr_a, attr_b if attr_a != attr_b:
            return Diff(before=attr_a, after=attr_b)
    return {}

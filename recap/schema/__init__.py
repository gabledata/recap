from typing import Any

from recap.schema.converters.converter import Converter
from recap.schema.diff import diff as obj_diff


def convert(
    schema_obj: Any,
    from_type: str = "recap",
    to_type: str = "recap",
    **schema_args,
):
    match from_type, to_type:
        case "recap", str(to_type):
            return Converter.get_converter(to_type).from_recap_type(
                schema_obj,
                **schema_args,
            )
        case str(from_type), "recap":
            return Converter.get_converter(from_type).to_recap_type(
                schema_obj,
                **schema_args,
            )
        case (str(from_type), str(to_type)):
            from_converter = Converter.get_converter(from_type)
            intermediate_recap_type = from_converter.to_recap_type(
                schema_obj,
                **schema_args,
            )
            return Converter.get_converter(to_type).from_recap_type(
                intermediate_recap_type,
                **schema_args,
            )
        case _:
            raise ValueError(
                f"Got unknown from_type=`{from_type}` or to_type=`{to_type}`."
            )


def diff(
    schema_obj_a: Any,
    schema_obj_b: Any,
    obj_a_type: str,
    obj_b_type: str,
):
    # Convert to Recap types since diff'ing requires it.
    type_a = convert(schema_obj_a, from_type=obj_a_type)
    type_b = convert(schema_obj_b, from_type=obj_b_type)
    recap_diffs = obj_diff(type_a, type_b)
    diffs = {}
    for recap_diff in recap_diffs:
        # Silly hack to prevent fields.[1]
        path_str = ".".join(recap_diff.path).replace(".[", "[")
        # Convert types back to schema_obj_a's type.
        before = (
            convert(recap_diff.before, to_type=obj_a_type)
            if recap_diff.before
            else None
        )
        after = (
            convert(recap_diff.after, to_type=obj_a_type) if recap_diff.after else None
        )
        diffs[path_str] = {"before": before, "after": after}
    return diffs

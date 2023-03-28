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
    # Convert Recap types to Recap type objects.
    type_obj_a = convert(type_a, to_type="recap")
    type_obj_b = convert(type_b, to_type="recap")
    return obj_diff(type_obj_a, type_obj_b)

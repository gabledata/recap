from typing import Any

from recap.schema.converters.converter import Converter


def convert(
    schema_obj: Any,
    from_type: str = "recap",
    to_type: str = "recap",
    **schema_args,
):
    match from_type, to_type:
        case "recap", "recap":
            raise ValueError("Please set `from_type` or `to_type` argument.")
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

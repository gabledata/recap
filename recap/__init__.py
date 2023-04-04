from typing import Any

from recap.converters.recap import CONVERTER
from recap.models import Diff, Schema


def schema(url: str, **kwargs) -> Schema | None:
    from recap.readers.readers import Reader, functions

    if type_ := Reader(functions).schema(url, **kwargs):
        return Schema.parse_obj(CONVERTER.from_recap_type(type_))


def ls(url: str, **kwargs) -> list[str] | None:
    from recap.readers.readers import Reader, functions

    return Reader(functions).ls(url, **kwargs)


def diff(schema_a: Schema | None, schema_b: Schema | None) -> list[Diff]:
    from recap.differ import diff as _diff

    type_a = type_b = None

    if schema_a:
        type_a = CONVERTER.to_recap_type(schema_a.dict())
    if schema_b:
        type_b = CONVERTER.to_recap_type(schema_b.dict())

    return [
        Diff(
            path=".".join(type_diff.path).replace(".[", "["),
            before=Schema.parse_obj(CONVERTER.from_recap_type(type_diff.before))
            if type_diff.before
            else None,
            after=Schema.parse_obj(CONVERTER.from_recap_type(type_diff.after))
            if type_diff.after
            else None,
        )
        for type_diff in _diff(type_a, type_b)
    ]


def convert(
    schema_obj: Any,
    from_format: str | None = None,
    to_format: str | None = None,
) -> Any:
    from recap.converters.converter import Converter

    if from_format:
        from_converter = Converter.get_converter(from_format)
        schema_obj = from_converter.to_recap_type(schema_obj)
        schema_obj = (
            Schema.parse_obj(CONVERTER.from_recap_type(schema_obj))
            if not to_format
            else schema_obj
        )
    if to_format:
        to_converter = Converter.get_converter(to_format)
        schema_obj = (
            CONVERTER.to_recap_type(schema_obj.dict())
            if not from_format
            else schema_obj
        )
        schema_obj = to_converter.from_recap_type(schema_obj)
    return schema_obj

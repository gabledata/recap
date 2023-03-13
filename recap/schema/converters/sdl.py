from typing import Any

from recap.schema import types

ALIASES = {
    "int8": types.Int8(),
    "uint8": types.Uint8(),
    "int16": types.Int16(),
    "uint16": types.Uint16(),
    "int32": types.Int32(),
    "uint32": types.Uint32(),
    "int64": types.Int64(),
    "uint64": types.Uint64(),
    "float16": types.Float16(),
    "float32": types.Float32(),
    "float64": types.Float64(),
}


def to_recap_schema(
    sdl_type: dict[str, Any] | list | str,
    aliases: dict[str, types.Type] | None = None,
) -> types.Type:
    aliases = aliases or ALIASES
    match sdl_type:
        case {"type": "int", **rest}:
            return types.Int(
                bits=sdl_type["bits"],
                signed=sdl_type.get("signed", True),
            )
        case {"type": "float", **rest}:
            return types.Float(bits=sdl_type["bits"])
        case {"type": "string", **rest}:
            return types.String(
                bytes=sdl_type["bytes"],
                variable=sdl_type.get("variable", True),
            )
        case {"type": "bytes", **rest}:
            return types.Bytes(
                bytes=sdl_type["bytes"],
                variable=sdl_type.get("variable", True),
            )
        case {"type": "list", **rest}:
            return types.List(
                values=to_recap_schema(sdl_type["values"]),
                bits=sdl_type.get("bits"),
            )
        case {"type": "map", **rest}:
            return types.Map(
                keys=to_recap_schema(sdl_type["keys"]),
                values=to_recap_schema(sdl_type["values"]),
            )
        case {"type": "struct", **rest}:
            return types.Struct(
                fields=[
                    types.Field(
                        name=field.get("name"),
                        type_=to_recap_schema(field),
                        default=(
                            types.DefaultValue(value=field["default"])
                            if "default" in field
                            else None
                        ),
                    )
                    for field in sdl_type.get("fields", [])
                ]
            )
        case {"type": "enum", **rest}:
            return types.Enum(
                symbols=sdl_type["symbols"],
            )
        case list():
            # Treat the type as a union.
            return types.Union(
                types=[to_recap_schema(union_type) for union_type in sdl_type]
            )
        case {"type": alias, **rest} if alias in aliases:
            # Treat the type as an alias.
            return aliases[alias]
        case str(alias) if alias in aliases:
            # Treat the type as an alias.
            return aliases[alias]
        case type:
            raise ValueError(f"Can't parse Recap SDL for type={type}")

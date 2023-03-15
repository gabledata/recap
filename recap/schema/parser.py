from typing import Any

from recap.schema import types


class RecapTypeConverter:
    def __init__(self):
        self.aliases_seen = set()
        self.aliases_resolved: dict[str, tuple[type[types.Type], dict[str, Any]]] = {}
        self._load_built_in_aliases()

    def parse_obj(self, obj: dict[str, Any] | str) -> types.Type:
        if isinstance(obj, str):
            obj = {"type": obj}
        if alias := obj.get("alias"):
            self.aliases_seen.add(alias)
        match obj:
            case {"type": "null", **type_args}:
                type_ = types.Null(**type_args)
            case {"type": "bool", **type_args}:
                type_ = types.Bool(**type_args)
            case {"type": "int", **type_args}:
                type_ = types.Int(**type_args)
            case {"type": "float", **type_args}:
                type_ = types.Float(**type_args)
            case {"type": "string", **type_args}:
                type_ = types.String(**type_args)
            case {"type": "bytes", **type_args}:
                type_ = types.Bytes(**type_args)
            case {"type": "list", "values": values, **type_args}:
                type_ = types.List(values=self.parse_obj(values), **type_args)
            case {
                "type": "map",
                "keys": dict(keys),
                "values": dict(values),
                **type_args,
            }:
                type_ = types.Map(
                    keys=self.parse_obj(keys),
                    values=self.parse_obj(values),
                    **type_args,
                )
            case {"type": "struct", "fields": list(field_objs), **type_args}:
                fields = []
                for field_obj in field_objs:
                    field = types.Field(
                        name=field_obj.get("name"),
                        type_=self.parse_obj(field_obj),
                    )
                    if not field_obj.get("nullable", True):
                        # TODO Might end up with nested unions here. Should flatten.
                        field.type_ = types.Union(
                            types=[
                                types.Null(),
                                field.type_,
                            ]
                        )
                        field.default = types.DefaultValue(value=None)
                    if "default" in field_obj:
                        field.default = types.DefaultValue(
                            value=field_obj.get("default"),
                        )
                    fields.append(field)
                type_ = types.Struct(fields=fields, **type_args)
            case {"type": "enum", **type_args}:
                type_ = types.Enum(**type_args)
            case {"type": "union", "types": list(union_types), **type_args}:
                type_ = types.Union(
                    types=[self.parse_obj(union_type) for union_type in union_types],
                    **type_args,
                )
            case {"type": alias, **alias_args} if alias in self.aliases_seen:
                if class_and_base_args := self.aliases_resolved.get(alias):
                    cls, base_args = class_and_base_args
                    overridden_args = set(base_args.keys() & alias_args.keys())
                    if len(overridden_args) > 0:
                        raise ValueError(
                            "Attempted to override pre-defined "
                            f"attributes={overridden_args} for alias={alias}"
                        )
                    # Deep copy and remove alias since we've resolved an alias reference.
                    type_ = cls(**(base_args | alias_args))
                    # for type_arg, type_arg_value in alias_args.items():
                    #    setattr(type_, type_arg, type_arg_value)
                else:
                    # Cyclic reference detected. Use base type.
                    type_ = types.Type(alias=alias)
            case _:
                raise ValueError(f"Unable to resolve object={obj}")
        if alias := obj.get("alias"):
            # Save the type's arguments in aliases_resolved so we can
            # create new instances whenever we come across an alias reference.
            alias_args = type_.dict()
            # Pop the alias and doc attributes so derived types can override
            # these attributes.
            alias_args.pop("alias")
            alias_args.pop("doc")
            # TODO Should we pop "name" here for field aliases?
            # Not sure if I want to support field aliases.
            self.aliases_resolved[alias] = (type(type_), alias_args)
        return type_

    def dict(self):
        pass

    def _load_built_in_aliases(self):
        # Load built-in derived types into aliases_seen and aliases_resolved.
        # TODO There are more efficient wayts to do this.
        self.parse_obj(
            {
                "type": "int",
                "alias": "int8",
                "bits": 8,
                "signed": True,
            }
        )
        self.parse_obj(
            {
                "type": "int",
                "alias": "uint8",
                "bits": 8,
                "signed": False,
            }
        )
        self.parse_obj(
            {
                "type": "int",
                "alias": "int16",
                "bits": 16,
                "signed": True,
            }
        )
        self.parse_obj(
            {
                "type": "int",
                "alias": "uint16",
                "bits": 16,
                "signed": False,
            }
        )
        self.parse_obj(
            {
                "type": "int",
                "alias": "int32",
                "bits": 32,
                "signed": True,
            }
        )
        self.parse_obj(
            {
                "type": "int",
                "alias": "uint32",
                "bits": 32,
                "signed": False,
            }
        )
        self.parse_obj(
            {
                "type": "int",
                "alias": "int64",
                "bits": 64,
                "signed": True,
            }
        )
        self.parse_obj(
            {
                "type": "int",
                "alias": "uint64",
                "bits": 64,
                "signed": False,
            }
        )
        self.parse_obj({"type": "float", "alias": "float16", "bits": 16})
        self.parse_obj({"type": "float", "alias": "float32", "bits": 32})
        self.parse_obj({"type": "float", "alias": "float64", "bits": 64})
        self.parse_obj(
            {
                "type": "string",
                "alias": "string32",
                "bytes": 2_147_483_647,
            }
        )
        self.parse_obj(
            {
                "type": "string",
                "alias": "string64",
                "bytes": 9_223_372_036_854_775_807,
            }
        )
        self.parse_obj(
            {
                "type": "bytes",
                "alias": "bytes32",
                "bytes": 2_147_483_647,
            }
        )
        self.parse_obj(
            {
                "type": "bytes",
                "alias": "bytes64",
                "bytes": 9_223_372_036_854_775_807,
            }
        )
        self.parse_obj(
            {
                "type": "string",
                "alias": "uuid",
                "bytes": 36,
                "variable": False,
            }
        )
        self.parse_obj({"type": "bytes32", "alias": "decimal"})
        self.parse_obj(
            {
                "type": "bytes",
                "alias": "decimal128",
                "bytes": 16,
                "variable": False,
            }
        )
        self.parse_obj(
            {
                "type": "bytes",
                "alias": "decimal256",
                "bytes": 32,
                "variable": False,
            }
        )
        self.parse_obj({"type": "int64", "alias": "duration64"})
        self.parse_obj(
            {
                "type": "bytes",
                "alias": "interval128",
                "bytes": 16,
                "variable": False,
            }
        )
        self.parse_obj({"type": "int32", "alias": "time32"})
        self.parse_obj({"type": "int64", "alias": "time64"})
        self.parse_obj({"type": "int64", "alias": "timestamp64"})
        self.parse_obj({"type": "int32", "alias": "date32"})
        self.parse_obj({"type": "int64", "alias": "date64"})

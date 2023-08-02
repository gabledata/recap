import json
import os
from curses import meta

import pytest

# import requests
from jsonschema import ValidationError, validate

from recap.types import (
    ListType,
    MapType,
    ProxyType,
    RecapType,
    RecapTypeRegistry,
    StructType,
    UnionType,
    clean_dict,
    from_dict,
)


def get_meta_schema() -> dict:
    # response = requests.get("https://recap.build/specs/type/0.2.0.json")
    # response.raise_for_status()
    # return response.json()
    with open("tests/0.2.0.json", "r") as file:
        return json.load(file)


valid_schema_dir = "tests/spec/valid"
valid_schema_paths = [
    os.path.join(valid_schema_dir, file)
    for file in os.listdir(valid_schema_dir)
    if file.endswith(".json")
]
invalid_schema_dir = "tests/spec/invalid"
invalid_schema_paths = [
    os.path.join(invalid_schema_dir, file)
    for file in os.listdir(invalid_schema_dir)
    if file.endswith(".json") and not file.endswith(".unimplemented.json")
]
unimplemented_invalid_schema_paths = [
    os.path.join(invalid_schema_dir, file)
    for file in os.listdir(invalid_schema_dir)
    if file.endswith(".unimplemented.invalid.json")
]


@pytest.mark.parametrize("schema_path", valid_schema_paths)
def test_valid_schemas(schema_path):
    meta_schema = get_meta_schema()
    with open(schema_path, "r") as file:
        schema = json.load(file)
        validate(schema, meta_schema)  # throws ValidationError if invalid
        registry = RecapTypeRegistry()
        recap_type = from_dict(
            clean_dict(schema), registry
        )  # throws ValueError if invalid
        recap_type.alias = "test.alias"
        registry.register_alias(recap_type)
        check_aliases(
            recap_type, registry
        )  # throws ValueError or Type Error if invalid


@pytest.mark.parametrize("schema_path", invalid_schema_paths)
def test_invalid_schemas(schema_path):
    meta_schema = get_meta_schema()
    with open(schema_path, "r") as file:
        schema = json.load(file)
        with pytest.raises(ValidationError):
            validate(schema, meta_schema)
        with pytest.raises((ValueError, TypeError)):
            registry = RecapTypeRegistry()
            recap_type = from_dict(clean_dict(schema), registry)
            registry.register_alias(recap_type)
            check_aliases(recap_type, registry)


# If the JSON Schema were more accurate with the recap spec, we would expect these files to be invalid recap specs,
# so validate() would throw. Mark these tests as expected to fail until implemented.
@pytest.mark.xfail(strict=True)
@pytest.mark.parametrize("schema_path", unimplemented_invalid_schema_paths)
def test_unimplemented_invalid_schemas(schema_path):
    meta_schema = get_meta_schema()
    with open(schema_path, "r") as file:
        schema = json.load(file)
        with pytest.raises(ValidationError):
            validate(schema, meta_schema)


# To ensure that the invalid test cases not implemented in the JSON Schema are actually invalid, we need to make sure
# that recap rejects them
@pytest.mark.parametrize("schema_path", unimplemented_invalid_schema_paths)
def test_recap_rejects_unimplemented_invalid_schemas(schema_path):
    with open(schema_path, "r") as file:
        schema = json.load(file)
        with pytest.raises(ValueError):
            registry = RecapTypeRegistry()
            recap_type = from_dict(clean_dict(schema), registry)
            registry.register_alias(recap_type)
            check_aliases(recap_type, registry)


def check_aliases(
    recap_type: RecapType,
    registry: RecapTypeRegistry,
):
    # If this is a proxy type, make sure its alias exists in the type registry
    if type(recap_type) == ProxyType:
        try:
            registry.from_alias(recap_type.alias)
        except TypeError as e:
            raise ValueError(f"Unrecognized type or alias '{recap_type.alias}'")
    elif type(recap_type) == StructType:
        for field in recap_type.fields:
            check_aliases(field, registry)
    elif type(recap_type) == UnionType:
        for field in recap_type.types:
            check_aliases(field, registry)
    elif type(recap_type) == ListType:
        check_aliases(recap_type.values, registry)
    elif type(recap_type) == MapType:
        check_aliases(recap_type.keys, registry)
        check_aliases(recap_type.values, registry)

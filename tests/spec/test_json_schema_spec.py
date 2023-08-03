import json
import os

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


@pytest.fixture(scope="session")
def meta_schema() -> dict:
    # response = requests.get("https://recap.build/specs/type/0.2.0.json")
    # response.raise_for_status()
    # return response.json()
    with open("tests/0.2.0.json", "r") as file:
        return json.load(file)


VALID_SCHEMA_DIR = "tests/spec/valid"
INVALID_SCHEMA_DIR = "tests/spec/invalid"

valid_schema_paths = [
    os.path.join(VALID_SCHEMA_DIR, file)
    for file in os.listdir(VALID_SCHEMA_DIR)
    if file.endswith(".json")
]
invalid_schema_paths = [
    os.path.join(INVALID_SCHEMA_DIR, file)
    for file in os.listdir(INVALID_SCHEMA_DIR)
    if file.endswith(".json") and not file.endswith(".unimplemented.json")
]
unimplemented_invalid_schema_paths = [
    os.path.join(INVALID_SCHEMA_DIR, file)
    for file in os.listdir(INVALID_SCHEMA_DIR)
    if file.endswith(".unimplemented.invalid.json")
]


@pytest.mark.parametrize("schema_path", valid_schema_paths)
def test_valid_schemas(schema_path: str, meta_schema: dict):
    with open(schema_path, "r") as file:
        schema = json.load(file)
        # throws ValidationError if invalid
        validate(schema, meta_schema)
        registry = RecapTypeRegistry()
        # throws ValueError if invalid
        recap_type = from_dict(clean_dict(schema), registry)
        # throws ValueError or TypeError if invalid
        check_aliases(recap_type, registry)


@pytest.mark.parametrize("schema_path", invalid_schema_paths)
def test_invalid_schemas(schema_path: str, meta_schema: dict):
    with open(schema_path, "r") as file:
        schema = json.load(file)
        with pytest.raises(ValidationError):
            validate(schema, meta_schema)
        with pytest.raises((ValueError, TypeError)):
            registry = RecapTypeRegistry()
            recap_type = from_dict(clean_dict(schema), registry)
            check_aliases(recap_type, registry)


# If the JSON Schema were more accurate with the recap spec, we would expect
# these files to be invalid recap specs, so validate() would throw. Mark these
# tests as expected to fail until implemented.
@pytest.mark.xfail(strict=True)
@pytest.mark.parametrize("schema_path", unimplemented_invalid_schema_paths)
def test_unimplemented_invalid_schemas(schema_path: str, meta_schema: dict):
    with open(schema_path, "r") as file:
        schema = json.load(file)
        with pytest.raises(ValidationError):
            validate(schema, meta_schema)


# To ensure that the invalid test cases not implemented in the JSON Schema are
# actually invalid, we need to make sure that recap rejects them.
@pytest.mark.parametrize("schema_path", unimplemented_invalid_schema_paths)
def test_recap_rejects_unimplemented_invalid_schemas(schema_path: str):
    with open(schema_path, "r") as file:
        schema = json.load(file)
        with pytest.raises(ValueError):
            registry = RecapTypeRegistry()
            recap_type = from_dict(clean_dict(schema), registry)
            check_aliases(recap_type, registry)


def check_aliases(
    recap_type: RecapType,
    registry: RecapTypeRegistry,
):
    # If this is a proxy type, make sure its alias exists in the type registry
    match recap_type:
        case ProxyType():
            try:
                registry.from_alias(recap_type.alias)
            except TypeError as e:
                raise ValueError(f"Unrecognized type or alias '{recap_type.alias}'")
        case StructType(fields=fields):
            for field in fields:
                check_aliases(field, registry)
        case UnionType(types=types):
            for field in types:
                check_aliases(field, registry)
        case ListType(values=values):
            check_aliases(values, registry)
        case MapType(keys=keys, values=values):
            check_aliases(keys, registry)
            check_aliases(values, registry)

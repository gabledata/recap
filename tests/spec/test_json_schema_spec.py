import json
import os
from typing import Callable

import pytest
import requests
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

VALID_SCHEMA_DIR = "tests/spec/valid"
INVALID_SCHEMA_DIR = "tests/spec/invalid"
RECAP_SPEC_JSON_HTTP = "https://recap.build/specs/type/0.3.0.json"


@pytest.fixture(scope="session")
def meta_schema() -> dict:
    if local_file := os.environ.get("RECAP_SPEC_JSON_LOCAL"):
        with open(local_file) as f:
            return json.load(f)
    else:
        response = requests.get(RECAP_SPEC_JSON_HTTP)
        response.raise_for_status()
        return response.json()


def get_schemas(schema_dir: str, schema_filter: Callable[[str], bool]):
    schemas = []
    for file in os.listdir(schema_dir):
        if schema_filter(file):
            path = os.path.join(schema_dir, file)
            with open(path) as f:
                file_schemas = json.load(f)
                for schema in file_schemas:
                    schemas.append((file, schema))
    return schemas


@pytest.mark.parametrize(
    "filename,schema",
    get_schemas(VALID_SCHEMA_DIR, lambda file: file.endswith(".json")),
)
def test_valid_schemas(filename: str, schema: dict, meta_schema: dict):
    # throws ValidationError if invalid
    validate(schema, meta_schema)
    registry = RecapTypeRegistry()
    # throws ValueError if invalid
    recap_type = from_dict(clean_dict(schema), registry)
    # throws ValueError or TypeError if invalid
    check_aliases(recap_type, registry)


@pytest.mark.parametrize(
    "filename,schema",
    get_schemas(
        INVALID_SCHEMA_DIR,
        lambda file: file.endswith(".json")
        and not file.endswith(".unimplemented.json"),
    ),
)
def test_invalid_schemas(filename: str, schema: dict, meta_schema: dict):
    try:
        validate(schema, meta_schema)
        is_valid_metaschema = True
    except ValidationError:
        # Got a validation error, so the metaschema is invalid.
        # The test should pass, since we're expecting an invalid schema.
        is_valid_metaschema = False

    # metaschema passed, so aliases should fail
    if is_valid_metaschema:
        registry = RecapTypeRegistry()
        # from_dict should parse since `validate` passed
        recap_type = from_dict(clean_dict(schema), registry)
        # aliases should fail, since we're expecting an invalid schema
        # and the metaschema is valid.
        with pytest.raises((ValueError, TypeError)):
            check_aliases(recap_type, registry)


# If the JSON Schema were more accurate with the recap spec, we would expect
# these files to be invalid recap specs, so validate() would throw. Mark these
# tests as expected to fail until implemented.
@pytest.mark.xfail(strict=True)
@pytest.mark.parametrize(
    "filename,schema",
    get_schemas(
        INVALID_SCHEMA_DIR,
        lambda file: file.endswith(".unimplemented.json"),
    ),
)
def test_unimplemented_invalid_schemas(
    filename: str,
    schema: dict,
    meta_schema: dict,
):
    with pytest.raises(ValidationError):
        validate(schema, meta_schema)


# To ensure that the invalid test cases not implemented in the JSON Schema are
# actually invalid, we need to make sure that recap rejects them.
@pytest.mark.parametrize(
    "filename,schema",
    get_schemas(
        INVALID_SCHEMA_DIR,
        lambda file: file.endswith(".unimplemented.json"),
    ),
)
def test_recap_rejects_unimplemented_invalid_schemas(
    filename: str,
    schema: dict,
    meta_schema: dict,
):
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
            except TypeError:
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

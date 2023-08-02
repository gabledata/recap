import json
import os
from curses import meta

import pytest

# import requests
from jsonschema import ValidationError, validate


def get_meta_schema() -> dict:
    # response = requests.get("https://recap.build/specs/type/0.2.0.json")
    # response.raise_for_status()
    # return response.json()
    with open("tests/0.2.0.json", "r") as file:
        return json.load(file)


schema_dir = "tests/spec"
valid_schema_paths = [
    os.path.join(schema_dir, file)
    for file in os.listdir(schema_dir)
    if file.endswith(".json") and not file.endswith(".invalid.json")
]
invalid_schema_paths = [
    os.path.join(schema_dir, file)
    for file in os.listdir(schema_dir)
    if file.endswith(".invalid.json")
    and not file.endswith(".unimplemented.invalid.json")
]
unimplemented_invalid_schema_paths = [
    os.path.join(schema_dir, file)
    for file in os.listdir(schema_dir)
    if file.endswith(".unimplemented.invalid.json")
]


@pytest.mark.parametrize("schema_path", valid_schema_paths)
def test_valid_schemas(schema_path):
    meta_schema = get_meta_schema()
    with open(schema_path, "r") as file:
        schema = json.load(file)
        validate(schema, meta_schema)  # throws ValidationError if invalid


@pytest.mark.parametrize("schema_path", invalid_schema_paths)
def test_invalid_schemas(schema_path):
    meta_schema = get_meta_schema()
    with open(schema_path, "r") as file:
        schema = json.load(file)
        with pytest.raises(ValidationError):
            validate(schema, meta_schema)


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

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


valid_schema_dir = "tests/spec"
valid_schema_paths = [
    os.path.join(valid_schema_dir, file)
    for file in os.listdir(valid_schema_dir)
    if file.endswith(".json")
]


@pytest.mark.parametrize("schema_path", valid_schema_paths)
def test_valid_schemas(schema_path):
    meta_schema = get_meta_schema()
    with open(schema_path, "r") as file:
        schema = json.load(file)
        validate(schema, meta_schema)  # throws ValidationError if invalid

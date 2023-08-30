from unittest.mock import patch

from fastapi.testclient import TestClient

from recap.gateway import app
from recap.types import IntType, StructType

client = TestClient(app)


@patch("recap.commands.ls")
def test_ls_root(mock_ls):
    mock_ls.return_value = ["foo", "bar"]
    response = client.get("/ls")
    assert response.status_code == 200
    assert response.json() == mock_ls.return_value


@patch("recap.commands.ls")
def test_ls_subpath(mock_ls):
    mock_ls.return_value = ["foo", "bar"]
    response = client.get("/ls/bar")
    assert response.status_code == 200
    assert response.json() == mock_ls.return_value


@patch("recap.commands.schema")
def test_schema(mock_schema):
    mock_schema.return_value = StructType([IntType(bits=32)])
    response = client.get("/schema/foo")
    expected = {"type": "struct", "fields": ["int32"]}
    assert response.status_code == 200
    assert response.json() == expected

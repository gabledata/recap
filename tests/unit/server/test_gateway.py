from unittest.mock import patch

from fastapi.testclient import TestClient

from recap.server.gateway import router

client = TestClient(router)


@patch("recap.commands.ls")
def test_ls_root(mock_ls):
    mock_ls.return_value = ["foo", "bar"]
    response = client.get("/gateway/ls")
    assert response.status_code == 200
    assert response.json() == mock_ls.return_value


@patch("recap.commands.ls")
def test_ls_subpath(mock_ls):
    mock_ls.return_value = ["foo", "bar"]
    response = client.get("/gateway/ls/bar")
    assert response.status_code == 200
    assert response.json() == mock_ls.return_value


@patch("recap.commands.schema")
def test_schema(mock_schema):
    mock_schema.return_value = {"type": "struct", "fields": ["int32"]}
    response = client.get("/gateway/schema/foo")
    expected = {"type": "struct", "fields": ["int32"]}
    assert response.status_code == 200
    assert response.json() == expected

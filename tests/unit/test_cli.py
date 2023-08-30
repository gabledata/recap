from json import loads
from unittest.mock import patch

from typer.testing import CliRunner

from recap.cli import app
from recap.types import IntType, StructType

runner = CliRunner()


@patch("recap.commands.ls")
def test_ls_root(mock_ls):
    mock_ls.return_value = ["foo", "bar"]
    result = runner.invoke(app, ["ls"])
    assert result.exit_code == 0
    assert loads(result.stdout) == mock_ls.return_value


@patch("recap.commands.ls")
def test_ls_subpath(mock_ls):
    mock_ls.return_value = ["foo", "bar"]
    result = runner.invoke(app, ["ls", "/foo/bar"])
    assert result.exit_code == 0
    assert loads(result.stdout) == mock_ls.return_value


@patch("recap.commands.schema")
def test_schema(mock_schema):
    mock_schema.return_value = StructType([IntType(bits=32)])
    result = runner.invoke(app, ["schema", "foo"])
    assert result.exit_code == 0
    assert loads(result.stdout) == {"type": "struct", "fields": ["int32"]}

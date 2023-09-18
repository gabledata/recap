import os
import tempfile
from json import loads
from unittest.mock import patch

from typer.testing import CliRunner

from recap.cli import app
from recap.types import IntType, StructType, to_dict

runner = CliRunner()


class TestCli:
    @classmethod
    def setup_class(cls):
        # Create a temporary .env file for the test
        cls.temp_file = tempfile.NamedTemporaryFile(delete=False)
        cls.temp_file_name = cls.temp_file.name

    @classmethod
    def teardown_class(cls):
        # Delete the temporary .env file
        os.unlink(cls.temp_file_name)

    @patch("recap.commands.ls")
    def test_ls_root(self, mock_ls):
        mock_ls.return_value = ["foo", "bar"]
        result = runner.invoke(app, ["ls"])
        assert result.exit_code == 0
        assert loads(result.stdout) == mock_ls.return_value

    @patch("recap.commands.ls")
    def test_ls_subpath(self, mock_ls):
        mock_ls.return_value = ["foo", "bar"]
        result = runner.invoke(app, ["ls", "/foo/bar"])
        assert result.exit_code == 0
        assert loads(result.stdout) == mock_ls.return_value

    @patch("recap.commands.schema")
    def test_schema(self, mock_schema):
        mock_schema.return_value = to_dict(StructType([IntType(bits=32)]))
        result = runner.invoke(app, ["schema", "foo"])
        assert result.exit_code == 0
        assert loads(result.stdout) == {"type": "struct", "fields": ["int32"]}

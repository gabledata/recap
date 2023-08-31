import os
import tempfile
from importlib import reload
from json import loads
from unittest.mock import patch

from pydantic import AnyUrl
from typer.testing import CliRunner

import recap.settings
from recap.cli import app
from recap.types import IntType, StructType

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
        mock_schema.return_value = StructType([IntType(bits=32)])
        result = runner.invoke(app, ["schema", "foo"])
        assert result.exit_code == 0
        assert loads(result.stdout) == {"type": "struct", "fields": ["int32"]}

    def test_add_remove(self):
        with patch.dict(os.environ, {"RECAP_CONFIG": self.temp_file_name}, clear=False):
            # Reload after patching to reset CONFIG_FILE
            reload(recap.settings)
            from recap.settings import RecapSettings

            # Test set_config
            url = AnyUrl("scheme://user:pw@localhost:1234/db")
            result = runner.invoke(app, ["add", "test_system", url.unicode_string()])
            assert result.exit_code == 0
            assert result.stdout == ""
            assert RecapSettings().systems.get("test_system") == url

            # Test unset_config
            result = runner.invoke(app, ["remove", "test_system"])
            assert result.exit_code == 0
            assert result.stdout == ""
            assert RecapSettings().systems.get("test_system") is None

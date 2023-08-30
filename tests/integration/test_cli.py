from json import loads

import psycopg2
import pytest
from typer.testing import CliRunner

from recap.cli import app

runner = CliRunner()


class TestCli:
    @classmethod
    def setup_class(cls):
        # Connect to the PostgreSQL database
        cls.connection = psycopg2.connect(
            host="localhost",
            port="5432",
            user="postgres",
            password="password",
            dbname="testdb",
        )

        # Create tables
        cursor = cls.connection.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS test_types (test_integer INTEGER);")
        cls.connection.commit()

    @classmethod
    def teardown_class(cls):
        # Delete tables
        cursor = cls.connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS test_types;")
        cls.connection.commit()

        # Close the connection
        cls.connection.close()

    @pytest.mark.parametrize(
        "args",
        [
            ["ls"],
            ["ls", "/"],
        ],
    )
    def test_ls_root(self, args: list[str]):
        result = runner.invoke(app, args)
        assert result.exit_code == 0
        assert loads(result.stdout) == ["pg"]

    @pytest.mark.parametrize(
        "args",
        [
            ["ls", "pg"],
            ["ls", "/pg"],
        ],
    )
    def test_ls_subpath(self, args: list[str]):
        result = runner.invoke(app, args)
        assert result.exit_code == 0
        assert loads(result.stdout) == ["postgres", "template0", "template1", "testdb"]

    def test_schema(self):
        result = runner.invoke(app, ["schema", "pg/testdb/public/test_types"])
        assert result.exit_code == 0
        assert loads(result.stdout) == {
            "type": "struct",
            "fields": [{"type": "int32", "name": "test_integer", "optional": True}],
        }

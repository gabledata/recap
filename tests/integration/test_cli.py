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
        "cmd, url, expected",
        [
            ["ls", "", ["postgresql://localhost:5432/testdb"]],
            [
                "ls",
                "postgresql://postgres:password@localhost:5432",
                ["postgres", "template0", "template1", "testdb"],
            ],
            [
                "ls",
                "postgresql://postgres:password@localhost:5432/testdb",
                [
                    "pg_toast",
                    "pg_catalog",
                    "public",
                    "information_schema",
                ],
            ],
            [
                "ls",
                "postgresql://postgres:password@localhost:5432/testdb/",
                [
                    "pg_toast",
                    "pg_catalog",
                    "public",
                    "information_schema",
                ],
            ],
            ["ls", "postgresql://localhost:5432/testdb/public", ["test_types"]],
        ],
    )
    def test_ls(self, cmd, url, expected):
        result = runner.invoke(app, [cmd, url])
        assert result.exit_code == 0
        assert loads(result.stdout) == expected

    def test_schema(self):
        result = runner.invoke(
            app,
            [
                "schema",
                "postgresql://localhost:5432/testdb/public/test_types",
            ],
        )
        assert result.exit_code == 0
        assert loads(result.stdout) == {
            "type": "struct",
            "fields": [{"type": "int32", "name": "test_integer", "optional": True}],
        }

    def test_schema_avro(self):
        result = runner.invoke(
            app,
            [
                "schema",
                "postgresql://localhost:5432/testdb/public/test_types",
                "-of=avro",
            ],
        )
        assert result.exit_code == 0
        assert loads(result.stdout) == {
            "type": "record",
            "fields": [
                {"name": "test_integer", "default": None, "type": ["null", "int"]}
            ],
        }

    def test_schema_json(self):
        result = runner.invoke(
            app,
            [
                "schema",
                "postgresql://localhost:5432/testdb/public/test_types",
                "-of=json",
            ],
        )
        assert result.exit_code == 0
        assert loads(result.stdout) == {
            "type": "object",
            "properties": {"test_integer": {"default": None, "type": "integer"}},
        }

    @pytest.mark.skip(reason="Enable when #397 is fixed")
    def test_schema_protobuf(self):
        result = runner.invoke(
            app,
            [
                "schema",
                "postgresql://localhost:5432/testdb/public/test_types",
                "-of=protobuf",
            ],
        )
        assert result.exit_code == 0
        assert (
            result.stdout
            == """
TODO: Some proto schema
"""
        )

    def test_schema_recap(self):
        result = runner.invoke(
            app,
            [
                "schema",
                "postgresql://localhost:5432/testdb/public/test_types",
                "-of=recap",
            ],
        )
        assert result.exit_code == 0
        assert loads(result.stdout) == {
            "type": "struct",
            "fields": [{"type": "int32", "name": "test_integer", "optional": True}],
        }

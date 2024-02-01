import threading
import time

import httpx
import psycopg2
import pytest
from uvicorn import Server
from uvicorn.config import Config

from recap.server.app import app

client = httpx.Client(follow_redirects=True, base_url="http://localhost:8000/gateway")


class TestGateway:
    server = None
    server_thread = None

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

        # Start FastAPI app
        config = Config(app=app, host="127.0.0.1", port=8000)
        cls.server = Server(config=config)
        cls.server_thread = threading.Thread(target=cls.server.run)
        cls.server_thread.start()
        cls.server.server_state

        # Wait for the server to start
        checks = 0
        while cls.server.started is False and checks < 10:
            time.sleep(1)
            checks += 1
        if cls.server.started is False:
            raise RuntimeError("Unable to start gateway server after 10 seconds")

    @classmethod
    def teardown_class(cls):
        # Delete tables
        cursor = cls.connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS test_types;")
        cls.connection.commit()

        # Close the connection
        cls.connection.close()

        # Stop the FastAPI app
        if server := cls.server:
            server.should_exit = True
        if server_thread := cls.server_thread:
            server_thread.join(10)
            if server_thread.is_alive():
                raise RuntimeError("Unable to shutdown gateway server after 10 seconds")

    def test_ls_root(self):
        response = client.get("/ls")
        assert response.status_code == 200
        assert response.json() == [
            "postgresql://localhost:5432/testdb",
            "sqlite:///file:mem1?mode=memory&cache=shared&uri=true",
        ]

    def test_ls_subpath(self):
        response = client.get("/ls/postgresql://localhost:5432/testdb")
        assert response.status_code == 200
        assert response.json() == [
            "pg_toast",
            "pg_catalog",
            "public",
            "information_schema",
        ]

    def test_schema(self):
        response = client.get(
            "/schema/postgresql://localhost:5432/testdb/public/test_types"
        )
        assert response.status_code == 200
        assert response.json() == {
            "type": "struct",
            "fields": [{"name": "test_integer", "type": "int32", "optional": True}],
        }

    def test_schema_avro(self):
        response = client.get(
            "/schema/postgresql://localhost:5432/testdb/public/test_types",
            headers={"Content-Type": "application/avro+json"},
        )
        assert response.status_code == 200
        assert response.json() == {
            "type": "record",
            "fields": [
                {"name": "test_integer", "default": None, "type": ["null", "int"]}
            ],
        }

    def test_schema_json(self):
        response = client.get(
            "/schema/postgresql://localhost:5432/testdb/public/test_types",
            headers={"Content-Type": "application/schema+json"},
        )
        assert response.status_code == 200
        assert response.json() == {
            "type": "object",
            "properties": {"test_integer": {"default": None, "type": "integer"}},
        }

    @pytest.mark.skip(reason="Enable when #397 is fixed")
    def test_schema_protobuf(self):
        response = client.get(
            "/schema/postgresql://localhost:5432/testdb/public/test_types",
            headers={"Content-Type": "application/x-protobuf"},
        )
        assert response.status_code == 200
        assert (
            response.text
            == """
TODO: Some proto schema
"""
        )

    def test_schema_recap(self):
        response = client.get(
            "/schema/postgresql://localhost:5432/testdb/public/test_types",
            headers={"Content-Type": "application/x-recap+json"},
        )
        assert response.status_code == 200
        assert response.json() == {
            "type": "struct",
            "fields": [{"name": "test_integer", "type": "int32", "optional": True}],
        }

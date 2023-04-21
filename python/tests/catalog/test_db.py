import pytest

from recap.catalog.storage import Storage, create_storage
from recap.models import Schema


class TestDatabaseStorage:
    @pytest.fixture
    def storage(self) -> Storage:
        return create_storage("sqlite:///:memory:")

    def test_write(self, storage: Storage):
        url = "bigquery://some-project-1234/some_dataset/some_table"
        schema = Schema(type="string32")
        assert storage.get_schema(url) is None

        storage.put_schema(url, schema)
        assert storage.get_schema(url) == schema

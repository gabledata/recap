import pytest

from recap.clients.mysql import MysqlClient


@pytest.mark.parametrize(
    "method, url_args, expected_result",
    [
        # Test "ls" method with only URL (no schema)
        (
            "ls",
            {"scheme": "mysql", "netloc": "mysql-url"},
            ("mysql://mysql-url", [None]),
        ),
        # Test "ls" method with URL and schema
        (
            "ls",
            {"scheme": "mysql", "netloc": "mysql-url", "paths": ["db1"]},
            ("mysql://mysql-url/db1", ["db1"]),
        ),
        # Test "schema" method with URL, schema, and table
        (
            "schema",
            {"scheme": "mysql", "netloc": "mysql-url", "paths": ["db1", "table1"]},
            ("mysql://mysql-url/db1", ["db1", "table1"]),
        ),
        # Test invalid method
        (
            "invalid_method",
            {"scheme": "mysql", "netloc": "mysql-url"},
            pytest.raises(ValueError, match="Invalid method"),
        ),
        # Test "schema" method with insufficient paths (only schema, no table)
        (
            "schema",
            {"scheme": "mysql", "netloc": "mysql-url", "paths": ["db1"]},
            pytest.raises(ValueError, match="Invalid method"),
        ),
    ],
)
def test_parse_method(method, url_args, expected_result):
    if isinstance(expected_result, tuple):
        result = MysqlClient.parse(method, **url_args)
        assert result == expected_result
    else:
        with expected_result:
            MysqlClient.parse(method, **url_args)

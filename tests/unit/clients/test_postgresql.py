import pytest

from recap.clients.postgresql import PostgresqlClient


@pytest.mark.parametrize(
    "method, url_args, expected_result",
    [
        # Test "ls" method with only URL (no catalog or schema)
        (
            "ls",
            {"scheme": "postgres", "netloc": "postgres-url"},
            ("postgres://postgres-url", [None, None]),
        ),
        # Test "ls" method with URL and catalog
        (
            "ls",
            {"scheme": "postgres", "netloc": "postgres-url", "paths": ["cat1"]},
            ("postgres://postgres-url/cat1", ["cat1", None]),
        ),
        # Test "ls" method with URL, catalog, and schema
        (
            "ls",
            {"scheme": "postgres", "netloc": "postgres-url", "paths": ["cat1", "sch1"]},
            ("postgres://postgres-url/cat1", ["cat1", "sch1"]),
        ),
        # Test "schema" method with URL, catalog, schema, and table
        (
            "schema",
            {
                "scheme": "postgres",
                "netloc": "postgres-url",
                "paths": ["cat1", "sch1", "tbl1"],
            },
            ("postgres://postgres-url/cat1", ["cat1", "sch1", "tbl1"]),
        ),
        # Test invalid method
        (
            "invalid_method",
            {"scheme": "postgres", "netloc": "postgres-url"},
            pytest.raises(ValueError, match="Invalid method"),
        ),
        # Test "schema" method with insufficient paths (only catalog and schema, no table)
        (
            "schema",
            {"scheme": "postgres", "netloc": "postgres-url", "paths": ["cat1", "sch1"]},
            pytest.raises(ValueError, match="Invalid method"),
        ),
        # Test "ls" method with all paths (catalog, schema, and table)
        (
            "ls",
            {
                "scheme": "postgres",
                "netloc": "postgres-url",
                "paths": ["cat1", "sch1", "tbl1"],
            },
            pytest.raises(ValueError, match="Invalid method"),
        ),
    ],
)
def test_parse_method(method, url_args, expected_result):
    if isinstance(expected_result, tuple):
        result = PostgresqlClient.parse(method, **url_args)
        assert result == expected_result
    else:
        with expected_result:
            PostgresqlClient.parse(method, **url_args)

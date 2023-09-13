import pytest

from recap.clients.bigquery import BigQueryClient


@pytest.mark.parametrize(
    "method, paths, expected_result",
    [
        # Test ls method
        ("ls", ["project1"], ("bigquery://", ["project1", None, None])),
        ("ls", [], ("bigquery://", [None, None, None])),
        (
            "ls",
            ["project1", "dataset1"],
            ("bigquery://", ["project1", "dataset1", None]),
        ),
        (
            "ls",
            ["project1", "dataset1", "table1"],
            ("bigquery://", ["project1", "dataset1", "table1"]),
        ),
        # Test schema method
        ("schema", ["project2"], ("bigquery://", ["project2", None, None])),
        (
            "schema",
            ["project2", "dataset2"],
            ("bigquery://", ["project2", "dataset2", None]),
        ),
        (
            "schema",
            ["project2", "dataset2", "table2"],
            ("bigquery://", ["project2", "dataset2", "table2"]),
        ),
        # Test invalid method
        (
            "invalid_method",
            ["project3"],
            pytest.raises(ValueError, match="Invalid method"),
        ),
    ],
)
def test_parse_method(method, paths, expected_result):
    if isinstance(expected_result, tuple):
        result = BigQueryClient.parse(method, paths)
        assert result == expected_result
    else:
        with expected_result:
            BigQueryClient.parse(method, paths)

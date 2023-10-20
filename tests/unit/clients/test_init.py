import pytest

from recap.clients import _url_to_dict, parse_url


def test_url_to_dict_basic():
    url = "http+csr://username:password@hostname:8080/path/to/resource?query=value"
    result = _url_to_dict(url)
    expected = {
        "scheme": "http+csr",
        "netloc": "username:password@hostname:8080",
        "path": "/path/to/resource",
        "params": "",
        "query": "query=value",
        "fragment": "",
        "username": "username",
        "password": "password",
        "hostname": "hostname",
        "port": 8080,
        "dialect": "http",
        "driver": "csr",
        "user": "username",
        "host": "hostname",
        "paths": ["path", "to", "resource"],
        "url": "http://username:password@hostname:8080/path/to/resource?query=value",
        "query": "value",
    }
    assert result == expected


def test_url_to_dict_no_plus():
    url = "https://hostname/path"
    result = _url_to_dict(url)
    assert result["scheme"] == "https"
    assert "dialect" not in result
    assert "driver" not in result


def test_url_to_dict_multiple_query_params():
    url = "http://hostname/path?param1=value1&param2=value2&param2=value3"
    result = _url_to_dict(url)
    assert result["param1"] == "value1"
    assert result["param2"] == ["value2", "value3"]


@pytest.mark.parametrize(
    "method, url, expected_result, strict",
    [
        # ls
        (
            "ls",
            "bigquery://bigquery-url/path1/path2",
            ("bigquery://", ["path1", "path2", None]),
            False,
        ),
        # schema
        (
            "schema",
            "bigquery://bigquery-url/path1/path2/path3",
            ("bigquery://", ["path1", "path2", "path3"]),
            False,
        ),
        # Example of a scheme that isn't supported
        (
            "ls",
            "invalidscheme://invalid-url",
            pytest.raises(ValueError, match="No clients available for scheme"),
            False,
        ),
        # Test invalid method
        (
            "invalid_method",
            "bigquery://bigquery-url",
            pytest.raises(ValueError, match="Invalid method"),
            False,
        ),
    ],
)
def test_parse_url(method, url, expected_result, strict):
    if isinstance(expected_result, tuple):
        result = parse_url(method, url, strict)
        assert result == expected_result
    else:
        with expected_result:
            parse_url(method, url, strict)

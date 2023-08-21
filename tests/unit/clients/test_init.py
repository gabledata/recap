from recap.clients import _parse_url


def test_parse_url_basic():
    url = "http+csr://username:password@hostname:8080/path/to/resource?query=value"
    result = _parse_url(url)
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


def test_parse_url_no_plus():
    url = "https://hostname/path"
    result = _parse_url(url)
    assert result["scheme"] == "https"
    assert "dialect" not in result
    assert "driver" not in result


def test_parse_url_multiple_query_params():
    url = "http://hostname/path?param1=value1&param2=value2&param2=value3"
    result = _parse_url(url)
    assert result["param1"] == "value1"
    assert result["param2"] == ["value2", "value3"]

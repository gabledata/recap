import pytest

from recap.settings import RecapSettings


@pytest.mark.parametrize(
    "input_urls, expected_output",
    [
        # Test with no credentials
        (["http://example.com"], ["http://example.com"]),
        # Test with a username
        (["http://user@example.com"], ["http://example.com"]),
        # Test with a username and password
        (["http://user:pass@example.com"], ["http://example.com"]),
        # Test with a port, username, and password
        (["http://user:pass@example.com:8080"], ["http://example.com:8080"]),
        # Test with multiple URLs
        (
            ["http://user:pass@example.com", "https://secure.site"],
            ["http://example.com", "https://secure.site"],
        ),
        # Test with similar schemes but different credentials
        (
            ["http://user1:pass1@example.com", "http://user2:pass2@example.com"],
            ["http://example.com", "http://example.com"],
        ),
        # Tests with queries
        (["http://example.com?query=value"], ["http://example.com?query=value"]),
        (
            ["http://user:pass@example.com?query=value"],
            ["http://example.com?query=value"],
        ),
        # Tests with fragments
        (["http://example.com#fragment"], ["http://example.com#fragment"]),
        (["http://user:pass@example.com#fragment"], ["http://example.com#fragment"]),
        # Tests with queries and fragments
        (
            ["http://example.com?query=value#fragment"],
            ["http://example.com?query=value#fragment"],
        ),
        (
            ["http://user:pass@example.com?query=value#fragment"],
            ["http://example.com?query=value#fragment"],
        ),
        # Multiple URLs with mixed properties
        (
            ["http://example.com?query=value", "http://user:pass@example.com#fragment"],
            ["http://example.com?query=value", "http://example.com#fragment"],
        ),
        # SQLite empty netloc
        (
            ["sqlite:///some/file.db"],
            ["sqlite:///some/file.db"],
        ),
        # Complex SQLite
        (
            ["sqlite:///file:mem1?mode=memory&cache=shared&uri=true"],
            ["sqlite:///file:mem1?mode=memory&cache=shared&uri=true"],
        ),
    ],
)
def test_safe_urls(input_urls, expected_output):
    settings = RecapSettings(urls=input_urls)
    assert settings.safe_urls == expected_output


@pytest.mark.parametrize(
    "stored_urls, merging_url, expected_output, strict",
    [
        # Basic test with scheme, host, and port match
        (
            ["http://user:pass@example.com"],
            "http://example.com",
            "http://user:pass@example.com",
            True,
        ),
        # Test with a different path in the merging URL
        (
            ["http://user:pass@example.com"],
            "http://example.com/path",
            "http://user:pass@example.com/path",
            True,
        ),
        # Test with a query in the merging URL
        (
            ["http://user:pass@example.com"],
            "http://example.com?query=value",
            "http://user:pass@example.com?query=value",
            True,
        ),
        # Test with a fragment in the merging URL
        (
            ["http://user:pass@example.com"],
            "http://example.com#fragment",
            "http://user:pass@example.com#fragment",
            True,
        ),
        # Test with both a query and a fragment in the merging URL
        (
            ["http://user:pass@example.com"],
            "http://example.com?query=value#fragment",
            "http://user:pass@example.com?query=value#fragment",
            True,
        ),
        # Merging query parameters from both stored and merging URL
        (
            ["http://user:pass@example.com?stored=query"],
            "http://example.com?query=value",
            "http://user:pass@example.com?stored=query&query=value",
            True,
        ),
        # Merging fragments from both stored and merging URL (prefer merging URL's fragment)
        (
            ["http://user:pass@example.com#stored_fragment"],
            "http://example.com#fragment",
            "http://user:pass@example.com#fragment",
            True,
        ),
        # Basic merging of paths
        (
            ["http://user:pass@example.com/path1"],
            "http://example.com/path1/path2",
            "http://user:pass@example.com/path1/path2",
            True,
        ),
        # Merging path and query
        (
            ["http://user:pass@example.com/path1?query1=value1"],
            "http://example.com/path1/path2?query2=value2",
            "http://user:pass@example.com/path1/path2?query1=value1&query2=value2",
            True,
        ),
        # Merging path and fragment
        (
            ["http://user:pass@example.com/path1#fragment1"],
            "http://example.com/path1/path2#fragment2",
            "http://user:pass@example.com/path1/path2#fragment2",
            True,
        ),
        # Merging path, query, and fragment
        (
            ["http://user:pass@example.com/path1?query1=value1#fragment1"],
            "http://example.com/path1/path2?query2=value2#fragment2",
            "http://user:pass@example.com/path1/path2?query1=value1&query2=value2#fragment2",
            True,
        ),
        # When both URLs have the same paths but different queries or fragments
        (
            ["http://user:pass@example.com/path"],
            "http://example.com/path?query=value",
            "http://user:pass@example.com/path?query=value",
            True,
        ),
        # Test unstrict mode
        (
            ["http://some-other.com"],
            "http://example.com/path",
            "http://example.com/path",
            False,
        ),
    ],
)
def test_unsafe_url(stored_urls, merging_url, expected_output, strict):
    settings = RecapSettings(urls=stored_urls)
    assert settings.unsafe_url(merging_url, strict) == expected_output


@pytest.mark.parametrize(
    "stored_urls, merging_url",
    [
        # Different paths
        (["http://example.com/path1"], "http://example.com/path2"),
        # Shorter path
        (["http://example.com/path1/path2"], "http://example.com/path1"),
        # Different ports
        (["http://example.com:8080"], "http://example.com:9090"),
        # Different schemes
        (["http://example.com"], "https://example.com"),
        # Different hostnames
        (["http://example1.com"], "http://example2.com"),
    ],
)
def test_unsafe_url_exceptions(stored_urls, merging_url):
    settings = RecapSettings(urls=stored_urls)

    with pytest.raises(
        ValueError, match=f"URL must be configured in Recap settings: {merging_url}"
    ):
        settings.unsafe_url(merging_url)

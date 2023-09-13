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
    ],
)
def test_safe_urls(input_urls, expected_output):
    settings = RecapSettings(urls=input_urls)
    assert settings.safe_urls == expected_output


@pytest.mark.parametrize(
    "stored_urls, merging_url, expected_output",
    [
        # Basic test with scheme, host, and port match
        (
            ["http://user:pass@example.com"],
            "http://example.com",
            "http://user:pass@example.com",
        ),
        # Test with a different path in the merging URL
        (
            ["http://user:pass@example.com"],
            "http://example.com/path",
            "http://user:pass@example.com/path",
        ),
        # Test with a query in the merging URL
        (
            ["http://user:pass@example.com"],
            "http://example.com?query=value",
            "http://user:pass@example.com?query=value",
        ),
        # Test with a fragment in the merging URL
        (
            ["http://user:pass@example.com"],
            "http://example.com#fragment",
            "http://user:pass@example.com#fragment",
        ),
        # Test with both a query and a fragment in the merging URL
        (
            ["http://user:pass@example.com"],
            "http://example.com?query=value#fragment",
            "http://user:pass@example.com?query=value#fragment",
        ),
        # Merging query parameters from both stored and merging URL
        (
            ["http://user:pass@example.com?stored=query"],
            "http://example.com?query=value",
            "http://user:pass@example.com?stored=query&query=value",
        ),
        # Merging fragments from both stored and merging URL (prefer merging URL's fragment)
        (
            ["http://user:pass@example.com#stored_fragment"],
            "http://example.com#fragment",
            "http://user:pass@example.com#fragment",
        ),
        # No matching base URL should result in original merging URL being returned
        (["http://different.com"], "http://example.com", "http://example.com"),
        # Basic merging of paths
        (
            ["http://user:pass@example.com/path1"],
            "http://example.com/path2",
            "http://user:pass@example.com/path2",
        ),
        # Merging path and query
        (
            ["http://user:pass@example.com/path1?query1=value1"],
            "http://example.com/path2?query2=value2",
            "http://user:pass@example.com/path2?query1=value1&query2=value2",
        ),
        # Merging path and fragment
        (
            ["http://user:pass@example.com/path1#fragment1"],
            "http://example.com/path2#fragment2",
            "http://user:pass@example.com/path2#fragment2",
        ),
        # Merging path, query, and fragment
        (
            ["http://user:pass@example.com/path1?query1=value1#fragment1"],
            "http://example.com/path2?query2=value2#fragment2",
            "http://user:pass@example.com/path2?query1=value1&query2=value2#fragment2",
        ),
        # When both URLs have the same paths but different queries or fragments
        (
            ["http://user:pass@example.com/path"],
            "http://example.com/path?query=value",
            "http://user:pass@example.com/path?query=value",
        ),
        # When stored URL has a longer path than merging URL
        (
            ["http://user:pass@example.com/path1/path2"],
            "http://example.com/path1",
            "http://user:pass@example.com/path1",
        ),
        # When merging URL has a longer path than stored URL
        (
            ["http://user:pass@example.com/path1"],
            "http://example.com/path1/path2",
            "http://user:pass@example.com/path1/path2",
        ),
    ],
)
def test_unsafe_url(stored_urls, merging_url, expected_output):
    settings = RecapSettings(urls=stored_urls)
    assert settings.unsafe_url(merging_url) == expected_output

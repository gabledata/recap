import os
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

from dotenv import load_dotenv
from pydantic import AnyUrl, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

load_dotenv()

HOME_PATH = Path(os.environ.get("RECAP_HOME") or os.path.expanduser("~/.recap"))
DEFAULT_REGISTRY_STORAGE_PATH = Path(HOME_PATH, "schemas")
SECRETS_PATH = Path(dir) if (dir := os.environ.get("RECAP_SECRETS")) else None


def mkdirs():
    HOME_PATH.mkdir(exist_ok=True)

    if SECRETS_PATH:
        SECRETS_PATH.mkdir(exist_ok=True)


mkdirs()


class RecapSettings(BaseSettings):
    urls: list[AnyUrl] = Field(default_factory=list)
    registry_storage_url: AnyUrl = Field(default=DEFAULT_REGISTRY_STORAGE_PATH.as_uri())
    registry_storage_url_args: dict[str, Any] = Field(default_factory=dict)
    model_config = SettingsConfigDict(
        env_file_encoding="utf-8",
        env_prefix="recap_",
        env_nested_delimiter="__",
        secrets_dir=str(SECRETS_PATH) if SECRETS_PATH else None,
    )

    @property
    def safe_urls(self) -> list[str]:
        """
        Return a list of URLs that have the username and password removed.
        """

        safe_urls_list = []

        for url in self.urls:
            split_url = urlsplit(str(url))
            netloc = split_url.netloc

            if split_url.username or split_url.password:
                if split_url.hostname is not None:
                    netloc = split_url.hostname

                if split_url.port is not None:
                    netloc += f":{split_url.port}"

            sanitized_url = urlunsplit(
                (
                    split_url.scheme,
                    netloc,
                    split_url.path.rstrip("/"),
                    split_url.query,
                    split_url.fragment,
                )
            )

            # urlunsplit does not double slashes if netloc is empty. But most
            # URLs with empty netloc should have a double slash (e.g.
            # bigquery:// or sqlite:///some/file.db).
            if not netloc:
                sanitized_url = sanitized_url.replace(":", "://", 1)

            safe_urls_list.append(sanitized_url)

        return safe_urls_list

    def unsafe_url(self, url: str, strict: bool = True) -> str:
        """
        Merge the a URL's user, pass, path, query, and fragment in settings
        into the input URL and return it. Merging occurs if:

        - The input URL's scheme, hostname, and port match a URL in settings.
        - The input URL's path starts with the settings URL's path.

        :param url: URL to merge
        :param strict: If True, raise an error if the URL is not configured in settings.
        :return: URL with user, pass, path, query, and fragment merged from settings.
        """

        url_split = urlsplit(url)
        url_path = Path(url_split.path or "/").as_posix()

        for unsafe_url in self.urls:
            unsafe_url_split = urlsplit(unsafe_url.unicode_string())

            if (
                unsafe_url_split.scheme == url_split.scheme
                and unsafe_url_split.hostname == url_split.hostname
                and unsafe_url_split.port == url_split.port
                and url_path.startswith(Path(unsafe_url_split.path).as_posix())
            ):
                netloc = unsafe_url_split.netloc

                # Merge query strings.
                # NOTE: This doesn't work for URLs that have multiple values
                # for the same key spanning both the unsafe and input URLs.
                unsafe_qs = parse_qs(unsafe_url_split.query)
                url_qs = parse_qs(url_split.query)
                merged_qs = unsafe_qs | url_qs
                query = urlencode(merged_qs, doseq=True)

                merged_url = urlunsplit(
                    (
                        url_split.scheme,
                        netloc,
                        url_path.rstrip("/"),
                        query,
                        url_split.fragment or unsafe_url_split.fragment,
                    )
                )

                # urlunsplit does not double slashes if netloc is empty. But most
                # URLs with empty netloc should have a double slash (e.g.
                # bigquery:// or sqlite:///some/file.db).
                if not netloc:
                    merged_url = merged_url.replace(":", "://", 1)

                return merged_url

        if strict:
            raise ValueError(f"URL must be configured in Recap settings: {url}")

        return url

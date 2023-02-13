from contextlib import contextmanager
from pathlib import PurePosixPath
from typing import Generator
from urllib.parse import urlparse

from frictionless import Resource, describe  # type: ignore

from recap.analyzers.abstract import AbstractAnalyzer, BaseMetadataModel

SUPPORTED_SCHEMES = set(["", "file", "http", "https", "s3"])


class Column(BaseMetadataModel):
    type: str


class Columns(BaseMetadataModel):
    __root__: dict[str, Column] = {}


class FileColumnAnalyzer(AbstractAnalyzer):
    """
    Use Frictionless to fetch table schema information for CSV, TSV, JSON, and
    Parquet files. The schema simply includes the name and type.

    CSV, TSV, and JSON schemas are inferred using Frictionless's `describe()`
    inferrence.
    """

    def __init__(self, url: str):
        """
        :param url: Base URL to connect to. The URL may be any format that
            Frictionless accepts (local, S3, http, and so on). Local URLs must
            start with `file://`.
        """

        self.url = url

    def analyze(
        self,
        path: str,
    ) -> Columns | None:
        """
        Analyze a path and return Frictionless's schema information.

        :param path: Path relative to the URL root.
        :returns: Frictionless schema description.
        """

        path_posix = PurePosixPath(str(path))
        url_and_path = self.url + str(path_posix)
        match path_posix.suffix:
            case (".csv" | ".tsv" | ".parquet"):
                resource = describe(url_and_path)
                if isinstance(resource, Resource):
                    columns_dict = {}
                    for field in resource.schema.fields:
                        columns_dict[field.name] = Column(type=field.type)
                    return Columns.parse_obj(columns_dict)
            case (".json" | ".ndjson" | ".jsonl"):
                resource = describe(path=url_and_path, format="ndjson")
                if isinstance(resource, Resource):
                    columns_dict = {}
                    for field in resource.schema.fields:
                        columns_dict[field.name] = Column(type=field.type)
                    return Columns.parse_obj(columns_dict)
            case _:
                return None


@contextmanager
def create_analyzer(
    url: str,
    **_,
) -> Generator[FileColumnAnalyzer, None, None]:
    scheme = urlparse(url).scheme
    if scheme in SUPPORTED_SCHEMES:
        if scheme == "":
            # Frictionless is paranoid about absolute paths. Use a file scheme
            # so that it allows them.
            url = f"file://{url}"
        yield FileColumnAnalyzer(url)
    else:
        raise ValueError(f"Unsupported url={url}")

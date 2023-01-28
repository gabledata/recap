import duckdb
from contextlib import contextmanager
from pathlib import PurePosixPath
from recap.analyzers.abstract import AbstractAnalyzer, BaseMetadataModel
from recap.browsers.fs import FilePath
from typing import Generator, Literal
from urllib.parse import urlparse


SUPPORTED_SCHEMES = set(['', 'file', 'http', 'https', 's3'])


class Column(BaseMetadataModel):
    type: str
    nullable: bool

class Columns(BaseMetadataModel):
    __root__: dict[str, Column] = {}


class FileColumnAnalyzer(AbstractAnalyzer):
    def __init__(self, url: str):
        # DuckDB doesn't understand 'file://' prefix, so remove it.
        self.url = url.removeprefix('file://')
        self.db = duckdb.connect()

    def analyze(
        self,
        path: FilePath,
    ) -> Columns | None:
        path_posix = PurePosixPath(str(path))
        url_and_path = self.url + str(path_posix)
        match path_posix.suffix:
            case ('.csv' | '.tsv'):
                self.db.execute(
                    "DESCRIBE SELECT * FROM read_csv_auto(?)",
                    [url_and_path],
                )
            case '.parquet':
                self.db.execute(
                    "DESCRIBE SELECT * FROM read_parquet(?)",
                    [url_and_path],
                )
            case _:
                return None
        columns_dict = {}
        for column_tuple in self.db.fetchall():
            name = column_tuple[0]
            type_ = column_tuple[1]
            nullable = column_tuple[2] == 'YES'
            columns_dict[name] = Column(type=type_, nullable=nullable)
        return Columns.parse_obj(columns_dict)


@contextmanager
def create_analyzer(
    url: str,
    **_,
) -> Generator['FileColumnAnalyzer', None, None]:
    scheme = urlparse(url).scheme
    if scheme in SUPPORTED_SCHEMES:
        yield FileColumnAnalyzer(url)
    else:
        raise ValueError(f"Unsupported url={url}")

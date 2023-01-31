import logging
import math
import pandas
from contextlib import contextmanager
from datetime import datetime
from json import dumps
from pathlib import PurePosixPath
from recap.analyzers.abstract import AbstractAnalyzer, BaseMetadataModel
from recap.browsers.db import TablePath, ViewPath
from recap.browsers.fs import FilePath
from typing import Any, Generator


log = logging.getLogger(__name__)


class ColumnProfile(BaseMetadataModel):
    count: int
    unique: int | None = None
    top: str | None = None
    freq: int | None = None
    mean: float | str | None = None
    std: float | str | None = None
    min: float | str | None = None
    p25: float | str | None = None
    p50: float | str | None = None
    p75: float | str | None = None
    p95: float | str | None = None
    p99: float | str | None = None
    p999: float | str | None = None
    max: float | str | None = None


class Profile(BaseMetadataModel):
    __root__: dict[str, ColumnProfile] = {}


class ProfileAnalyzer(AbstractAnalyzer):
    """
    Analyze CSV, TSV, JSON, Parquet, tables, and views using Parquet's
    `describe()` method. `describe()` returns data profile statistics such as
    count, unique, min, max, and various percentiles.
    """

    def __init__(
        self,
        url: str,
    ):
        """
        :param url: Base URL to connect to. The URL may be any format that
            Pandas accepts (local, S3, http, and so on).
        """
        self.url = url

    def analyze(
        self,
        path: TablePath | ViewPath | FilePath,
    ) -> Profile | None:
        """
        Analyze a path and return a JSON schema.

        :param path: Path relative to the URL root.
        :returns: Data profile descriptions for each column.
        """

        path_posix = PurePosixPath(str(path))
        url_and_path = self.url + str(path_posix)
        df = pandas.DataFrame()
        match (path, path_posix.suffix):
            case (FilePath(), '.csv'):
                df = pandas.read_csv(url_and_path)
            case (FilePath(), '.tsv'):
                df = pandas.read_csv(url_and_path, sep='\t')
            case (FilePath(), '.json' | '.ndjson' | '.jsonl'):
                df = pandas.read_json(url_and_path, lines=True)
            case (FilePath(), '.parquet'):
                df = pandas.read_parquet(url_and_path)
            case (TablePath() | ViewPath(), _):
                # Meh.. try a SQL connection, I guess.
                # Types are pretty busted with structured matching. :(
                name = path.table if isinstance(path, TablePath) else path.view # pyright: ignore [reportGeneralTypeIssues]
                # TODO should sample the data here
                df = pandas.read_sql_table(
                    table_name=name,
                    schema=path.schema_, # pyright: ignore [reportGeneralTypeIssues]
                    con=self.url,
                )
        return self._analyze_dataframe(df) if not df.empty else None

    def _analyze_dataframe(self, df: pandas.DataFrame) -> Profile:
        profile_dict = {}
        df_description = df.describe(
            percentiles=[.25, .5, .75, .95, .99, .999],
            include='all',
            # Get rid of FutureWarning
            datetime_is_numeric=True,
        )
        # Get rid of 'nan' for JSON.
        df_description = df_description.replace({math.nan: None})
        for name, stats_dict in df_description.to_dict().items():
            # Replace percentiles if they're set.
            formatted_percentiles = {
                'p25': stats_dict.pop('25%'),
                'p50': stats_dict.pop('50%'),
                'p75': stats_dict.pop('75%'),
                'p95': stats_dict.pop('95%'),
                'p99': stats_dict.pop('99%'),
                'p999': stats_dict.pop('99.9%'),
            } if '25%' in stats_dict else {}
            column_profile_dict: dict[str, Any] = (
                formatted_percentiles
                | stats_dict
            )
            # Replace datetime with ISO 8601 for JSON.
            column_profile_dict = dict([
                (k, v.isoformat())
                if isinstance(v, datetime)
                else (k, v)
                for k, v in column_profile_dict.items()
            ])
            # Pandas `top` can be a list or dict if input was JSON.
            # Convert it to a JSON string.
            if (
                column_profile_dict['top'] is not None
                and not isinstance(column_profile_dict['top'], str)
            ):
                column_profile_dict['top'] = dumps(column_profile_dict['top'])
            # Finally! Create the ColumnProfile.
            profile_dict[name] = ColumnProfile(**column_profile_dict)
        return Profile.parse_obj(profile_dict)


@contextmanager
def create_analyzer(url: str, **_) -> Generator['ProfileAnalyzer', None, None]:
    # TODO if URL is DB, create an engine that uses engine.* configs like db.py
    yield ProfileAnalyzer(url)

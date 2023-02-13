from contextlib import contextmanager
from json import loads
from pathlib import PurePosixPath
from typing import Any, Generator

from fsspec import AbstractFileSystem
from genson import SchemaBuilder  # type: ignore

from recap.analyzers.abstract import AbstractAnalyzer, BaseMetadataModel
from recap.browsers.fs import create_browser


class JsonSchema(BaseMetadataModel):
    __root__: dict[str, Any]


class FileColumnAnalyzer(AbstractAnalyzer):
    """
    Use Genson to infer a JSON schema for a JSON file.
    """

    def __init__(
        self,
        fs: AbstractFileSystem,
        base_path: str,
        sample: int | None = 1024,
    ):
        """
        :param fs: An fsspec filesystem to read files from.
        :param base_path: The `path` portion of the URL to read from. Acts as
            the root path for child paths.
        :param sample: If set, read the first N rows of a JSON file when
            inferring its schema. Sometimes preferable for performance reasons.
        """

        self.fs = fs
        self.base_path = base_path
        self.sample = sample

    def analyze(
        self,
        path: str,
    ) -> JsonSchema | None:
        """
        Analyze a path and return a JSON schema.

        :param path: Path relative to the URL root.
        :returns: An inferred JSON Schema.
        """

        builder = SchemaBuilder()
        absolute_path_posix = PurePosixPath(self.base_path, str(path).lstrip("/"))
        if (
            absolute_path_posix.suffix == ".json"
            or absolute_path_posix.suffix == ".ndjson"
            or absolute_path_posix.suffix == ".jsonl"
        ):
            with self.fs.open(str(absolute_path_posix), "rt") as f:
                line_count = 0
                while (obj := f.readline().strip()) and (
                    not self.sample or line_count < self.sample
                ):
                    builder.add_object(loads(obj))
                    line_count += 1
            return JsonSchema.parse_obj(builder.to_schema())
        return None


@contextmanager
def create_analyzer(
    url: str,
    sample: int | None = 1024,
    **config,
) -> Generator[FileColumnAnalyzer, None, None]:
    # TODO This is hacky. Shoulld factor out FS and patch creation.
    with create_browser(url=url, **config) as browser:
        yield FileColumnAnalyzer(browser.fs, browser.base_path, sample)

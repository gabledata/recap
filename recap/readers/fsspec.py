from json import loads
from pathlib import PurePosixPath
from urllib.parse import urlparse

from frictionless import Resource, describe  # type: ignore
from fsspec import AbstractFileSystem
from genson import SchemaBuilder

from recap import types
from recap.converters.frictionless import FrictionlessConverter
from recap.converters.json_schema import JsonSchemaConverter
from recap.readers.readers import functions


@functions.schema("s3://{bucket}/{path:path}", include_fs=True, include_url=True)
@functions.schema("gs://{bucket}/{path:path}", include_fs=True, include_url=True)
@functions.schema("file:///{path:path}", include_fs=True, include_url=True)
@functions.schema("/{path:path}", include_fs=True, include_url=True)
def schema(
    fs: AbstractFileSystem,
    url: str,
    path: str,
    **_,
) -> types.Type | None:
    """
    Fetch a Recap schema for a URL. This method supports S3 and local
    filesystems, and CSV, TSV, Parquet, and JSON filetypes.

    Recap uses `frictionless` for CSV and TSV schema inference. Genson is used
    for JSON file schema inference.

    :param url: The fully matched URL when using the function registry.
    :param path: Path to a CSV, TSV, Parquet, or JSON file.
    """

    path_posix = PurePosixPath(path)
    resource = None

    # Frictionless is picky about local file paths.
    if urlparse(url).scheme == "":
        url = f"file://{url}"

    match path_posix.suffix:
        case (".csv" | ".tsv" | ".parquet"):
            resource = describe(url)
        case (".json" | ".ndjson" | ".jsonl"):
            builder = SchemaBuilder()
            with fs.open(url) as f:
                for line in f.readlines():
                    builder.add_object(loads(line))
            return JsonSchemaConverter().to_recap_type(builder.to_schema())

    if isinstance(resource, Resource):
        return FrictionlessConverter().to_recap_type(
            resource.schema,  # pyright: ignore[reportOptionalMemberAccess]
        )


@functions.ls("s3://{path:path}", include_fs=True, include_url=True)
@functions.ls("gs://{path:path}", include_fs=True, include_url=True)
@functions.ls("file:///{path:path}", include_fs=True, include_url=True)
@functions.ls("/{path:path}", include_fs=True, include_url=True)
def ls(
    url: str,
    fs: AbstractFileSystem,
    path: str | None = None,
) -> list[str] | None:
    """
    List all children in a filesystem path. Recap treats all filesystem paths
    as objects (similar to S3), so each URL might contain data and/or child
    URLs.

    :param url: The fully matched URL when using the function registry.
    :param fs: A `fsspec` filesystem.
    :param path: Filesystem path.
    :returns: A list of child URLs.
    """

    scheme = urlparse(url).scheme
    # Force a "file" scheme because frictionless is picky.
    if scheme == "":
        scheme = "file"
    if scheme == "file":
        path = f"/{path}"
    if fs.isdir(path):
        return [
            f"{scheme}://{child['name']}"
            # Force detail=True because gcsfs doesn't honor defaults.
            for child in fs.ls(path or "", detail=True)
        ]

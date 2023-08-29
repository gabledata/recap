from recap.clients import create_client
from recap.settings import RecapSettings
from recap.types import StructType

settings = RecapSettings()


def ls(path: str = "/") -> list[str] | None:
    """
    List the children of a path.

    :param path: Path to list children of. Defaults to root.
    :return: List of children.
    """

    system, *args = _args(path) if path not in ("", "/") else [None]
    if not system:
        return list(settings.systems.keys())
    if system and (url := settings.systems.get(system)):
        with create_client(url.unicode_string()) as client:
            return client.ls(*args)


def schema(path: str) -> StructType | None:
    """
    Get the schema of a path.

    :param path: Path to get schema of.
    :return: Schema of path.
    """

    system, *args = _args(path)
    if system and (url := settings.systems.get(system)):
        with create_client(url.unicode_string()) as client:
            return client.get_schema(*args)


def _args(path: str | None) -> list[str]:
    return path.strip("/").split("/") if path else []

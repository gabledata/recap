from .config import settings
from . import storage, search
from .search.abstract import AbstractSearchIndex
from .storage.abstract import AbstractStorage
from .storage.notifier import StorageNotifier
from fastapi import Body, Depends, FastAPI
from pathlib import PurePosixPath
from typing import Any, List, Generator


app = FastAPI()


def get_search() -> Generator[AbstractSearchIndex, None, None]:
    with search.open(**settings['search']) as s:
        yield s


def get_storage() -> Generator[AbstractStorage, None, None]:
    with storage.open(**settings['storage']) as st:
        with search.open(**settings['search']) as se:
            yield StorageNotifier(st, se)


# WARN This must go before get_path since get_path is a catch-all.
@app.get("/search")
def query_search(
    query: str,
    search: AbstractSearchIndex = Depends(get_search),
) -> List[dict[str, Any]]:
    return search.search(query)


@app.get("/{path:path}")
def get_path(
    # TODO Make this a PurePosixPath type. FastAPI is hassling me right now.
    path: str,
    read: bool | None = None,
    storage: AbstractStorage = Depends(get_storage),
) -> List[str] | dict[str, Any]:
    # TODO should probably return a 404 if we get None fro storage
    if read:
        return storage.read(PurePosixPath(path)) or {}
    else:
        return storage.ls(PurePosixPath(path)) or []


@app.put("/{path:path}")
def put_path(
    # TODO Make this a PurePosixPath type. FastAPI is hassling me right now.
    path: str,
    type: str | None = None,
    metadata: Any = Body(default=None),
    storage: AbstractStorage = Depends(get_storage),
):
    if type and metadata:
        storage.write(PurePosixPath(path), type, metadata)
    else:
        return storage.touch(PurePosixPath(path))


@app.delete("/{path:path}")
def delete_path(
    # TODO Make this a PurePosixPath type. FastAPI is hassling me right now.
    path: str,
    type: str | None = None,
    storage: AbstractStorage = Depends(get_storage),
):
    storage.rm(PurePosixPath(path), type)

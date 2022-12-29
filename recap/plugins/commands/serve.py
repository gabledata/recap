import typer
from fastapi import Body, Depends, FastAPI
from pathlib import PurePosixPath
from typing import Any, List, Generator
from recap.plugins import catalogs
from recap.plugins.catalogs.abstract import AbstractCatalog
from recap.config import settings
from recap.logging import setup_logging


DEFAULT_URL = 'http://localhost:8000'

app = typer.Typer()
fastapp = FastAPI()


def get_catalog() -> Generator[AbstractCatalog, None, None]:
    with catalogs.open(**settings('catalog', {})) as c:
        yield c


# WARN This must go before get_path since get_path is a catch-all.
@fastapp.get("/search")
def query_search(
    query: str,
    catalog: AbstractCatalog = Depends(get_catalog),
) -> List[dict[str, Any]]:
    return catalog.search(query)


@fastapp.get("/{path:path}")
def get_path(
    # TODO Make this a PurePosixPath type. FastAPI is hassling me right now.
    path: str,
    read: bool | None = None,
    catalog: AbstractCatalog = Depends(get_catalog),
) -> List[str] | dict[str, Any]:
    # TODO should probably return a 404 if we get None from storage
    if read:
        return catalog.read(PurePosixPath(path)) or {}
    else:
        return catalog.ls(PurePosixPath(path)) or []


@fastapp.put("/{path:path}")
def put_path(
    # TODO Make this a PurePosixPath type. FastAPI is hassling me right now.
    path: str,
    type: str | None = None,
    metadata: Any = Body(default=None),
    catalog: AbstractCatalog = Depends(get_catalog),
):
    if type and metadata:
        catalog.write(PurePosixPath(path), type, metadata)
    else:
        return catalog.touch(PurePosixPath(path))


@fastapp.delete("/{path:path}")
def delete_path(
    # TODO Make this a PurePosixPath type. FastAPI is hassling me right now.
    path: str,
    type: str | None = None,
    catalog: AbstractCatalog = Depends(get_catalog),
):
    catalog.rm(PurePosixPath(path), type)


@app.command()
def serve():
    import uvicorn

    uvicorn.run(
        fastapp,
        log_config=setup_logging(),
        **settings('api', {}),
    )

import json

from fastapi import APIRouter, Depends, HTTPException, Request

from recap.settings import RecapSettings
from recap.storage.registry import RegistryStorage
from recap.types import RecapType, from_dict, to_dict

router = APIRouter(prefix="/registry")
settings = RecapSettings()


def get_storage() -> RegistryStorage:
    return RegistryStorage(
        settings.registry_storage_url.unicode_string(),
        **settings.registry_storage_url_args,
    )


@router.get("/")
async def ls(storage: RegistryStorage = Depends(get_storage)) -> list[str]:
    return storage.ls()


@router.get("/{name:str}")
async def latest(
    name: str,
    storage: RegistryStorage = Depends(get_storage),
) -> tuple[dict | list | str, int]:
    if type_and_version := storage.get(name):
        type_, version = type_and_version
        return to_dict(type_), version
    else:
        raise HTTPException(status_code=404, detail="Not found")


@router.get("/{name:str}/versions")
async def versions(
    name: str,
    storage: RegistryStorage = Depends(get_storage),
) -> list[int]:
    if versions := storage.versions(name):
        return versions
    else:
        raise HTTPException(status_code=404, detail="Not found")


@router.get("/{name:str}/versions/{version:int}")
async def version(
    name: str,
    version: int,
    storage: RegistryStorage = Depends(get_storage),
) -> tuple[dict | list | str, int]:
    if type_and_version := storage.get(name, version):
        type_, version = type_and_version
        return to_dict(type_), version
    else:
        raise HTTPException(status_code=404, detail="Not found")


@router.post("/{name:str}")
async def post(
    name: str,
    request: Request,
    storage: RegistryStorage = Depends(get_storage),
) -> int:
    type_ = await _request_to_type(request)

    return storage.put(name, type_)


@router.put("/{name:str}/versions/{version:int}")
async def put(
    name: str,
    version: int,
    request: Request,
    storage: RegistryStorage = Depends(get_storage),
):
    if storage.get(name, version):
        raise HTTPException(
            status_code=409,
            detail=f"Type {name} version {version} already exists",
        )

    type_ = await _request_to_type(request)

    return storage.put(name, type_, version)


async def _request_to_type(request: Request) -> RecapType:
    content_type = request.headers.get("content-type") or "application/x-recap+json"

    if content_type != "application/x-recap+json":
        raise HTTPException(
            status_code=415,
            detail=f"Unsupported content type: {content_type}",
        )

    type_bytes = await request.body()
    type_str = type_bytes.decode("utf-8")

    try:
        type_json = json.loads(type_str)
    except json.JSONDecodeError:
        # Assume payload is string alias if we can't decode as JSON.
        # If it's not, from_dict will fail below.
        type_json = type_str

    try:
        return from_dict(type_json)
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to parse type: {e}",
        )

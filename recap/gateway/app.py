from fastapi import Depends, FastAPI

from recap.clients import Client, create_client
from recap.gateway.settings import RecapSettings
from recap.types import to_dict

app = FastAPI()
settings = RecapSettings()


async def get_reader(system_name: str | None = None):
    if system_name and (url := settings.systems.get(system_name)):
        with create_client(url.unicode_string()) as client:
            yield client
    else:
        yield None


@app.get("/ls")
@app.get("/ls/{system_name}")
@app.get("/ls/{system_name}/{path:path}")
async def ls(
    system_name: str | None = None,
    path: str | None = None,
    client: Client | None = Depends(get_reader),
) -> list[str]:
    if system_name is None:
        return list(settings.systems.keys())
    if client is None:
        raise ValueError(f"Unknown system: {system_name}")
    return client.ls(*_args(path))


@app.get("/schema/{system_name}/{path:path}")
async def schema(
    path: str,
    client: Client = Depends(get_reader),
) -> dict:
    print(path)
    recap_struct = client.get_schema(*_args(path))
    recap_dict = to_dict(recap_struct)
    if not isinstance(recap_dict, dict):
        raise ValueError(f"Expected dict, got {type(recap_dict)}")
    return recap_dict


def _args(path: str | None) -> list[str]:
    return path.strip("/").split("/") if path else []

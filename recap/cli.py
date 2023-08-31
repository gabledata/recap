from typing import Annotated

import typer
import uvicorn
from rich import print_json

from recap import commands
from recap.settings import set_config, unset_config
from recap.types import to_dict

app = typer.Typer()


@app.command()
def ls(path: Annotated[str, typer.Argument(help="Path to list children of.")] = "/"):
    """
    List the children of a path.
    """

    if children := commands.ls(path):
        print_json(data=children)


@app.command()
def schema(path: Annotated[str, typer.Argument(help="Path to get schema of.")]):
    """
    Get the schema of a path.
    """

    if recap_struct := commands.schema(path):
        print_json(data=to_dict(recap_struct))


@app.command()
def add(
    system: Annotated[str, typer.Argument(help="User-defined name of the system.")],
    url: Annotated[str, typer.Argument(help="URL for the system.")],
):
    """
    Add a system to the config.
    """

    set_config(f"RECAP_SYSTEMS__{system}", url)


@app.command()
def remove(
    system: Annotated[str, typer.Argument(help="User-defined name of the system.")],
):
    """
    Remove a system from the config.
    """

    unset_config(f"RECAP_SYSTEMS__{system}")


@app.command()
def serve(
    host: str = "127.0.0.1",
    port: int = 8000,
    log_level: str = "info",
):
    """
    Serve the API.
    """

    uvicorn.run("recap.gateway:app", host=host, port=port, log_level=log_level)

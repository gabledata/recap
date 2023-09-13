from typing import Annotated, Optional

import typer
import uvicorn
from rich import print_json

from recap import commands
from recap.types import to_dict

app = typer.Typer()


@app.command()
def ls(url: Annotated[Optional[str], typer.Argument(help="URL to parent.")] = None):
    """
    List a URL's children.
    """

    if children := commands.ls(url):
        print_json(data=children)


@app.command()
def schema(url: Annotated[str, typer.Argument(help="URL to schema.")]):
    """
    Get a URL's schema.
    """

    if recap_struct := commands.schema(url):
        print_json(data=to_dict(recap_struct))


@app.command()
def serve(
    host: str = "127.0.0.1",
    port: int = 8000,
    log_level: str = "info",
):
    """
    Start Recap's HTTP/JSON gateway server.
    """

    uvicorn.run("recap.gateway:app", host=host, port=port, log_level=log_level)

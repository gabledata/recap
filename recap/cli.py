from typing import Annotated, Optional

import typer
from rich import print, print_json

from recap import commands

app = typer.Typer()


@app.command()
def ls(url: Annotated[Optional[str], typer.Argument(help="URL to parent.")] = None):
    """
    List a URL's children.
    """

    if children := commands.ls(url, strict=False):
        print_json(data=children)


@app.command()
def schema(
    url: Annotated[str, typer.Argument(help="URL to schema.")],
    output_format: Annotated[
        commands.SchemaFormat,
        typer.Option("--output-format", "-of", help="Schema output format."),
    ] = commands.SchemaFormat.recap,
):
    """
    Get a URL's schema.
    """

    struct_obj = commands.schema(url, output_format, strict=False)
    match struct_obj:
        case dict():
            print_json(data=struct_obj)
        case str():
            print(struct_obj)
        case _:
            raise ValueError(f"Unexpected schema type: {type(struct_obj)}")


@app.command()
def serve(
    host: str = "127.0.0.1",
    port: int = 8000,
    log_level: str = "info",
):
    """
    Start Recap's HTTP/JSON gateway server.
    """

    import uvicorn

    uvicorn.run("recap.server.app:app", host=host, port=port, log_level=log_level)

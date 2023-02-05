import typer

from recap.config import settings
from recap.logging import setup_logging
from recap.server import fastapp

app = typer.Typer()


@app.command()
def serve():
    """
    Starts Recap's HTTP/JSON API server.

    \b
    You might wish to centralize your Recap data catalog in one place, and have
    all users read from the same location. Recap ships with an HTTP/JSON server
    for this use case. Start a Recap server on a single host using
    `recap serve`.

    \b
    Clients can then configure their Recap catalog in `settings.toml` to point
    to the server location:

    \b
    ```toml
    [catalog]
    type = "recap"
    url = "http://192.168.0.1:8000"
    ```
    """

    import uvicorn

    uvicorn.run(
        fastapp,
        log_config=setup_logging(),
        **settings("server.uvicorn", {}),
    )

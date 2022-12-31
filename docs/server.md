Recap comes with an HTTP/JSON API server implementation of Recap's [catalog](catalogs.md) interface. The server allows non-Python systems to integrate with Recap. The server also allows metadata to be shared amgonst different systems when they all point to the same server using [Recap's catalog](catalogs.md#recap-catalog).

## Starting

Recap's server starts when the `recap serve` command is executed.

## Configuring

Recap's server is implemented as a [FastAPI](https://fastapi.tiangolo.com/) in a [uvicorn](https://www.uvicorn.org/) web server. Any configuration set in your `settings.toml` file under the `api` namespace will be passed to Recap's `uvicorn.run` invocation.

```toml
[api]
host = "192.168.0.1"
port = 9000
access_log = false
```

## Endpoints

Recap has only two endpoints:

* GET `/search` - Search the Recap catalog.
* GET | PUT | DELETE `/<path>` - Read, write, or remove metadata or directory paths.

Recap's JSON schema is visible at [http://localhost:8000/docs](http://localhost:8000/docs) when you start the server with the `recap server` command. Recap's catalog [source code](https://github.com/recap-cloud/recap/blob/main/recap/plugins/catalogs/recap.py) illustrates how to call Recap server.

!!! note

    Recap's server does not currently have data models for each metadata type; they're just JSON blobs. The JSON schema displays the return type as an `application/json` string.

## Examples

The following examples illlustrate how to call a Recap server running at [http://localhost:8000](http://localhost:8000).

### Read a directory

```bash
curl http://localhost:8000/databases/postgresql
```

### Read metadata

```bash
curl 'http://localhost:8000/databases/postgresql/instances/some_instance/schemas/some_db/tables/some_table?read=true'
```

### Write a directory

```bash
curl -X PUT http://localhost:8000/databases/postgresql/instances/some_instance/schemas/some_db/tables
```

### Write metadata

```bash
curl -X PUT 'http://localhost:8000/databases/postgresql/instances/some_instance/schemas/some_db/tables/some_table?type=some_metadata_type' \
  -d '{"some_metadata_type": {"metadata_key": "metadata_value"}}' \
  -H "Content-Type: application/json"
```

### Delete a directory

```bash
curl -X DELETE http://localhost:8000/databases/postgresql
```

### Delete metadata

```bash
curl -X DELETE 'http://localhost:8000/databases/postgresql/instances/some_instance/schemas/some_db/tables/some_table?type=some_metadata_type'
```

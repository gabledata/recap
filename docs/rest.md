Recap comes with an HTTP/JSON API server implementation of Recap's [storage](api/storage.md) interface. The server allows non-Python systems to integrate with Recap, and different systems to share their metadata when they all point to the same server.

## Starting

Execute `recap serve` to start Recap's server.

## Configuring

Recap's server is implemented as a [FastAPI](https://fastapi.tiangolo.com/) in a [uvicorn](https://www.uvicorn.org/) web server. You can configure uvicorn using the `RECAP_UVICORN_SETTINGS` environment variable.

```bash
RECAP_UVICORN_SETTINGS='{"port": 9000}'
```

## Endpoints

Recap's JSON schema is visible at [http://localhost:8000/openapi.json](http://localhost:8000/openapi.json). API documentation is also visible at [http://localhost:8000/docs](http://localhost:8000/docs) and [http://localhost:8000/redoc](http://localhost:8000/redoc).

## Examples

The following examples illlustrate how to call a Recap server running at [http://localhost:8000](http://localhost:8000).

### Write a Schema

```bash
curl -X PUT 'http://localhost:8000/storage/postgresql://localhost/some_db/some_schema/some_table/metadata/schema' \
  -d '{"fields": [{"name": "test"}]}' \
  -H "Content-Type: application/json"
```

### Read a Schema

```bash
curl 'http://localhost:8000/storage/postgresql://localhost/some_db/some_schema/some_table/metadata/schema'
```

### Write a Relationship

```bash
curl -X POST 'http://localhost:8000/storage/postgresql://localhost/some_db/some_schema/links/contains?relationship=contains&other_url=http://localhost:8000/storage/postgresql://localhost/some_db/some_schema/some_table'
```

### Read a Relationship

```bash
curl 'http://localhost:8000/storage/postgresql://localhost/some_db/some_schema/links/contains'
```

### Delete a Relationship

```bash
curl -X DELETE 'http://localhost:8000/storage/postgresql://localhost/some_db/some_schema/links/contains?relationship=contains&other_url=http://localhost:8000/storage/postgresql://localhost/some_db/some_schema/some_table'
```

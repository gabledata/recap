Recap comes with an HTTP/JSON API server implementation of Recap's [catalog](catalogs.md) interface. The server allows non-Python systems to integrate with Recap. The server also allows metadata to be shared amgonst different systems when they all point to the same server.

## Starting

Execute `recap serve` to start Recap's server.

## Configuring

Recap's server is implemented as a [FastAPI](https://fastapi.tiangolo.com/) in a [uvicorn](https://www.uvicorn.org/) web server. Any configuration set in your `settings.toml` file under the `server.uvicorn` namespace will be passed to Recap's `uvicorn.run` invocation.

```toml
[server]
uvicorn.port = 9000
```

You can also enable only certain [router plugins](plugins.md#routers) using the `server.plugins` setting:

```toml
[server]
plugins = [
	"catalog.typed"
]
```

## Endpoints

Recap has the following endpoints:

* GET `/catalog?query=<query>` - Search the Recap catalog.
* GET | PUT | PATCH `/catalog/{path}/metadata` - Read, write, or remove metadata.
* GET `/catalog/{path}/children` - List a path's child directories.
* DELETE `/catalog/{path}` - Delete a path, its metadata, and its children.

Recap's JSON schema is visible at [http://localhost:8000/openapi.json](http://localhost:8000/openapi.json). API documentation is also visible at [http://localhost:8000/docs](http://localhost:8000/docs) and [http://localhost:8000/redoc](http://localhost:8000/redoc).

## Models

Recap's catalog [router plugin](plugins.md#routers) comes in two flavors: typed and untyped. The [endpoints](server.md#endpoints) listed above are untyped--metadata is a `dict[str, Any]`. Typed endpoints use [Pydantic](https://pydantic.dev) data models from [analyzers](analyzers.md) to define strongly typed (and validated) metadata models.

Recap's server turns on both routers by default. Interacting with typed paths will validate metadata JSON. Interacting with untyped paths will allow arbitrary JSON dictionaries through. A typed path looks something like this:

    /catalog/databases/{scheme}/instances/{name}/schemas/{schema}/views/{view}/metadata

## Examples

The following examples illlustrate how to call a Recap server running at [http://localhost:8000](http://localhost:8000).

### Write metadata

```bash
curl -X PATCH 'http://localhost:8000/catalog/databases/postgresql/instances/some_instance/schemas/some_db/tables/some_table/metadata' \
  -d '{"comment": "This is some table"}' \
  -H "Content-Type: application/json"
```

### Read metadata

```bash
curl 'http://localhost:8000/catalog/databases/postgresql/instances/some_instance/schemas/some_db/tables/some_table/metadata'
```

### Read a directory

```bash
curl http://localhost:8000/catalog/databases/postgresql/children
```

### Delete a directory

```bash
curl -X DELETE 'http://localhost:8000/catalog/databases/postgresql/instances/some_instance/schemas/some_db/tables/some_table'
```

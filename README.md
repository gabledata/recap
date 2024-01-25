<div align="center">
  <img src="https://github.com/recap-build/recap/blob/main/static/recap-logo.png?raw=true" alt="recap">
</div>

## What is Recap?

Recap reads and writes schemas from web services, databases, and schema registries in a standard format.

⭐️ _If you like this project, please give it a star! It helps the project get more visibility._

## Table of Contents

* [What is Recap?](#what-is-recap)
* [Supported Formats](#supported-formats)
* [Install](#install)
* [Usage](#usage)
   * [CLI](#cli)
   * [Gateway](#gateway)
   * [Registry](#registry)
   * [API](#api)
   * [Docker](#docker)
* [Schema](#schema)
* [Documentation](#documentation)

## Supported Formats

| Format      | Read | Write |
| :---------- | :-: | :-: |
| [Avro](https://recap.build/docs/integrations/avro/) | ✅ | ✅ |
| [Protobuf](https://recap.build/docs/integrations/protobuf/) | ✅ | ✅ |
| [JSON Schema](https://recap.build/docs/integrations/json-schema/) | ✅ | ✅ |
| [Snowflake](https://recap.build/docs/integrations/snowflake/) | ✅ |  |
| [PostgreSQL](https://recap.build/docs/integrations/postgresql/) | ✅ |  |
| [MySQL](https://recap.build/docs/integrations/mysql/) | ✅ |  |
| [BigQuery](https://recap.build/docs/integrations/bigquery/) | ✅ |  |
| [Confluent Schema Registry](https://recap.build/docs/integrations/confluent-schema-registry/) | ✅ |  |
| [Hive Metastore](https://recap.build/docs/integrations/hive-metastore/) | ✅ |  |

## Install

Install Recap and all of its optional dependencies:

```bash
pip install 'recap-core[all]'
```

You can also select specific dependencies:

```bash
pip install 'recap-core[avro,kafka]'
```

See `pyproject.toml` for a list of optional dependencies.

## Usage

### CLI

Recap comes with a command line interface that can list and read schemas from external systems.

List the children of a URL:

```bash
recap ls postgresql://user:pass@host:port/testdb
```

```json
[
  "pg_toast",
  "pg_catalog",
  "public",
  "information_schema"
]
```

Keep drilling down:

```bash
recap ls postgresql://user:pass@host:port/testdb/public
```

```json
[
  "test_types"
]
```

Read the schema for the `test_types` table as a Recap struct:

```bash
recap schema postgresql://user:pass@host:port/testdb/public/test_types
```

```json
{
  "type": "struct",
  "fields": [
    {
      "type": "int64",
      "name": "test_bigint",
      "optional": true
    }
  ]
}
```

### Gateway

Recap comes with a stateless HTTP/JSON gateway that can list and read schemas from data catalogs and databases.

Start the server at [http://localhost:8000](http://localhost:8000):

```bash
recap serve
```

List the schemas in a PostgreSQL database:

```bash
curl http://localhost:8000/gateway/ls/postgresql://user:pass@host:port/testdb
```

```json
["pg_toast","pg_catalog","public","information_schema"]
```

And read a schema:

```bash
curl http://localhost:8000/gateway/schema/postgresql://user:pass@host:port/testdb/public/test_types
```

```json
{"type":"struct","fields":[{"type":"int64","name":"test_bigint","optional":true}]}
```

The gateway fetches schemas from external systems in realtime and returns them as Recap schemas.

An OpenAPI schema is available at [http://localhost:8000/docs](http://localhost:8000/docs).

### Registry

You can store schemas in Recap's schema registry.

Start the server at [http://localhost:8000](http://localhost:8000):

```bash
recap serve
```

Put a schema in the registry:

```bash
curl -X POST \
    -H "Content-Type: application/x-recap+json" \
    -d '{"type":"struct","fields":[{"type":"int64","name":"test_bigint","optional":true}]}' \
    http://localhost:8000/registry/some_schema
```

Get the schema (and version) from the registry:

```bash
curl http://localhost:8000/registry/some_schema
```

```json
[{"type":"struct","fields":[{"type":"int64","name":"test_bigint","optional":true}]},1]
```

Put a new version of the schema in the registry:

```bash
curl -X POST \
    -H "Content-Type: application/x-recap+json" \
    -d '{"type":"struct","fields":[{"type":"int32","name":"test_int","optional":true}]}' \
    http://localhost:8000/registry/some_schema
```

List schema versions:

```bash
curl http://localhost:8000/registry/some_schema/versions
```

```json
[1,2]
```

Get a specific version of the schema:

```bash
curl http://localhost:8000/registry/some_schema/versions/1
```

```json
[{"type":"struct","fields":[{"type":"int64","name":"test_bigint","optional":true}]},1]
```

The registry uses [fsspec](https://filesystem-spec.readthedocs.io/en/latest/) to store schemas in a variety of filesystems like S3, GCS, ABS, and the local filesystem. See the [registry](https://recap.build/docs/registry/) docs for more details.

An OpenAPI schema is available at [http://localhost:8000/docs](http://localhost:8000/docs).

### API

Recap has `recap.converters` and `recap.clients` packages.

- Converters convert schemas to and from Recap schemas.
- Clients read schemas from external systems (databases, schema registries, and so on) and use converters to return Recap schemas.

Read a schema from PostgreSQL:

```python
from recap.clients import create_client

with create_client("postgresql://user:pass@host:port/testdb") as c:
    c.schema("testdb", "public", "test_types")
```

Convert the schema to Avro, Protobuf, and JSON schemas:

```python
from recap.converters.avro import AvroConverter
from recap.converters.protobuf import ProtobufConverter
from recap.converters.json_schema import JSONSchemaConverter

avro_schema = AvroConverter().from_recap(struct)
protobuf_schema = ProtobufConverter().from_recap(struct)
json_schema = JSONSchemaConverter().from_recap(struct)
```

Transpile schemas from one format to another:

```python
from recap.converters.json_schema import JSONSchemaConverter
from recap.converters.avro import AvroConverter

json_schema = """
{
    "type": "object",
    "$id": "https://recap.build/person.schema.json",
    "properties": {
        "name": {"type": "string"}
    }
}
"""

# Use Recap as an intermediate format to convert JSON schema to Avro
struct = JSONSchemaConverter().to_recap(json_schema)
avro_schema = AvroConverter().from_recap(struct)
```

Store schemas in Recap's schema registry:

```python
from recap.storage.registry import RegistryStorage
from recap.types import StructType, IntType

storage = RegistryStorage("file:///tmp/recap-registry-storage")
version = storage.put(
    "postgresql://localhost:5432/testdb/public/test_table",
    StructType(fields=[IntType(32)])
)
storage.get("postgresql://localhost:5432/testdb/public/test_table")

# Get all versions of a schema
versions = storage.versions("postgresql://localhost:5432/testdb/public/test_table")

# List all schemas in the registry
schemas = storage.ls()
```

### Docker

Recap's gateway and registry are also available as a Docker image:

```bash
docker run \
    -p 8000:8000 \
    -e RECAP_URLS=["postgresql://user:pass@localhost:5432/testdb"]' \
    ghcr.io/recap-build/recap:latest
```

See [Recap's Docker documentation](https://recap.build/docs/gateway/docker) for more details.

## Schema

See [Recap's type spec](https://recap.build/specs/type) for details on Recap's type system.

## Documentation

Recap's documentation is available at [recap.build](https://recap.build).

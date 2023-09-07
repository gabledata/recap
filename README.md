<div align="center">
  <img src="https://github.com/recap-cloud/recap/blob/main/static/recap-logo.png?raw=true" alt="recap">
</div>

## What is Recap?

Recap reads and writes schemas from web services, databases, and schema registries in a standard format.

## Table of Contents

* [What is Recap?](#what-is-recap)
* [Supported Formats](#supported-formats)
* [Install](#install)
* [Usage](#usage)
   * [CLI](#cli)
   * [Gateway](#gateway)
   * [API](#api)
   * [Docker](#docker)
* [Schema](#schema)
* [Documentation](#documentation)

## Supported Formats

| Format      | Read | Write |
| :---------- | :-: | :-: |
| [Avro](https://recap.build/docs/converters/avro/) | ✅ | ✅ |
| [Protobuf](https://recap.build/docs/converters/protobuf/) | ✅ | ✅ |
| [JSON Schema](https://recap.build/docs/converters/json-schema/) | ✅ | ✅ |
| [Snowflake](https://recap.build/docs/readers/snowflake/) | ✅ |  |
| [PostgreSQL](https://recap.build/docs/readers/postgresql/) | ✅ |  |
| [MySQL](https://recap.build/docs/readers/mysql/) | ✅ |  |
| [BigQuery](https://recap.build/docs/readers/bigquery/) | ✅ |  |
| [Confluent Schema Registry](https://recap.build/docs/readers/confluent-schema-registry/) | ✅ |  |
| [Hive Metastore](https://recap.build/docs/readers/hive-metastore/) | ✅ |  |

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

Recap comes with a command line interface that can list and read schemas.

Configure Recap to connect to one or more of your systems:

```bash
recap add my_pg postgresql://user:pass@host:port/dbname
```

List the paths in your system:

```bash
recap ls my_pg
```

```json
[
  "postgres",
  "template0",
  "template1",
  "testdb"
]
```

Recap models Postgres paths as `system/database/schema/table`. Keep drilling down:

```bash
recap ls my_pg/testdb
```

```json
[
  "pg_toast",
  "pg_catalog",
  "public",
  "information_schema"
]
```

Now we have a path to a testdb's public schemas:

```bash
recap ls my_pg/testdb/public
```

```json
[
  "test_types"
]
```

Read the schema:

```bash
recap schema my_pg/testdb/public/test_types
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

Recap comes with a stateless HTTP/JSON gateway that can list and read schemas.

Configure Recap to connect to one or more of your systems:

```bash
recap add my_pg postgresql://user:pass@host:port/dbname
```

Start the server at [http://localhost:8000](http://localhost:8000):

```bash
recap serve
```

List the schemas in your system:

```bash
$ curl http://localhost:8000/ls/my_pg
```

```json
["postgres","template0","template1","testdb"]
```

And read a schema:

```bash
curl http://localhost:8000/schema/my_pg/testdb/public/test_types
```

```json
{"type":"struct","fields":[{"type":"int64","name":"test_bigint","optional":true}]}
```

The gateway fetches schemas from external systems in realtime and returns them as Recap schemas.

An OpenAPI schema is available at [http://localhost:8000/docs](http://localhost:8000/docs).

### API

Recap has `recap.converters` and `recap.clients` packages.

- Converters convert schemas to and from Recap schemas.
- Clients read schemas from external systems (databases, schema registries, and so on) and use converters to return Recap schemas.

Read a schema from PostgreSQL:

```python
from recap.clients import create_client

client = create_client("postgresql://user:pass@host:port/dbname")
struct = client.get_schema("testdb", "public", "test_types")
```

Convert the schema to Avro, Protobuf, and JSON schemas:

```python
from recap.converters.avro import AvroConverter
from recap.converters.protobuf import ProtobufConverter
from recap.converters.json_schema import JsonSchemaConverter

avro_schema = AvroConverter().from_recap(struct)
protobuf_schema = ProtobufConverter().from_recap(struct)
json_schema = JsonSchemaConverter().from_recap(struct)
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

### Docker

Recap's gateway is also available as a Docker image:

```bash
docker run \
    -p 8000:8000 \
    -e "RECAP_SYSTEMS__PG=postgresql://user:pass@localhost:5432/testdb" \
    ghcr.io/recap-build/recap:latest
```

See [Recap's Docker documentation](https://recap.build/docs/gateway/docker) for more details.

## Schema

See [Recap's type spec](https://recap.build/specs/type) for details on Recap's type system.

## Documentation

Recap's documentation is available at [recap.build](https://recap.build).

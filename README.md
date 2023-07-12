<div align="center">
  <img src="https://github.com/recap-cloud/recap/blob/main/static/recap-logo.png?raw=true" alt="recap"></a>
</div>

## What is Recap?

Recap is a Python library that reads and converts web service schemas, database schemas, and schema registry schemas in a standard way.

Your data passes through web services, databases, message brokers, and object stores. Recap describes these schemas in a single language, regardless of which system your data passes through.

## Format

Recap schemas use [Recap's type spec](https://recap.build/spec). Schemas can be serialized in YAML, TOML, JSON, XML, or any other compatible language. Here’s a YAML example:

```yaml
type: struct
fields:
  - name: id
    type: int
    bits: 64
    signed: false
  - name: email
    type: string
    bytes: 255
```

## Features

Get Recap schemas for...

* Serialization formats ([Avro](https://avro.apache.org), [Protobuf](https://protobuf.dev), and [JSON Schema](https://json-schema.org))
* Databases ([Snowflake](https://www.snowflake.com) and [PostgreSQL](https://www.postgresql.org))
* Schema registries ([Confluent Schema Registry](https://github.com/confluentinc/schema-registry) and [Hive Metastore](https://cwiki.apache.org/confluence/display/hive/design#Design-Metastore))

## Usage

Install Recap:

```bash
pip install recap-core
```

Get a Recap schema from a Protobuf message:

```python
from recap.converters.protobuf import ProtobufConverter

protobuf_schema = """
message Person {
    string name = 1;
}
"""

recap_schema = ProtobufConverter().to_recap(protobuf_schema)
```

Or a Snowflake table:

```python
import snowflake.connector
from recap.readers.snowflake import SnowflakeReader

with snowflake.connector.connect(...) as conn:
  recap_schema = SnowflakeReader(conn).to_recap("TABLE", "PUBLIC", "TESTDB")
```

Or Hive's Metastore:

```python
from pymetastore import HMS
from recap.readers.hive_metastore import HiveMetastoreReader

with HMS.create(...) as conn:
  recap_schema = HiveMetastoreReader(conn).to_recap("testdb", "table")
```

## Warning

Recap is still a little baby application. It's going to wake up crying in the middle of the night. It's going to vomit on the floor once in a while. But if you give it some love and care, it'll be worth it. As time goes on, it'll grow up and be more mature. Bear with it.

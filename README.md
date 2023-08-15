<div align="center">
  <img src="https://github.com/recap-cloud/recap/blob/main/static/recap-logo.png?raw=true" alt="recap"></a>
</div>

## What is Recap?

Recap is a Python library that reads and writes schemas from web services, databases, and schema registries in a standard format.

## Use Cases

* Compare schemas
* Check schema compatibility
* Store schemas in a catalog or registry
* Transpile schemas
* Transform schemas

## Supported Formats

| Format      | Read | Write |
| ----------- | ----------- | ----------- |
| [Avro](https://recap.build/docs/converters/avro/) | ✅ | ✅ |
| [Protobuf](https://recap.build/docs/converters/protobuf/) | ✅ | ✅ |
| [JSON Schema](https://recap.build/docs/converters/json-schema/) | ✅ | ✅ |
| [Snowflake](https://recap.build/docs/readers/snowflake/) | ✅ |  |
| [PostgreSQL](https://recap.build/docs/readers/postgresql/) | ✅ |  |
| [MySQL](https://recap.build/docs/readers/mysql/) | ✅ |  |
| [BigQuery](https://recap.build/docs/readers/bigquery/) | ✅ |  |
| [Confluent Schema Registry](https://recap.build/docs/readers/confluent-schema-registry/) | ✅ |  |
| [Hive Metastore](https://recap.build/docs/readers/hive-metastore/) | ✅ |  |

## Documentation

Recap's documentation is available at [recap.build](https://recap.build).

## Supported Types

Recap borrows types from [Apache Arrow](https://arrow.apache.org/)'s [Schema.fbs](https://github.com/apache/arrow/blob/main/format/Schema.fbs) and [Apache Kafka](https://kafka.apache.org/)'s [Schema.java](https://github.com/apache/kafka/blob/trunk/connect/api/src/main/java/org/apache/kafka/connect/data/Schema.java).

* null
* bool
* int
* float
* string
* bytes
* list
* map
* struct
* enum
* union

## Recap Format

Recap schemas can be stored in YAML, TOML, or JSON formats using [Recap's type spec](https://recap.build/specs/type). Here’s a YAML example:

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

## Usage

Install Recap:

```bash
pip install recap-core
```

Get a Recap schema from a Protobuf schema:

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

And write the schema as an Avro schema:

```python
from recap.converters.avro import AvroConverter
avro_schema = AvroConverter().from_recap(recap_schema)
```

Or as a Protobuf schema:

```python
from recap.converters.protobuf import ProtobufConverter
protobuf_schema = ProtobufConverter().from_recap(recap_schema)
```

## Warning

Recap is still a little baby application. It's going to wake up crying in the middle of the night. It's going to vomit on the floor once in a while. But if you give it some love and care, it'll be worth it. As time goes on, it'll grow up and be more mature. Bear with it.

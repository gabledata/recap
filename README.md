<h1 align="center">
  <img src="https://github.com/recap-cloud/recap/blob/main/static/recap-logo.png?raw=true" alt="recap"></a>
  <br>
</h1>

<p align="center">
<i>A metadata toolkit written in Python</i>
</p>

<p align="center">
<a href="https://github.com/recap-cloud/recap/actions"><img alt="Actions Status" src="https://github.com/recap-cloud/recap/actions/workflows/ci.yaml/badge.svg"></a>
<a href="https://pycqa.github.io/isort/"><img alt="Imports: isort" src="https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336"></a>
<a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>
<a href="https://github.com/PyCQA/pylint"><img alt="pylint" src="https://img.shields.io/badge/linting-pylint-yellowgreen"></a>
</p>

## About

Recap reads and converts schemas in dozens of formats including [Parquet](https://parquet.apache.org), [Avro](https://avro.apache.org), and [JSON schema](https://json-schema.org), [BigQuery](https://cloud.google.com/bigquery), [Snowflake](https://www.snowflake.com/), and [PostgreSQL](https://www.postgresql.org/).

## Features

* Read schemas from filesystems, object stores, and databases.
* Convert schemas between [Parquet](https://parquet.apache.org), [Avro](https://avro.apache.org), and [JSON schema](https://json-schema.org).
* Generate `CREATE TABLE` DDL from schemas for popular database SQL dialects.
* Infer schemas from unstructured data like CSV, TSV, and JSON.

## Compatibility

* Any [SQLAlchemy-compatible](https://docs.sqlalchemy.org/en/13/dialects/) database
* Any [fsspec-compatible](https://filesystem-spec.readthedocs.io) filesystem
* [Parquet](https://parquet.apache.org), [Avro](https://avro.apache.org), and [JSON schema](https://json-schema.org)
* CSV, TSV, and JSON files

## Installation

    pip install recap-core

## Examples

Read schemas from filesystems:

```python
s = schema("s3://corp-logs/2022-03-01/0.json")
```

And databases:

```python
s = schema("snowflake://ycbjbzl-ib10693/TEST_DB/PUBLIC/311_service_requests")
```

And convert them to other formats:

```python
to_json_schema(s)
```
```json
{
  "type": "object",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "properties": {
    "id": {
      "type": "integer"
    },
    "name": {
      "type": "string"
    }
  },
  "required": [
    "id"
  ]
}
```

Or even `CREATE TABLE` statements:

```python
s = schema("/tmp/data/file.json")
to_ddl(s, "my_table", dialect="snowflake")
```
```sql
CREATE TABLE "my_table" (
  "col1" BIGINT,
  "col2" STRUCT<"col3" VARCHAR>
)
```

## Getting Started

See the [Quickstart](https://docs.recap.cloud/latest/quickstart) page to get started.

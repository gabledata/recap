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

# About

Recap is a Python library that helps you build tools for data quality, data governance, data profiling, data lineage, data contracts, and schema conversion.

## Features

* Compatible with [fsspec](https://filesystem-spec.readthedocs.io/en/latest/) filesystems and [SQLAlchemy](https://www.sqlalchemy.org) databases.
* Built-in support for [Parquet](https://parquet.apache.org), CSV, TSV, and JSON files.
* Includes [Pandas](https://pandas.pydata.org) for data profiling.
* Uses [Pydantic](https://pydantic.dev) for metadata models.
* Convenient [CLI](cli.md), [Python API](api/recap.analyzers.md), and [REST API](rest.md)
* No external system dependencies.

## Installation

    pip install recap-core

## Usage

Grab schemas from filesystems:

```python
schema("s3://corp-logs/2022-03-01/0.json")
```

And databases:

```python
schema("snowflake://ycbjbzl-ib10693/TEST_DB/PUBLIC/311_service_requests")
```

In a standardized format:

```json
{
  "fields": [
    {
      "name": "unique_key",
      "type": "VARCHAR",
      "nullable": false,
      "comment": "The service request tracking number."
    },
    {
      "name": "complaint_description",
      "type": "VARCHAR",
      "nullable": true,
      "comment": "Service request type"
    }
  ]
}
```

See what schemas used to look like:

```python
schema("snowflake://ycbjbzl-ib10693/TEST_DB/PUBLIC/311_service_requests", datetime(2023, 1, 1))
```

Build metadata extractors:

```python
@registry.metadata("s3://{path:path}.json", include_df=True)
@registry.metadata("bigquery://{project}/{dataset}/{table}", include_df=True)
def pandas_describe(df: DataFrame, *_) -> BaseModel:
    description_dict = df.describe(include="all")
    return PandasDescription.parse_obj(description_dict)
```

Crawl your data:

```python
crawl("s3://corp-logs")
crawl("bigquery://floating-castle-728053")
```

And read the results:

```python
search("json_extract(metadata_obj, '$.count') > 9999", PandasDescription)
```

See where data comes from:

```python
writers("bigquery://floating-castle-728053/austin_311/311_service_requests")
```

And where it's going:

```python
readers("bigquery://floating-castle-728053/austin_311/311_service_requests")
```

All cached in Recap's catalog.

## Getting Started

See the [Quickstart](docs/quickstart.md) page to get started.

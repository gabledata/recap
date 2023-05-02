<h1 align="center">
  <img src="https://github.com/recap-cloud/recap/blob/main/static/recap-logo.png?raw=true" alt="recap"></a>
  <br>
</h1>

<p align="center">
<i>A Schema Language Compatible With JSON, Protobuf, Avro, SQL, Parquet, Arrow, and More...</i>
</p>

## What is Recap?

Recap is a schema language and multi-language toolkit to track and transform schemas across your whole application.

Your data passes through web services, databases, message brokers, and object stores. Recap describes these schemas in a single language, regardless of which system your data passes through.

## Format

Recap schemas can be defined in YAML, TOML, JSON, XML, or any other compatible language. Here’s a YAML example:

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

Read [Recap's type spec](https://recap.build/spec) for more information.

## Tools

* **Converter**: Convert schemas from one format to another.
* **Schema Registry**: Track schemas in a registry.
* **Crawler**: Discover schemas and store them in the schema registry.

## Use Cases

Build one set of schema management tools for your whole ecosystem.

* Compare schemas
* Check schema compatibility
* Store schemas in a catalog or registry
* Transpile schemas
* Transform schemas

## Non-Goals

Recap is a user-friendly, approachable schema language. Recap is not a...

* Serialization format (Protobuf, Avro)
* Programmable type system (CUE)
* Templating system (Jsonnet)
* In-memory analytics format (Arrow)
* DB migration tool (Alembic, Flyway)

## Getting Started

Read [Recap's type spec](https://recap.build/spec) to start writing recap schemas.

## Warning

Recap is still a little baby application. It's going to wake up crying in the middle of the night. It's going to vomit on the floor once in a while. But if you give it some love and care, it'll be worth it. As time goes on, it'll grow up and be more mature. Bear with it.

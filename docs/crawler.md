Recap's crawler does three things:

1. Browses the configured infrastructure
2. Analyzes the infrastructure's data to generate metadata
3. Stores the metadata in Recap's data catalog

## Behavior

Recap's crawler is very simple right now. The crawler recursively browses and analyzes all children starting from an infrastructure's root location.

!!! note
    The meaning of an infrastructure's _root_ location depends on its type. For a database, the _root_ usually denotes a database or catalog (to use [_information_schema_](https://en.wikipedia.org/wiki/Information_schema) terminology). For object stores, the _root_ is usually the bucket location.

## Scheduling

Recap's crawler does not have a built in scheduler or orchestrator. You can run crawls manually with `recap crawl`, or you can schedule `recap crawl` to run periodically using [cron](https://en.wikipedia.org/wiki/Cron), [Airflow](https://airflow.apache.org), [Prefect](https://prefect.io), [Dagster](https://dagster.io/), [Modal](https://modal.com), or any other scheduler.

## Commands

Run `recap crawl` to begin crawling all configured infrastructure. You may optionall specify a URL to crawl:

    recap crawl postgresql://username@localhost/some_db

See the [commands](commands.md) page for crawler command details.

## Configuration

See the [configuration](configuration.md) page for crawler configuration details.

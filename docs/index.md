# Recap

Recap makes it easy for engineers to build infrastructure and tools that need metadata. Unlike traditional data catalogs, Recap is designed to power software. Read [Recap: A Data Catalog for People Who Hate Data Catalogs](https://cnr.sh/essays/recap-for-people-who-hate-data-catalogs) to learn more.

## Features

* Supports major cloud data warehouses and PostgreSQL
* No external system dependencies required
* Designed for the [CLI](commands.md)
* Runs as a Python library or [REST API](server.md)
* Fully [pluggable](plugins.md)

## Installation

    pip install recap-core

## Commands

* `recap catalog list` - List a data catalog directory.
* `recap catalog read` - Read metadata from the data catalog.
* `recap catalog search` - Search the data catalog for metadata.
* `recap crawl` - Crawl infrastructure and save metadata in the data catalog.
* `recap plugins analyzers` - List all analyzer plugins.
* `recap plugins browsers` - List all browser plugins.
* `recap plugins catalogs` - List all catalog plugins.
* `recap plugins commands` - List all command plugins.
* `recap serve` - Start Recap's REST API.

## Getting Started

See the [Quickstart](quickstart.md) page to get started.

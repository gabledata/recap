<h1 align="center">
  <img src="https://github.com/recap-cloud/recap/blob/main/static/recap-logo.png?raw=true" alt="recap"></a>
  <br>
</h1>

<p align="center">
<i>A dead simple data catalog for engineers</i>
</p>

## About

Recap makes it easy for engineers to build infrastructure and tools that need metadata. Unlike traditional data catalogs, Recap is designed to power software. Read [Recap: A Data Catalog for People Who Hate Data Catalogs](https://cnr.sh/essays/recap-for-people-who-hate-data-catalogs) to learn more.

## Features

* Supports major cloud data warehouses and Postgres
* No external system dependencies required
* Designed for the [CLI](https://docs.recap.cloud/latest/commands/)
* Runs as a Python library or [REST API](https://docs.recap.cloud/latest/server/)
* Fully [pluggable](https://docs.recap.cloud/latest/plugins/)

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

See the [Quickstart](https://docs.recap.cloud/latest/quickstart/) page to get started.

## Warning

> ⚠️ This package is still under development and may not be stable. The API may break at any time.

Recap is still a little baby application. It's going to wake up crying in the middle of the night. It's going to vomit on the floor once in a while. But if you give it some love and care, it'll be worth it. As time goes on, it'll grow up and be more mature. Bear with it.

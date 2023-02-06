<h1 align="center">
  <img src="https://github.com/recap-cloud/recap/blob/main/static/recap-logo.png?raw=true" alt="recap"></a>
  <br>
</h1>

<p align="center">
<i>A dead simple data catalog for engineers</i>
</p>

<p align="center">
<a href="https://github.com/recap-cloud/recap/actions"><img alt="Actions Status" src="https://github.com/recap-cloud/recap/actions/workflows/ci.yaml/badge.svg"></a>
<a href="https://pycqa.github.io/isort/"><img alt="Imports: isort" src="https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336"></a>
<a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>
<a href="https://github.com/PyCQA/pylint"><img alt="pylint" src="https://img.shields.io/badge/linting-pylint-yellowgreen"></a>
</p>

## About

Recap makes it easy for engineers to build infrastructure and tools that need metadata. Unlike traditional data catalogs, Recap is designed to power software. Read [Recap: A Data Catalog for People Who Hate Data Catalogs](https://cnr.sh/essays/recap-for-people-who-hate-data-catalogs) to learn more.

## Features

* Supports major cloud data warehouses and Postgres
* No external system dependencies required
* Designed for the [CLI](https://docs.recap.cloud/latest/cli/)
* Runs as a [Python API](https://docs.recap.cloud/latest/api/recap.analyzers/) or [REST API](https://docs.recap.cloud/latest/rest/)
* Fully [pluggable](https://docs.recap.cloud/latest/guides/plugins/)

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

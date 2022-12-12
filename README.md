# Recap

Recap crawls, indexes, and serves metadata.

## What is Recap?

Recap is a light-weight metadata service for engineering and operations teams. Recap crawls your data and collects schema, statistics, and usage information. Recap serves this metadata through a REST API.

You can use Recap to answer questions like:

* What tables are in this database?
* Which tables have this column?
* Who has access to this data?
* What is this data's schema?
* What is this data's count/max/min/stddev/cardinality?
* Does this data have PII?
* How big is this data?
* When was the data created, updated?
* Where did this data come from?
* Where does this data go to?

## What is Recap good for?

Recap is good for building scripts, tools, and UIs for data:

* Observability
* Monitoring
* Debugging
* Security
* Compliance
* Governance

## What is Recap not good for?

* Manual annotation and curation
* Non-technical users

## How do I use Recap?

### API

Tell Recap to crawl something:

```
/crawl/<infra>/<instance>/<path...>
/crawl/kafka/tracking_cluster
/crawl/mysql/LedgerDB
/crawl/bigquery/vanilla-bus-892345
/crawl/s3/prod_logs/2022-12-06
```

Tell Recap to index something:

```
/index/<infra>/<instance>/<path...>
/index/kafka/tracking_cluster/PageViews
/index/mysql/LedgerDB/ledger/accounts
/index/bigquery/vanilla-bus-892345/dm_users/addresses
/index/s3/prod_logs/2022-12-06/00000.log
```

Search metadata:

```
/search?infra=kafka&instance=tracking_cluster
```

### CLI

recap ls /<infra>/<instance>/<path...>
recap ls /kafka/tracking_cluster # Cluster
recap ls /kafka/tracking_cluster/PageViews # Topic
recap ls /mysql
recap ls /mysql/LedgerDB # Host
recap ls /mysql/LedgerDB/ledger # DB
recap ls /mysql/LedgerDB/ledger/accounts # Table
recap ls /bigquery
recap ls /bigquery/vanilla-bus-892345 # Project
recap ls /bigquery/vanilla-bus-892345/dm_users # Dataset
recap ls /bigquery/vanilla-bus-892345/dm_users/addresses # Table
recap ls /s3
recap ls /s3/prod_logs # Bucket
recap ls /s3/prod_logs/2022-12-06 # Dir
recap ls /s3/prod_logs/2022-12-06/00000.log # File
recap stat /kafka/tracking_cluster/PageViews
recap stat /bigquery/vanilla-bus-892345/dm_users/addresses
recap find --col=user_id --infra=bq
recap find --user=criccomini* --infra=mysql --tables

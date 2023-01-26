Different data infrastructure have different types of objects (tables, columns, files, topics, partitions, and so on).

Recap uses a _browser_ abstraction map infrastructure objects into a standard directory format. A different browser is used for each type of infrastructure. Browsers do not actually analyze a system's data for metadata; they simply show what's available.

Recap comes with a database browser and a filesystem browser. Other browsers can be implemented and added as [plugins](plugins.md#browsers).

## Root Paths

Browsers expose a _root_ path, which defines where a data system will be located in Recap's data catalog. The [DatabaseBrowser](https://github.com/recap-cloud/recap/blob/main/recap/browsers/db.py)'s root path format is:

    /databases/{scheme}/instances/{name}

So, a DatabaseBrowser for a PostgreSQL might have a root path like:

    /databases/postgresql/instances/prd-profile-db

## Database Browser

[DatabaseBrowser](https://github.com/recap-cloud/recap/blob/main/recap/browsers/db.py) browses different databases and maps their database objects into a directory structure:

```
/schemas
/schemas/{schema}
/schemas/{schema}/tables
/schemas/{schema}/tables/{table}
/schemas/{schema}/views
/schemas/{schema}/views/{view}
```

This full directory path (including the root) is what you use when you run `recap catalog list` and `recap catalog read`. The directory structure is also used when executing `recap crawl` with a `--filter` option.

!!! note

    Recap uses [SQLAlchemy](https://www.sqlalchemy.org) as its database abstraction, so any [SQLAlchemy dialect](https://docs.sqlalchemy.org/en/14/dialects/) should work if you've installed the appropriate package.

## Filesystem Browser

[FilesystemBrowser](https://github.com/recap-cloud/recap/blob/main/recap/browsers/fs.py) browses both local filesystems and remote object stores like S3, GCS, and ABS. The directory structure format mirrors the crawled filesystem.

!!! note

    Recap uses [fsspec](https://github.com/fsspec/filesystem_spec) as its filesystem abstraction, so any known [built-in](https://filesystem-spec.readthedocs.io/en/latest/api.html#built-in-implementations) or [other known implementation](https://filesystem-spec.readthedocs.io/en/latest/api.html#other-known-implementations) should work.

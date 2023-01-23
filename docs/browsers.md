Different data infrastructure have different types of objects:

* schemas
* tables
* columns
* files
* paths
* topics
* partitions

Recap uses a _browser_ abstraction to deal with different data infrastructure in a standard way.

!!! note

    Browsers do not actually analyze a system's data for metadata, they simply show what's available.

Browsers map infrastructure objects into a standard directory format. A different browser is used for each type of infrastructure. Out of the box, Recap ships with a [DatabaseBrowser](https://github.com/recap-cloud/recap/blob/main/recap/browsers/db.py), which maps database objects into a directory structure:

```
/schemas
/schemas/{schema}
/schemas/{schema}/tables
/schemas/{schema}/tables/{table}
/schemas/{schema}/views
/schemas/{schema}/views/{view}
```

Browsers also expose a _root_ path. The [DatabaseBrowser](https://github.com/recap-cloud/recap/blob/main/recap/browsers/db.py)'s root path format is:

    /databases/{scheme}/instances/{name}

So, a DatabaseBrowser for a PostgreSQL might have a path like:

    /databases/postgresql/instances/prd-profile-db

This full directory path (including the root) is what you use when you run `recap catalog list` and `recap catalog read`. The directory structure is also used when executing `recap crawl` with a `--filter` option.

## Commands

### Browse

`recap browse` fetches children for a path in a system rather than going through Recap's catalog. See the [commands](commands.md#browse) page for more information.

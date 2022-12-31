Different data infrastructure have different types of objects:

* schemas
* tables
* columns
* files
* paths
* topics
* partitions

Recap uses a _browser_ abstraction to deal with different data infrastructure in a standard way.

Browsers map infrastructure objects into a standard directory format. A different browser is used for each type of infrastructure. Out of the box, Recap ships with a [DatabaseBrowser](https://github.com/recap-cloud/recap/blob/main/recap/plugins/browsers/db.py), which maps database objects into a directory structure:

```
/
/databases
/databases/<some_db>
/databases/<some_db>/instances
/databases/<some_db>/instances/<some_instance>
/databases/<some_db>/instances/<some_instance>/schemas
/databases/<some_db>/instances/<some_instance>/schemas/<some_schema>
/databases/<some_db>/instances/<some_instance>/schemas/<some_schema>/tables
/databases/<some_db>/instances/<some_instance>/schemas/<some_schema>/tables/<some_view>
/databases/<some_db>/instances/<some_instance>/schemas/<some_schema>/views
/databases/<some_db>/instances/<some_instance>/schemas/<some_schema>/views/<some_view>
```

!!! note

    Browsers do not actually analyze a system's data for metdata, they simply show what's available.

This directory path is what you use when you run `recap catalog list` and `recap catalog read`. The directory structure is also used when executing `recap crawl` with a `--filter` option.

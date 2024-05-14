# logdk
logdk is a log collector and viewer powered by DuckDB, a fast in-process analytical database. logdk is a self-hosted, efficient log aggregator that speaks SQL and can ingest and analyze a large volume of logs. It is loosely designed around the idea of "wide events". The schema auto-adjusts based on the shape of your logs.

Focus is currently on the more basic, but still fundamental, log ingestion and viewing. However, I believe that by leveraging DuckDB powerful analytics can be made available to everyone in a cost-effective package.

A live demo is available at <https://logdk.goblgobl.com>.

Documentation is available at <https://www.goblgobl.com/docs/logdk>.

# Running
Download the latest precompiled binary for your platform from the [release section](https://github.com/karlseguin/logdk/releases). The archive contains the logdk binary and the duckdb library.

Run the executable and open http://127.0.0.1:7714.

logdk [does not support TLS](https://github.com/ziglang/zig/issues/14171). It should not be exposed directly to the internet.

## DataSets
The primary organization unit is a "dataset" which maps directly to a DuckDB table. It is OK to have wide datasets with many null columns. logdk automatically alters the dataset (and the underlying table) to accommodate your data. This can involve adding a column, making a column nullable, or changing the column data type.

## API

### Create Events
`POST /api/1/datasets/NAME/events` is used to send events to logdk. The body payload can either be a single object or an array of objects (for bulk insertion).

Events are inserted asynchronously, and are flushed every 10000 events or every 5 seconds, on a per-dataset basis. (TODO: make these configuration). Flushing may occasionally happen more often. 

## Shutdown
When possible, logdk should be shutdown via SIGINT or SIGTERM to ensure all pending messages are flushed. Such graceful shutdown is not [currently possible on Windows](https://github.com/karlseguin/logdk/issues/1).

## Config
The `--config PATH` flag can be used to specify the path a JSON configuration file. By default, logdk will attempt to load `config.json` from the current working directory.

```json
{
    "db": {
        "path": "db.duckdb",
        "pool_size": 20,
    },
    "http": {
        "port": 7714,
        "address": "127.0.0.1"
    },
    "log_http": "smart", 
    "logger": {
        "level": "Error"
    }
}
```

(`log_http` can be one of; "off", "all" or "smart". "smart", the default, does not log some HTTP request so that logdk logs can themselves be ingested into logdk without causing an endless loop).



## Roadmap
The following large features are planned:
* Basic event parsing to compensate for the lack of datetime type in JSON and to support basic parsing of numbers and booleans sent as string values.
* Authentication and authorization. Very simple, with a few permissions, largely aimed at protecting "admin" features. No plan to support multi-tenancy, single sign on, row level security, or anything more advanced.
* Administration utilities to manage and customize logdk and the logged data.
* Analytics in the shape of canned reports, ad-hoc analysis tools, dashboards. This is why DuckDB was chosen and, in the end, the main focus on the project.

## Development
logdk is written in [Zig](https://ziglang.org/). The master branch of Zig is followed (up to a couple weeks behind). If you want to learn zig, checkout my [learning zig](https://www.openmymind.net/learning_zig/) series.

The UI is developed in the [logdk-ui](https://github.com/karlseguin/logdk-ui) repository and compiled into the logdk binary as part of the build process.

`duckdb.h` and libduckdb must be available. 

On Linux, [download libduckdb-linux-amd64.zip](https://github.com/duckdb/duckdb/releases/download/v0.10.2/libduckdb-linux-amd64.zip) and place `duckdb.h` and `libduckdb.so` in the root of the repository (alongside this readme).

On MacOs, [download libduckdb-osx-universal.zip](https://github.com/duckdb/duckdb/releases/download/v0.10.2/libduckdb-osx-universal.zip) and place `duckdb.h` and `libduckdb.dylib` in the root of the repository (alongside this readme). 

On Windows, [download libduckdb-windows-amd64.zip](https://github.com/duckdb/duckdb/releases/download/v0.10.2/libduckdb-windows-amd64.zip) and  place `duckdb.h` and `duckdb.dll` in the root of the repository (alongside this readme). 

If you have zig locally installed, you should be able to check this project out and run `make t` to run unit test and `make s` to start a local copy.

Once someone shows interest in contributing, I'm happy to provide a more detailed description of the project/source to help onboard developers.

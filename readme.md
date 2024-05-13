# logdk
logdk is a log collector and viewer powered by DuckDB, a fast in-process analytical database. logdk is a self-hosted, efficient log aggregator that speaks SQL and can ingest and analyze a large volume of logs. It is loosely designed around the idea of "wide events". The schema auto-adjusts based on the shape of your logs.

A live demo is available at <https://logdk.goblgobl.com>.

Documentation is available at <https://www.goblgobl.com/docs/logdk>.

## Roadmap
logdk is in early development. Focus is currently on the more basic, but fundamental, log viewing and exploring.

The following features are planned:
* Basic event parsing to compensate for the lack of datetime type in JSON and to support basic parsing of numbers and booleans sent as string values.
* Authentication and authorization. Very simple, with a few permissions, largely aimed at protecting "admin" features. No plan to support multi-tenancy, single sign on, row level security, or anything more advanced.
* Administration utilities to manage and customize logdk and the logged data.
* Analytics in the shape of canned reports, ad-hoc analysis tools, dashboards. This is why DuckDB was chosen and, in the end, the main focus on the project.

## Development
logdk is written in [Zig](https://ziglang.org/). The master branch of Zig is followed (up to a couple weeks behind). If you want to learn zig, checkout my [learning zig](https://www.openmymind.net/learning_zig/) series.

The UI is developed in the [logdk-ui](https://github.com/karlseguin/logdk-ui) repository and compiled into the logdk binary as part of the build process.

If you have zig locally installed, you should be able to check this project out and run `make t` to run unit test and `make s` to start a local copy.

Once someone shows interest in contributing, I'm happy to provide a more detailed description of the project/source to help onboard developers.

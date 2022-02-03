# Stairlight

[![PyPi Version](https://img.shields.io/pypi/v/stairlight.svg?style=flat-square&logo=PyPi)](https://pypi.org/project/stairlight/)
[![PyPi License](https://img.shields.io/pypi/l/stairlight.svg?style=flat-square)](https://pypi.org/project/stairlight/)
[![PyPi Python Versions](https://img.shields.io/pypi/pyversions/stairlight.svg?style=flat-square)](https://pypi.org/project/stairlight/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg?style=flat-square)](https://github.com/psf/black)
[![CI](https://github.com/tosh2230/stairlight/actions/workflows/ci.yml/badge.svg)](https://github.com/tosh2230/stairlight/actions/workflows/ci.yml)

Stairlight is a table-level data lineage tool, detects table dependencies by SELECT queries.

Queries can be read from following systems.

- [Google Cloud Storage](https://cloud.google.com/storage)
    - Mainly designed for use with [Google Cloud Composer](https://cloud.google.com/composer)
- [Redash](https://redash.io/)
- Local file system(with Python Pathlib module)

## Installation

```sh
$ pip install stairlight
```

## Getting Started

There are 3 steps to use.

```sh
# Step 1: Initialize and set data location settings
$ stairlight init
'./stairlight.yaml' has created.
Please edit it to set your data sources.

# Step 2: Map SQL queries and tables, and add metadata
$ stairlight check
'./mapping_checked_yyyyMMddhhmmss.yaml' has created.
Please map undefined tables and parameters, and append to your latest configuration file.

# Step 3: Get a table dependency map
$ stairlight
```

## Description

### Input

- SQL `SELECT` queries
- Configuration files (YAML)
    - stairlight.yaml: SQL query locations and include/exclude conditions.
    - mapping.yaml: Mapping SQL queries and tables.

### Output

- Dependency map (JSON)

    <details>

    <summary>Example</summary>

    ```json
    {
        "PROJECT_d.DATASET_e.TABLE_f": {
            "PROJECT_j.DATASET_k.TABLE_l": {
                "TemplateSourceType": "File",
                "Key": "tests/sql/main/test_e.sql",
                "Uri": "/foo/bar/stairlight/tests/sql/main/test_e.sql",
                "Lines": [
                    {
                        "LineNumber": 1,
                        "LineString": "SELECT * FROM PROJECT_j.DATASET_k.TABLE_l WHERE 1 = 1"
                    }
                ]
            },
            "PROJECT_C.DATASET_C.TABLE_C": {
                "TemplateSourceType": "GCS",
                "Key": "sql/test_b/test_b.sql",
                "Uri": "gs://stairlight/sql/test_b/test_b.sql",
                "Lines": [
                    {
                        "LineNumber": 6,
                        "LineString": "        PROJECT_C.DATASET_C.TABLE_C"
                    }
                ],
                "BucketName": "stairlight",
                "Labels": {
                    "Source": "gcs",
                    "Test": "b"
                }
            }
        },
        "AggregateSales": {
            "PROJECT_e.DATASET_e.TABLE_e": {
                "TemplateSourceType": "Redash",
                "Key": 5,
                "Uri": "AggregateSales",
                "Lines": [
                    {
                        "LineNumber": 1,
                        "LineString": "SELECT service, SUM(total_amount) FROM PROJECT_e.DATASET_e.TABLE_e GROUP BY service"
                    }
                ],
                "DataSourceName": "BigQuery",
                "Labels": {
                    "Category": "Sales"
                }
            }
        },
    }
    ```

    </details>

## Configuration

Configuration files can be found [here](https://github.com/tosh2230/stairlight/tree/main/config), used for unit test in CI.

### stairlight.yaml

'stairlight.yaml' is for setting up Stairlight itself.

It is responsible for specifying the destination of SQL queries to be read, and for specifying data sources.

```yaml
Include:
  - TemplateSourceType: File
    FileSystemPath: "./tests/sql"
    Regex: ".*/*.sql$"
    DefaultTablePrefix: "PROJECT_A"
  - TemplateSourceType: GCS
    ProjectId: null
    BucketName: stairlight
    Regex: "^sql/.*/*.sql$"
    DefaultTablePrefix: "PROJECT_A"
  - TemplateSourceType: Redash
    DatabaseUrlEnvironmentVariable: REDASH_DATABASE_URL
    DataSourceName: BigQuery
    QueryIds:
      - 1
      - 3
      - 5
Exclude:
  - TemplateSourceType: File
    Regex: "main/test_exclude.sql$"
Settings:
  MappingPrefix: "mapping"
```

### mapping.yaml

'mapping.yaml' is used to define relationships between input queries and tables.

A template of this file can be created by `check` command, based on the configuration of 'stairlight.yaml'.

```yaml
Mapping:
  - TemplateSourceType: File
    FileSuffix: "tests/sql/main/test_union_same_table.sql"
    Tables:
      - TableName: "test_project.beam_streaming.taxirides_aggregation"
  - TemplateSourceType: GCS
    Uri: "gs://stairlight/sql/test_a/test_a.sql"
    Tables:
      - TableName: "PROJECT_a.DATASET_b.TABLE_c"
  - TemplateSourceType: Redash
    QueryId: 5
    DataSourceName: metadata
    Tables:
      - TableName: Copy of (#4) New Query
        Parameters:
          table: dashboards
        Labels:
          Category: Redash test
Metadata:
  - TableName: "PROJECT_A.DATASET_A.TABLE_A"
    Labels:
      Source: Null
      Test: a
```

#### mapping section

This section is used to define relationships between queries and tables that created as a result of query execution.

`Parameters` attribute allows you to reflect settings in [jinja](https://jinja.palletsprojects.com/) template variables embedded in queries. If multiple settings are applied to a query using jinja template, the query will be read as if there were the same number of queries as the number of settings.

#### metadata section

This section is mainly used to set metadata to tables appears only in queries.

## Command and option

```txt
$ stairlight --help
usage: stairlight [-h] [-c CONFIG] [--save SAVE | --load LOAD] {init,check,up,down} ...

A table-level data lineage tool, detects table dependencies by SELECT queries.
Without positional arguments, return a table dependency map as JSON format.

positional arguments:
  {init,check,up,down}
    init                create new Stairlight configuration file
    check               create new configuration file about undefined mappings
    up                  return upstairs ( table | SQL file ) list
    down                return downstairs ( table | SQL file ) list

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        set Stairlight configuration directory
  --save SAVE           file path where results will be saved(File system or GCS)
  --load LOAD           file path in which results are saved(File system or GCS)
```

### init

`init` creates a new Stairlight configuration file.

```txt
$ stairlight init --help
usage: stairlight init [-h] [-c CONFIG] [-s SAVE | -l LOAD]

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        set Stairlight configuration directory.
```

### check

`check` creates new configuration file about undefined mappings.
The option specification is the same as `init`.

### up

`up` outputs a list of tables or SQL files located upstream from the specified table.

- Use table(`-t`, `--table`) or label(`-l`, `--label`) option to specify tables to search.
- Recursive option(`-r`, `--recursive`) is set, Stairlight will find tables recursively and output as a list.
- Verbose option(`-v`, `--verbose`) is set, Stairlight will add detailed information and output it as a dict.

```txt
$ stairlight up --help
usage: stairlight up [-h] [-c CONFIG] [--save SAVE | --load LOAD] (-t TABLE | -l LABEL)
                  [-o {table,file}] [-v] [-r]

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        set Stairlight configuration directory
  --save SAVE           file path where results will be saved(File system or GCS)
  --load LOAD           file path in which results are saved(File system or GCS)
  -t TABLE, --table TABLE
                        table names that Stairlight searches for, can be specified
                        multiple times. e.g. -t PROJECT_a.DATASET_b.TABLE_c -t
                        PROJECT_d.DATASET_e.TABLE_f
  -l LABEL, --label LABEL
                        labels set for the table in mapping configuration, can be
                        specified multiple times. The separator between key and value
                        should be a colon(:). e.g. -l key_1:value_1 -l key_2:value_2
  -o {table,file}, --output {table,file}
                        output type
  -v, --verbose         return verbose results
  -r, --recursive       search recursively
```

### down

`down` outputs a list of tables or SQL files located downstream from the specified table.
The option specification is the same as `up`.

## Use as a library

Stairlight can also be used as a library.

[tosh2230/stairlight-app](https://github.com/tosh2230/stairlight-app) is a sample web application rendering table dependency graph with Stairlight, using Graphviz, Streamlit and Google Cloud Run.

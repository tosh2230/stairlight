<div align="center">
  <img src="img/stairlight_white.png" width="400" alt="Stairlight">
</div>

-----------------

# Stairlight

[![PyPi Version](https://img.shields.io/pypi/v/stairlight.svg?style=flat-square&logo=PyPi)](https://pypi.org/project/stairlight/)
[![PyPi License](https://img.shields.io/pypi/l/stairlight.svg?style=flat-square)](https://pypi.org/project/stairlight/)
[![PyPi Python Versions](https://img.shields.io/pypi/pyversions/stairlight.svg?style=flat-square)](https://pypi.org/project/stairlight/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg?style=flat-square)](https://github.com/psf/black)
[![CI](https://github.com/tosh2230/stairlight/actions/workflows/ci.yml/badge.svg)](https://github.com/tosh2230/stairlight/actions/workflows/ci.yml)

An end-to-end data lineage tool, detects table dependencies by SELECT queries.

## Supported Data Sources

- Local file system(with Python Pathlib module)
- [Google Cloud Storage(GCS)](https://cloud.google.com/storage)
    - Also available for [Google Cloud Composer](https://cloud.google.com/composer)
- [Redash](https://redash.io/)
- [dbt](https://www.getdbt.com/) (using `dbt compile` command internally)
    - Google BigQuery

## Installation

This package is distributed on PyPI.

```sh
$ pip install stairlight
```

(v0.4+) The base package is for Local file system only. Please set extras when reading from other data sources.

| TemplateSourceType | DataSource | Extra |
| --- | --- | --- |
| File | Local file system | - |
| GCS | Google Cloud Storage | gcs |
| Redash | Redash | redash |
| dbt | dbt(Google Bigquery) | dbt-bigquery |

```sh
# e.g. Read from GCS and Redash
$ pip install "stairlight[gcs,redash]"
```

## Getting Started

There are 3 steps to use.

```sh
# Step 1: Initialize and set data location settings
$ stairlight init
'./stairlight.yaml' has created.
Please edit it to set your data sources.

# Step 2: Map SQL queries and tables, and add metadata
$ stairlight map
'./mapping_yyyyMMddhhmmss.yaml' has created.
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
          "Key": "tests/sql/main/one_line_2.sql",
          "Uri": "/foo/bar/stairlight/tests/sql/main/one_line_2.sql",
          "Lines": [
            {
              "LineNumber": 1,
              "LineString": "SELECT * FROM PROJECT_j.DATASET_k.TABLE_l WHERE 1 = 1"
            }
          ]
        },
        "PROJECT_C.DATASET_C.TABLE_C": {
          "TemplateSourceType": "GCS",
          "Key": "sql/cte/cte_multi_line.sql",
          "Uri": "gs://stairlight/sql/cte/cte_multi_line.sql",
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
      "dummy.dummy.example_b": {
        "PROJECT_t.DATASET_t.TABLE_t": {
          "TemplateSourceType": "dbt",
          "Key": "tests/dbt/project_01/target/compiled/project_01/models/b/example_b.sql",
          "Uri": "/foo/bar/stairlight/tests/dbt/project_01/target/compiled/project_01/models/b/example_b.sql",
          "Lines": [
            {
              "LineNumber": 1,
              "LineString": "select * from PROJECT_t.DATASET_t.TABLE_t where value_a = 0 and value_b = 0"
            }
          ]
        }
      }
    }
    ```

    </details>

## Configuration

Configuration files can be found [here](https://github.com/tosh2230/stairlight/tree/main/tests/config), used for unit test in CI.

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
  - TemplateSourceType: dbt
    ProjectDir: tests/dbt/project_01
    ProfilesDir: tests/dbt
    Vars:
      key_a: value_a
      key_b: value_b
Exclude:
  - TemplateSourceType: File
    Regex: "main/exclude.sql$"
Settings:
  MappingPrefix: "mapping"
```

### mapping.yaml

'mapping.yaml' is used to define relationships between input queries and tables.

A template of this file can be created by `map` command, based on the configuration of 'stairlight.yaml'.

```yaml
Global:
  Parameters:
    DESTINATION_PROJECT: stairlight
    params:
      PROJECT: 1234567890
      DATASET: public
      TABLE: taxirides
Mapping:
  - TemplateSourceType: File
    FileSuffix: "tests/sql/main/union_same_table.sql"
    Tables:
      - TableName: "test_project.beam_streaming.taxirides_aggregation"
        Parameters:
          params:
            source_table: source
            destination_table: destination
        IgnoreParameters:
          - execution_date.add(days=1).isoformat()
  - TemplateSourceType: GCS
    Uri: "gs://stairlight/sql/one_line/one_line.sql"
    Tables:
      - TableName: "PROJECT_a.DATASET_b.TABLE_c"
  - TemplateSourceType: Redash
    QueryId: 5
    DataSourceName: metadata
    Tables:
      - TableName: New Query
        Parameters:
          table: dashboards
        Labels:
          Category: Redash test
  - TemplateSourceType: dbt
    ProjectName: project_01
    FileSuffix: tests/dbt/project_01/target/compiled/project_01/models/example/my_first_dbt_model.sql
    Tables:
      - TableName: dummy.dummy.my_first_dbt_model
Metadata:
  - TableName: "PROJECT_A.DATASET_A.TABLE_A"
    Labels:
      Source: Null
      Test: a
```

#### Global Section

This section is for global configurations.

`Parameters` attribute is used to set common parameters. If conflicts has occurred with `Parameters` attributes in mapping section, mapping section's parameters will be used in preference to global.

#### Mapping Section

Mapping section is used to define relationships between queries and tables that created as a result of query execution.

`Parameters` attribute allows you to reflect settings in [jinja](https://jinja.palletsprojects.com/) template variables embedded in queries. If multiple settings are applied to a query using jinja template, the query will be read as if there were the same number of queries as the number of settings.

In contrast, `IgnoreParameters` attribute handles a list to ignore when rendering queries.

#### Metadata Section

This section is mainly used to set metadata to tables appears only in queries.

## Command and Option

```txt
$ stairlight --help
usage: stairlight [-h] [-c CONFIG] [--save SAVE] [--load LOAD] {init,check,up,down} ...

An end-to-end data lineage tool, detects table dependencies by SELECT queries.
Without positional arguments, return a table dependency map as JSON format.

positional arguments:
  {init,map,check,up,down}
    init                create new Stairlight configuration file
    map (check)         create new configuration file about undefined mappings
    up                  return upstairs ( table | SQL file ) list
    down                return downstairs ( table | SQL file ) list

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        set Stairlight configuration directory
  -q, --quiet           keep silence
  --save SAVE           file path where results will be saved(File system or GCS)
  --load LOAD           file path in which results are saved(File system or GCS), can be specified multiple times
```

### init

`init` creates a new Stairlight configuration file.

```txt
$ stairlight init --help
usage: stairlight init [-h] [-c CONFIG]

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        set Stairlight configuration directory.
  -q, --quiet           keep silence
```

### map(check)

`map` creates new configuration file about undefined mappings.`check` is an alias.
The option specification is the same as `init`.

### up

`up` outputs a list of tables or SQL files located upstream from the specified table.

- Use table(`-t`, `--table`) or label(`-l`, `--label`) option to specify tables to search.
- Recursive option(`-r`, `--recursive`) is set, Stairlight will find tables recursively and output as a list.
- Verbose option(`-v`, `--verbose`) is set, Stairlight will add detailed information and output it as a dict.

```txt
$ stairlight up --help
usage: stairlight up [-h] [-c CONFIG] [--save SAVE] [--load LOAD] (-t TABLE | -l LABEL) [-o {table,file}]
                     [-v] [-r]

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        set Stairlight configuration directory
  -q, --quiet           keep silence
  --save SAVE           file path where results will be saved(File system or GCS)
  --load LOAD           file path in which results are saved(File system or GCS), can be specified multiple times
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

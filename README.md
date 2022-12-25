<div align="center">
  <img src="https://raw.githubusercontent.com/tosh2230/stairlight/main/img/stairlight_white.png" width="400" alt="Stairlight">
</div>

-----------------

# Stairlight

[![PyPi Version](https://img.shields.io/pypi/v/stairlight.svg?style=flat-square&logo=PyPi)](https://pypi.org/project/stairlight/)
[![PyPi License](https://img.shields.io/pypi/l/stairlight.svg?style=flat-square)](https://pypi.org/project/stairlight/)
[![PyPi Python Versions](https://img.shields.io/pypi/pyversions/stairlight.svg?style=flat-square)](https://pypi.org/project/stairlight/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg?style=flat-square)](https://github.com/psf/black)
[![CI](https://github.com/tosh2230/stairlight/actions/workflows/ci.yml/badge.svg)](https://github.com/tosh2230/stairlight/actions/workflows/ci.yml)

An end-to-end data lineage tool, detects table dependencies from SQL statements.

<div align="left">
  <img src="https://raw.githubusercontent.com/tosh2230/stairlight/main/img/drawio/concepts.drawio.png" width="1080" alt="concepts">
</div>

## Supported Data Sources

| Data Source | Remarks |
| --- | --- |
| Local file system | Python Pathlib module |
| [Amazon S3](https://aws.amazon.com/s3/) | Available for [Amazon Managed Workflows for Apache Airflow (MWAA)](https://aws.amazon.com/managed-workflows-for-apache-airflow/) |
| [Google Cloud Storage](https://cloud.google.com/storage) | Available for [Google Cloud Composer](https://cloud.google.com/composer) |
| [dbt](https://www.getdbt.com/) - [Google BigQuery](https://cloud.google.com/bigquery) | Using `dbt compile` command internally |
| [Redash](https://redash.io/) | |

## Installation

This package is distributed on [PyPI](https://pypi.org/project/stairlight/).

```sh
# The base package is for local file system only.
$ pip install stairlight

# Set extras when detecting from other data sources.
# e.g. Amazon S3 and Google Cloud Storage
$ pip install "stairlight[s3, gcs]"
```

| Data Source | TemplateSourceType | Extra |
| --- | --- | --- |
| Local file system | File | - |
| Amazon S3 | S3 | s3 |
| Google Cloud Storage | GCS | gcs |
| dbt - Google Bigquery | dbt | dbt-bigquery |
| Redash | Redash | redash |

## Getting Started

There are 3 steps to use.

```sh
# 1: Initialize and set your data source settings
$ stairlight init

# 2: Map your SQL statements and tables
$ stairlight map

# 3: Get table dependencies
$ stairlight
```

## Description

### Input

- SQL statements
- Configuration YAML files
    - stairlight.yaml: SQL statements locations and include/exclude conditions.
    - mapping.yaml: For mapping SQL statements and tables.

### Output

Stairlight outputs table dependencies as JSON format.

Top-level keys are table names, and values represents tables that are the data source for each key's table.

<details>

<summary>Example</summary>

```json
{
  "test_project.beam_streaming.taxirides_aggregation": {
    "test_project.beam_streaming.taxirides_realtime": {
      "TemplateSourceType": "File",
      "Key": "tests/sql/main/union_same_table.sql",
      "Uri": "/foo/bar/stairlight/tests/sql/main/union_same_table.sql",
      "Lines": [
        {
          "LineNumber": 6,
          "LineString": "    test_project.beam_streaming.taxirides_realtime"
        },
        {
          "LineNumber": 15,
          "LineString": "    test_project.beam_streaming.taxirides_realtime"
        }
      ]
    }
  },
  "PROJECT_a.DATASET_b.TABLE_c": {
    "PROJECT_A.DATASET_A.TABLE_A": {
      "TemplateSourceType": "GCS",
      "Key": "sql/one_line/one_line.sql",
      "Uri": "gs://stairlight/sql/one_line/one_line.sql",
      "Lines": [
        {
          "LineNumber": 1,
          "LineString": "SELECT * FROM PROJECT_A.DATASET_A.TABLE_A WHERE 1 = 1"
        }
      ],
      "BucketName": "stairlight",
      "Labels": {
        "Source": null,
        "Test": "a"
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
  },
  "PROJECT_as.DATASET_bs.TABLE_cs": {
    "PROJECT_A.DATASET_A.TABLE_A": {
      "TemplateSourceType": "S3",
      "Key": "sql/one_line/one_line.sql",
      "Uri": "s3://stairlight/sql/one_line/one_line.sql",
      "Lines": [
        {
          "LineNumber": 1,
          "LineString": "SELECT * FROM PROJECT_A.DATASET_A.TABLE_A WHERE 1 = 1"
        }
      ],
      "BucketName": "stairlight",
      "Labels": {
        "Source": null,
        "Test": "a"
      }
    }
  }
}
```

</details>

### Collecting patterns

#### Centralization

<div align="left">
  <img src="https://raw.githubusercontent.com/tosh2230/stairlight/main/img/drawio/centralization.drawio.png" width="800" alt="centralization">
</div>

#### Agents

<div align="left">
  <img src="https://raw.githubusercontent.com/tosh2230/stairlight/main/img/drawio/agents.drawio.png" width="800" alt="agents">
</div>

## Configuration

Examples can be found [here](https://github.com/tosh2230/stairlight/tree/main/tests/config), used for unit testing in CI.

### stairlight.yaml

'stairlight.yaml' is for setting up Stairlight itself. It is responsible for specifying SQL statements to be read.

`stairlight init` creates a template of stairlight.yaml.

<details>

<summary>Example</summary>

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
  - TemplateSourceType: S3
    BucketName: stairlight
    Regex: "^sql/.*/*.sql$"
    DefaultTablePrefix: "PROJECT_A"
Exclude:
  - TemplateSourceType: File
    Regex: "main/exclude.sql$"
Settings:
  MappingPrefix: "mapping"
```

</details>

### mapping.yaml

'mapping.yaml' is used to define relationships between input SELECT statements and tables.

`stairlight map` creates a template of mapping.yaml and attempts to read from data sources specified in stairlight.yaml.
If successfully read, it outputs settings that have not yet configured in an existing 'mapping.yaml' file.

<details>

<summary>Example</summary>

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
  - TemplateSourceType: S3
    Uri: "s3://stairlight/sql/one_line/one_line.sql"
    Tables:
      - TableName: "PROJECT_as.DATASET_bs.TABLE_cs"
ExtraLabels:
  - TableName: "PROJECT_A.DATASET_A.TABLE_A"
    Labels:
      Source: Null
      Test: a
```

</details>

#### Global Section

This section is for global configurations.

`Parameters` is used to set common parameters. If conflicts has occurred with `Parameters` in mapping section, mapping section's parameters will be used in preference to global.

#### Mapping Section

Mapping section is used to define relationships between input SELECT statements and tables that created as a result of query execution.

`Parameters` allows you to reflect settings in [jinja](https://jinja.palletsprojects.com/) template variables embedded in statements. If multiple settings are applied to a statement using jinja template, the statement will be read as if there were the same number of queries as the number of settings.

In contrast, `IgnoreParameters` handles a list to ignore when rendering queries.

#### Extra labels Section

This section sets labels to tables that appears only in queries.

## Arguments and Options

```txt
$ stairlight --help
usage: stairlight [-h] [-c CONFIG] [--save SAVE] [--load LOAD] {init,check,up,down} ...

An end-to-end data lineage tool, detects table dependencies by SQL SELECT statements.
Without positional arguments, return a table dependency map as JSON format.

positional arguments:
  {init,map,check,list,up,down}
    init                create a new Stairlight configuration file
    map (check)         create a new configuration file about undefined mappings
    list                return all ( tables | URIs )
    up                  return upstairs ( tables | URIs )
    down                return downstairs ( tables | URIs )

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        set a Stairlight configuration directory
  -q, --quiet           keep silence
  --save SAVE           A file path where map results will be saved.
                        You can choose from local file system, GCS, S3.
  --load LOAD           A file path where map results are saved.
                        You can choose from local file system, GCS, S3.
                        It can be specified multiple times.
```

### init

`stairlight init` creates a new Stairlight configuration file.

```txt
$ stairlight init --help
usage: stairlight init [-h] [-c CONFIG]

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        set a Stairlight configuration directory
  -q, --quiet           keep silence
```

### map(check)

`stairlight map` creates a new configuration file about undefined settings. `stairlight check` is an alias.
Options are the same as `stairlight init`.

### list

`stairlight list` outputs all of tables or SQL URIs.

- Output option(`-o`, `--output`) determines the output type, tables or URIs.

### up

`stairlight up` outputs tables or SQL URIs located upstream(upstairs) from the specified table.

- Use table(`-t`, `--table`) or label(`-l`, `--label`) option to specify tables to search.
- Output option(`-o`, `--output`) is same as `stairlight list`.
- Recursive option(`-r`, `--recursive`) is set, Stairlight will find dependencies recursively and output as a list.
- Verbose option(`-v`, `--verbose`) is set, Stairlight will add detailed information and output it as a dict.

```txt
$ stairlight up --help
usage: stairlight up [-h] [-c CONFIG] [--save SAVE] [--load LOAD] (-t TABLE | -l LABEL) [-o {table,uri}]
                     [-v] [-r]

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        set a Stairlight configuration directory
  -q, --quiet           keep silence
  --save SAVE           A file path where mapped results will be saved.
                        You can choose from local file system, GCS, S3.
  --load LOAD           A file path where mapped results are saved.
                        You can choose from local file system, GCS, S3.
                        It can be specified multiple times.
  -t TABLE, --table TABLE
                        table names that Stairlight searches for, can be specified
                        multiple times. e.g. -t PROJECT_a.DATASET_b.TABLE_c -t
                        PROJECT_d.DATASET_e.TABLE_f
  -l LABEL, --label LABEL
                        labels set for the table in mapping configuration, can be specified multiple times.
                        The separator between key and value should be a colon(:).
                        e.g. -l key_1:value_1 -l key_2:value_2
  -o {table,uri}, --output {table,uri}
                        output type
  -v, --verbose         return verbose results
  -r, --recursive       search recursively
```

### down

`stairlight down` outputs tables or SQL URIs located downstream(downstairs) from the specified table.
Options are the same as `stairlight up`.

## Use as a library

Stairlight can also be used as a library.

[tosh2230/stairlight-app](https://github.com/tosh2230/stairlight-app) is a sample web application rendering table dependency graph with Stairlight, using Graphviz, Streamlit and Google Cloud Run.

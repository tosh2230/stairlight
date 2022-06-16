from dataclasses import asdict

import pytest

from src.stairlight.configurator import MAPPING_CONFIG_PREFIX_DEFAULT, Configurator
from src.stairlight.source.config_key import MappingConfigKey
from src.stairlight.source.controller import (
    collect_mapping_attributes,
    get_default_table_name,
)
from src.stairlight.source.dbt.template import DbtTemplate
from src.stairlight.source.file.template import FileTemplate
from src.stairlight.source.gcs.template import GcsTemplate
from src.stairlight.source.redash.template import RedashTemplate


class TestSuccess:
    # file
    @pytest.fixture(scope="class")
    def file_template(self, configurator: Configurator) -> FileTemplate:
        return FileTemplate(
            mapping_config=configurator.read(prefix=MAPPING_CONFIG_PREFIX_DEFAULT),
            key="tests/sql/main/test_undefined.sql",
        )

    def test_get_default_table_name_file(self, file_template: FileTemplate):
        actual = get_default_table_name(template=file_template)
        expected = "test_undefined"
        assert actual == expected

    def test_collect_mapping_attributes_file(self, file_template: FileTemplate):
        actual = asdict(collect_mapping_attributes(template=file_template, tables=[]))
        expected = {
            MappingConfigKey.File.FILE_SUFFIX: "tests/sql/main/test_undefined.sql",
            MappingConfigKey.TABLES: [],
            MappingConfigKey.TEMPLATE_SOURCE_TYPE: "File",
        }
        assert actual == expected

    # gcs
    @pytest.fixture(scope="class")
    def gcs_template(self, configurator: Configurator) -> GcsTemplate:
        return GcsTemplate(
            mapping_config=configurator.read(prefix=MAPPING_CONFIG_PREFIX_DEFAULT),
            bucket="stairlight",
            key="tests/sql/gcs/one_line/one_line.sql",
        )

    def test_get_default_table_name_gcs(self, gcs_template: GcsTemplate):
        actual = get_default_table_name(template=gcs_template)
        expected = "one_line"
        assert actual == expected

    def test_collect_mapping_attributes_gcs(self, gcs_template: GcsTemplate):
        actual = asdict(collect_mapping_attributes(template=gcs_template, tables=[]))
        expected = {
            MappingConfigKey.Gcs.URI: (
                "gs://stairlight/tests/sql/gcs/one_line/one_line.sql"
            ),
            MappingConfigKey.TABLES: [],
            MappingConfigKey.TEMPLATE_SOURCE_TYPE: "GCS",
        }
        assert actual == expected

    # redash
    @pytest.fixture(scope="class")
    def redash_template(self, configurator: Configurator) -> RedashTemplate:
        return RedashTemplate(
            mapping_config=configurator.read(prefix=MAPPING_CONFIG_PREFIX_DEFAULT),
            query_id=5,
            query_name="Copy of (#4) New Query",
            query_str="SELECT * FROM {{ table }}",
            data_source_name="metadata",
        )

    def test_get_default_table_name_redash(self, redash_template: RedashTemplate):
        actual = get_default_table_name(template=redash_template)
        expected = "Copy of (#4) New Query"
        assert actual == expected

    def test_collect_mapping_attributes_redash(self, redash_template: RedashTemplate):
        actual = asdict(collect_mapping_attributes(template=redash_template, tables=[]))
        expected = {
            MappingConfigKey.Redash.QUERY_ID: 5,
            MappingConfigKey.Redash.DATA_SOURCE_NAME: "metadata",
            MappingConfigKey.TABLES: [],
            MappingConfigKey.TEMPLATE_SOURCE_TYPE: "Redash",
        }
        assert actual == expected

    # dbt
    @pytest.fixture(scope="class")
    def dbt_template(self, configurator: Configurator) -> DbtTemplate:
        return DbtTemplate(
            mapping_config=configurator.read(prefix=MAPPING_CONFIG_PREFIX_DEFAULT),
            key="tests/dbt/project_01/target/compiled/project_01/a/example_a.sql",
            project_name="project_01",
        )

    def test_get_default_table_name_dbt(self, dbt_template: DbtTemplate):
        actual = get_default_table_name(template=dbt_template)
        expected = "example_a"
        assert actual == expected

    def test_collect_mapping_attributes_dbt(self, dbt_template: DbtTemplate):
        actual = asdict(collect_mapping_attributes(template=dbt_template, tables=[]))
        expected = {
            MappingConfigKey.Dbt.FILE_SUFFIX: (
                "tests/dbt/project_01/target/compiled/project_01/a/example_a.sql"
            ),
            MappingConfigKey.Dbt.PROJECT_NAME: "project_01",
            MappingConfigKey.TABLES: [],
            MappingConfigKey.TEMPLATE_SOURCE_TYPE: "dbt",
        }
        assert actual == expected

from dataclasses import asdict

import pytest

from src.stairlight.configurator import MAPPING_CONFIG_PREFIX_DEFAULT, Configurator
from src.stairlight.source.config_key import MappingConfigKey
from src.stairlight.source.controller import (
    collect_mapping_attributes,
    get_default_table_name,
    get_template_source_class,
)
from src.stairlight.source.dbt.template import DbtTemplate, DbtTemplateSource
from src.stairlight.source.file.template import FileTemplate, FileTemplateSource
from src.stairlight.source.gcs.template import GcsTemplate, GcsTemplateSource
from src.stairlight.source.redash.template import RedashTemplate, RedashTemplateSource
from src.stairlight.source.s3.template import S3Template, S3TemplateSource
from src.stairlight.source.template import TemplateSourceType


class TestControlFile:
    @pytest.fixture(scope="class")
    def file_template(self, configurator: Configurator) -> FileTemplate:
        return FileTemplate(
            mapping_config=configurator.read_mapping(
                prefix=MAPPING_CONFIG_PREFIX_DEFAULT
            ),
            key="tests/sql/main/test_undefined.sql",
        )

    def test_get_template_source_class(self):
        actual = get_template_source_class(
            template_source_type=TemplateSourceType.FILE.value
        )
        assert actual == FileTemplateSource

    def test_get_default_table_name(self, file_template: FileTemplate):
        actual = get_default_table_name(template=file_template)
        expected = "test_undefined"
        assert actual == expected

    def test_collect_mapping_attributes(self, file_template: FileTemplate):
        actual = asdict(collect_mapping_attributes(template=file_template, tables=[]))
        expected = {
            MappingConfigKey.File.FILE_SUFFIX: "tests/sql/main/test_undefined.sql",
            MappingConfigKey.TABLES: [],
            MappingConfigKey.TEMPLATE_SOURCE_TYPE: "File",
        }
        assert actual == expected


class TestControlGcs:
    @pytest.fixture(scope="class")
    def gcs_template(self, configurator: Configurator) -> GcsTemplate:
        return GcsTemplate(
            mapping_config=configurator.read_mapping(
                prefix=MAPPING_CONFIG_PREFIX_DEFAULT
            ),
            bucket="stairlight",
            key="tests/sql/gcs/one_line/one_line.sql",
        )

    def test_get_template_source_class(self):
        actual = get_template_source_class(
            template_source_type=TemplateSourceType.GCS.value
        )
        assert actual == GcsTemplateSource

    def test_get_default_table_name(self, gcs_template: GcsTemplate):
        actual = get_default_table_name(template=gcs_template)
        expected = "one_line"
        assert actual == expected

    def test_collect_mapping_attributes(self, gcs_template: GcsTemplate):
        actual = asdict(collect_mapping_attributes(template=gcs_template, tables=[]))
        expected = {
            MappingConfigKey.Gcs.URI: (
                "gs://stairlight/tests/sql/gcs/one_line/one_line.sql"
            ),
            MappingConfigKey.TABLES: [],
            MappingConfigKey.TEMPLATE_SOURCE_TYPE: "GCS",
        }
        assert actual == expected


class TestControlRedash:
    @pytest.fixture(scope="class")
    def redash_template(self, configurator: Configurator) -> RedashTemplate:
        return RedashTemplate(
            mapping_config=configurator.read_mapping(
                prefix=MAPPING_CONFIG_PREFIX_DEFAULT
            ),
            query_id=5,
            query_name="Copy of (#4) New Query",
            query_str="SELECT * FROM {{ table }}",
            data_source_name="metadata",
        )

    def test_get_template_source_class(self):
        actual = get_template_source_class(
            template_source_type=TemplateSourceType.REDASH.value
        )
        assert actual == RedashTemplateSource

    def test_get_default_table_name(self, redash_template: RedashTemplate):
        actual = get_default_table_name(template=redash_template)
        expected = "Copy of (#4) New Query"
        assert actual == expected

    def test_collect_mapping_attributes(self, redash_template: RedashTemplate):
        actual = asdict(collect_mapping_attributes(template=redash_template, tables=[]))
        expected = {
            MappingConfigKey.Redash.QUERY_ID: 5,
            MappingConfigKey.Redash.DATA_SOURCE_NAME: "metadata",
            MappingConfigKey.TABLES: [],
            MappingConfigKey.TEMPLATE_SOURCE_TYPE: "Redash",
        }
        assert actual == expected


class TestControlDbt:
    @pytest.fixture(scope="class")
    def dbt_template(self, configurator: Configurator) -> DbtTemplate:
        return DbtTemplate(
            mapping_config=configurator.read_mapping(
                prefix=MAPPING_CONFIG_PREFIX_DEFAULT
            ),
            key="tests/dbt/project_01/target/compiled/project_01/a/example_a.sql",
            project_name="project_01",
        )

    def test_get_template_source_class(self):
        actual = get_template_source_class(
            template_source_type=TemplateSourceType.DBT.value
        )
        assert actual == DbtTemplateSource

    def test_get_default_table_name(self, dbt_template: DbtTemplate):
        actual = get_default_table_name(template=dbt_template)
        expected = "example_a"
        assert actual == expected

    def test_collect_mapping_attributes(self, dbt_template: DbtTemplate):
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


class TestControlS3:
    @pytest.fixture(scope="class")
    def s3_template(self, configurator: Configurator) -> S3Template:
        return S3Template(
            mapping_config=configurator.read_mapping(
                prefix=MAPPING_CONFIG_PREFIX_DEFAULT
            ),
            bucket="stairlight",
            key="tests/sql/gcs/one_line/one_line.sql",
        )

    def test_get_template_source_class(self):
        actual = get_template_source_class(
            template_source_type=TemplateSourceType.S3.value
        )
        assert actual == S3TemplateSource

    def test_get_default_table_name(self, s3_template: S3Template):
        actual = get_default_table_name(template=s3_template)
        expected = "one_line"
        assert actual == expected

    def test_collect_mapping_attributes(self, s3_template: S3Template):
        actual = asdict(collect_mapping_attributes(template=s3_template, tables=[]))
        expected = {
            MappingConfigKey.S3.URI: (
                "s3://stairlight/tests/sql/gcs/one_line/one_line.sql"
            ),
            MappingConfigKey.TABLES: [],
            MappingConfigKey.TEMPLATE_SOURCE_TYPE: "S3",
        }
        assert actual == expected

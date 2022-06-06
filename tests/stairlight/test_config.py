import os
from collections import OrderedDict

import pytest

from src.stairlight import config_key
from src.stairlight.config import (
    ConfigKeyNotFoundException,
    Configurator,
    get_config_value,
)
from src.stairlight.key import MapKey
from src.stairlight.source.dbt import DbtTemplate
from src.stairlight.source.file import FileTemplate
from src.stairlight.source.gcs import GcsTemplate
from src.stairlight.source.redash import RedashTemplate


class TestSuccess:
    def test_read_map(self, configurator: Configurator):
        assert configurator.read(prefix=config_key.MAPPING_CONFIG_FILE_PREFIX)

    def test_read_sql(self, configurator: Configurator):
        assert configurator.read(prefix=config_key.STAIRLIGHT_CONFIG_FILE_PREFIX)

    def test_create_stairlight_file(
        self, configurator: Configurator, stairlight_template_prefix: str
    ):
        file_name = configurator.create_stairlight_file(
            prefix=stairlight_template_prefix
        )
        assert os.path.exists(file_name)

    def test_create_mapping_file(
        self, configurator: Configurator, mapping_template_prefix: str
    ):
        file_name = configurator.create_mapping_file(
            unmapped=[], prefix=mapping_template_prefix
        )
        assert os.path.exists(file_name)

    def test_build_stairlight_config(self, configurator: Configurator):
        stairlight_template = configurator.build_stairlight_config()
        assert list(stairlight_template.keys()) == [
            config_key.STAIRLIGHT_CONFIG_INCLUDE_SECTION,
            config_key.STAIRLIGHT_CONFIG_EXCLUDE_SECTION,
            config_key.STAIRLIGHT_CONFIG_SETTING_SECTION,
        ]

    def test_get_config_value(self):
        actual = get_config_value(key="a", target={"a": "c"})
        expected = "c"
        assert actual == expected

    # file
    @pytest.fixture(scope="class")
    def file_template(self, configurator) -> FileTemplate:
        return FileTemplate(
            mapping_config=configurator.read(
                prefix=config_key.MAPPING_CONFIG_FILE_PREFIX
            ),
            key="tests/sql/main/test_undefined.sql",
        )

    def test_select_mapping_values_by_template_file(
        self, configurator: Configurator, file_template: FileTemplate
    ):
        actual = configurator.select_mapping_values_by_template(template=file_template)
        expected = {config_key.FILE_SUFFIX: "tests/sql/main/test_undefined.sql"}
        assert actual == expected

    def test_get_default_table_name_file(
        self, configurator: Configurator, file_template: FileTemplate
    ):
        actual = configurator.get_default_table_name(template=file_template)
        expected = "test_undefined"
        assert actual == expected

    def test_build_mapping_config(
        self, configurator: Configurator, file_template: FileTemplate
    ):
        unmapped_templates = [
            {
                MapKey.TEMPLATE: file_template,
                MapKey.PARAMETERS: [
                    "params.main_table",
                    "params.sub_table_01",
                    "params.sub_table_02",
                ],
            }
        ]

        global_value = OrderedDict({config_key.PARAMETERS: {}})
        mapping_value = OrderedDict(
            {
                config_key.TEMPLATE_SOURCE_TYPE: file_template.source_type.value,
                config_key.FILE_SUFFIX: file_template.key,
                config_key.TABLES: [
                    OrderedDict(
                        {
                            config_key.TABLE_NAME: "test_undefined",
                            config_key.PARAMETERS: OrderedDict(
                                {
                                    "params": {
                                        "main_table": None,
                                        "sub_table_01": None,
                                        "sub_table_02": None,
                                    }
                                }
                            ),
                            config_key.LABELS: OrderedDict({"key": "value"}),
                        }
                    )
                ],
            }
        )

        metadata_value = OrderedDict(
            {
                config_key.TABLE_NAME: None,
                config_key.LABELS: OrderedDict({"key": "value"}),
            }
        )

        expected = OrderedDict(
            {
                config_key.MAPPING_CONFIG_GLOBAL_SECTION: global_value,
                config_key.MAPPING_CONFIG_MAPPING_SECTION: [mapping_value],
                config_key.MAPPING_CONFIG_METADATA_SECTION: [metadata_value],
            }
        )
        actual = configurator.build_mapping_config(
            unmapped_templates=unmapped_templates
        )
        assert actual == expected

    # gcs
    @pytest.fixture(scope="class")
    def gcs_template(self, configurator: Configurator) -> GcsTemplate:
        return GcsTemplate(
            mapping_config=configurator.read(
                prefix=config_key.MAPPING_CONFIG_FILE_PREFIX
            ),
            bucket="stairlight",
            key="tests/sql/gcs/one_line/one_line.sql",
        )

    def test_select_mapping_values_by_template_gcs(
        self, configurator: Configurator, gcs_template: GcsTemplate
    ):
        actual = configurator.select_mapping_values_by_template(template=gcs_template)
        expected = {
            config_key.URI: "gs://stairlight/tests/sql/gcs/one_line/one_line.sql",
            config_key.BUCKET_NAME: "stairlight",
        }
        assert actual == expected

    def test_get_default_table_name_gcs(
        self, configurator: Configurator, gcs_template: GcsTemplate
    ):
        actual = configurator.get_default_table_name(template=gcs_template)
        expected = "one_line"
        assert actual == expected

    # redash
    @pytest.fixture(scope="class")
    def redash_template(self, configurator: Configurator) -> RedashTemplate:
        return RedashTemplate(
            mapping_config=configurator.read(
                prefix=config_key.MAPPING_CONFIG_FILE_PREFIX
            ),
            query_id=5,
            query_name="Copy of (#4) New Query",
            query_str="SELECT * FROM {{ table }}",
            data_source_name="metadata",
        )

    def test_select_mapping_values_by_template_redash(
        self, configurator: Configurator, redash_template: RedashTemplate
    ):
        actual = configurator.select_mapping_values_by_template(
            template=redash_template
        )
        expected = {
            config_key.QUERY_ID: 5,
            config_key.DATA_SOURCE_NAME: "metadata",
        }
        assert actual == expected

    def test_get_default_table_name_redash(
        self, configurator: Configurator, redash_template: RedashTemplate
    ):
        actual = configurator.get_default_table_name(template=redash_template)
        expected = "Copy of (#4) New Query"
        assert actual == expected

    # dbt
    @pytest.fixture(scope="class")
    def dbt_template(self, configurator: Configurator) -> DbtTemplate:
        return DbtTemplate(
            mapping_config=configurator.read(
                prefix=config_key.MAPPING_CONFIG_FILE_PREFIX
            ),
            key="tests/dbt/project_01/target/compiled/project_01/a/example_a.sql",
            project_name="project_01",
        )

    def test_select_mapping_values_by_template_dbt(
        self, configurator: Configurator, dbt_template: DbtTemplate
    ):
        actual = configurator.select_mapping_values_by_template(template=dbt_template)
        expected = {
            config_key.FILE_SUFFIX: (
                "tests/dbt/project_01/target/compiled/project_01/a/example_a.sql"
            ),
            config_key.PROJECT_NAME: "project_01",
        }
        assert actual == expected

    def test_get_default_table_name_dbt(
        self, configurator: Configurator, dbt_template: DbtTemplate
    ):
        actual = configurator.get_default_table_name(template=dbt_template)
        expected = "example_a"
        assert actual == expected


class TestFailure:
    def test_get_config_value(self):
        with pytest.raises(ConfigKeyNotFoundException):
            _ = get_config_value(key="a", target={"b": "c"}, fail_if_not_found=True)

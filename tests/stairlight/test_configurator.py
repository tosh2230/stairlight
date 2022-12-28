from __future__ import annotations

import os
from collections import OrderedDict
from typing import Any

import pytest

from src.stairlight.configurator import (
    MAPPING_CONFIG_PREFIX_DEFAULT,
    STAIRLIGHT_CONFIG_PREFIX_DEFAULT,
    Configurator,
    create_nested_dict,
)
from src.stairlight.source.config_key import (
    MapKey,
    MappingConfigKey,
    StairlightConfigKey,
)
from src.stairlight.source.controller import GCS_URI_SCHEME, S3_URI_SCHEME
from src.stairlight.source.dbt.template import DbtTemplate
from src.stairlight.source.file.template import FileTemplate
from src.stairlight.source.gcs.template import GcsTemplate
from src.stairlight.source.redash.template import RedashTemplate
from src.stairlight.source.s3.template import S3Template


class TestStairlightConfig:
    def test_read_stairlight(self, configurator: Configurator):
        assert configurator.read_stairlight(prefix=STAIRLIGHT_CONFIG_PREFIX_DEFAULT)

    def test_create_stairlight_file(
        self, configurator: Configurator, stairlight_template_prefix: str
    ):
        file_name = configurator.create_stairlight_file(
            prefix=stairlight_template_prefix
        )
        assert os.path.exists(file_name)

    def test_build_stairlight_config(self, configurator: Configurator):
        stairlight_template = configurator.build_stairlight_config()
        assert list(stairlight_template.keys()) == [
            StairlightConfigKey.INCLUDE_SECTION,
            StairlightConfigKey.EXCLUDE_SECTION,
            StairlightConfigKey.SETTING_SECTION,
        ]


class TestMappingConfig:
    def test_read_mapping(self, configurator: Configurator):
        assert configurator.read_mapping(prefix=MAPPING_CONFIG_PREFIX_DEFAULT)

    def test_create_mapping_file(
        self, configurator: Configurator, mapping_template_prefix: str
    ):
        file_name = configurator.create_mapping_file(
            unmapped=[], prefix=mapping_template_prefix
        )
        assert os.path.exists(file_name)


class TestBuildMappingConfigFile:
    @pytest.fixture(scope="class")
    def file_template(self, configurator: Configurator) -> FileTemplate:
        return FileTemplate(
            mapping_config=configurator.read_mapping(
                prefix=MAPPING_CONFIG_PREFIX_DEFAULT
            ),
            key="tests/sql/main/test_undefined.sql",
        )

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

        mapping_value = OrderedDict(
            {
                MappingConfigKey.TEMPLATE_SOURCE_TYPE: file_template.source_type.value,
                MappingConfigKey.TABLES: [
                    OrderedDict(
                        {
                            MappingConfigKey.TABLE_NAME: "test_undefined",
                            MappingConfigKey.PARAMETERS: OrderedDict(
                                {
                                    "params": {
                                        "main_table": None,
                                        "sub_table_01": None,
                                        "sub_table_02": None,
                                    }
                                }
                            ),
                        }
                    )
                ],
                MappingConfigKey.File.FILE_SUFFIX: file_template.key,
            }
        )

        expected = OrderedDict(
            {
                MappingConfigKey.MAPPING_SECTION: [mapping_value],
            }
        )
        actual = configurator.build_mapping_config(
            unmapped_templates=unmapped_templates
        )
        assert actual == expected


class TestBuildMappingConfigGcs:
    @pytest.fixture(scope="class")
    def gcs_template(self, configurator: Configurator) -> GcsTemplate:
        return GcsTemplate(
            mapping_config=configurator.read_mapping(
                prefix=MAPPING_CONFIG_PREFIX_DEFAULT
            ),
            bucket="stairlight",
            key="sql/cte/cte_multi_line.sql",
        )

    def test_build_mapping_config(
        self, configurator: Configurator, gcs_template: GcsTemplate
    ):
        unmapped_templates = [
            {
                MapKey.TEMPLATE: gcs_template,
                MapKey.PARAMETERS: [
                    "params.PROJECT",
                    "params.DATASET",
                    "params.TABLE",
                ],
            }
        ]

        mapping_value = OrderedDict(
            {
                MappingConfigKey.TEMPLATE_SOURCE_TYPE: gcs_template.source_type.value,
                MappingConfigKey.TABLES: [
                    OrderedDict(
                        {
                            MappingConfigKey.TABLE_NAME: "cte_multi_line",
                            MappingConfigKey.PARAMETERS: OrderedDict(
                                {
                                    "params": {
                                        "PROJECT": None,
                                        "DATASET": None,
                                        "TABLE": None,
                                    }
                                }
                            ),
                        }
                    )
                ],
                MappingConfigKey.Gcs.URI: (
                    f"{GCS_URI_SCHEME}{gcs_template.bucket}/{gcs_template.key}"
                ),
            }
        )

        expected = OrderedDict(
            {
                MappingConfigKey.MAPPING_SECTION: [mapping_value],
            }
        )
        actual = configurator.build_mapping_config(
            unmapped_templates=unmapped_templates
        )
        assert actual == expected


class TestBuildMappingConfigRedash:
    @pytest.fixture(scope="class")
    def redash_template(self, configurator: Configurator) -> RedashTemplate:
        return RedashTemplate(
            mapping_config=configurator.read_mapping(prefix="mapping_redash"),
            query_id=5,
            query_name="redash_test_query",
            data_source_name="redash_test_data",
        )

    def test_build_mapping_config(
        self, configurator: Configurator, redash_template: RedashTemplate
    ):
        unmapped_templates = [
            {
                MapKey.TEMPLATE: redash_template,
                MapKey.PARAMETERS: [
                    "params.PROJECT",
                    "params.DATASET",
                    "params.TABLE",
                ],
            }
        ]

        mapping_value = OrderedDict(
            {
                MappingConfigKey.TEMPLATE_SOURCE_TYPE: (
                    redash_template.source_type.value
                ),
                MappingConfigKey.TABLES: [
                    OrderedDict(
                        {
                            MappingConfigKey.TABLE_NAME: "redash_test_query",
                            MappingConfigKey.PARAMETERS: OrderedDict(
                                {
                                    "params": {
                                        "PROJECT": None,
                                        "DATASET": None,
                                        "TABLE": None,
                                    }
                                }
                            ),
                        }
                    )
                ],
                MappingConfigKey.Redash.QUERY_ID: 5,
                MappingConfigKey.Redash.DATA_SOURCE_NAME: "redash_test_data",
            }
        )

        expected = OrderedDict(
            {
                MappingConfigKey.MAPPING_SECTION: [mapping_value],
            }
        )
        actual = configurator.build_mapping_config(
            unmapped_templates=unmapped_templates
        )
        assert actual == expected


class TestBuildMappingConfigDbt:
    @pytest.fixture(scope="class")
    def dbt_template(self, configurator: Configurator) -> DbtTemplate:
        return DbtTemplate(
            mapping_config=configurator.read_mapping(
                prefix=MAPPING_CONFIG_PREFIX_DEFAULT
            ),
            key=(
                "tests/dbt/project_01/target/compiled/project_01/"
                "models/example/my_first_dbt_model.sql"
            ),
            project_name="project_01",
        )

    def test_build_mapping_config(
        self, configurator: Configurator, dbt_template: DbtTemplate
    ):
        unmapped_templates = [
            {
                MapKey.TEMPLATE: dbt_template,
                MapKey.PARAMETERS: [
                    "params.PROJECT",
                    "params.DATASET",
                    "params.TABLE",
                ],
            }
        ]

        mapping_value = OrderedDict(
            {
                MappingConfigKey.TEMPLATE_SOURCE_TYPE: dbt_template.source_type.value,
                MappingConfigKey.TABLES: [
                    OrderedDict(
                        {
                            MappingConfigKey.TABLE_NAME: "my_first_dbt_model",
                            MappingConfigKey.PARAMETERS: OrderedDict(
                                {
                                    "params": {
                                        "PROJECT": None,
                                        "DATASET": None,
                                        "TABLE": None,
                                    }
                                }
                            ),
                        }
                    )
                ],
                MappingConfigKey.Dbt.PROJECT_NAME: "project_01",
                MappingConfigKey.Dbt.FILE_SUFFIX: (
                    "tests/dbt/project_01/target/compiled/project_01/models/"
                    "example/my_first_dbt_model.sql"
                ),
            }
        )

        expected = OrderedDict(
            {
                MappingConfigKey.MAPPING_SECTION: [mapping_value],
            }
        )
        actual = configurator.build_mapping_config(
            unmapped_templates=unmapped_templates
        )
        assert actual == expected


class TestBuildMappingConfigS3:
    @pytest.fixture(scope="class")
    def s3_template(self, configurator: Configurator) -> S3Template:
        return S3Template(
            mapping_config=configurator.read_mapping(
                prefix=MAPPING_CONFIG_PREFIX_DEFAULT
            ),
            bucket="stairlight",
            key="sql/cte/cte_multi_line.sql",
        )

    def test_build_mapping_config(
        self, configurator: Configurator, s3_template: S3Template
    ):
        unmapped_templates = [
            {
                MapKey.TEMPLATE: s3_template,
                MapKey.PARAMETERS: [
                    "params.PROJECT",
                    "params.DATASET",
                    "params.TABLE",
                ],
            }
        ]

        mapping_value = OrderedDict(
            {
                MappingConfigKey.TEMPLATE_SOURCE_TYPE: s3_template.source_type.value,
                MappingConfigKey.TABLES: [
                    OrderedDict(
                        {
                            MappingConfigKey.TABLE_NAME: "cte_multi_line",
                            MappingConfigKey.PARAMETERS: OrderedDict(
                                {
                                    "params": {
                                        "PROJECT": None,
                                        "DATASET": None,
                                        "TABLE": None,
                                    }
                                }
                            ),
                        }
                    )
                ],
                MappingConfigKey.Gcs.URI: (
                    f"{S3_URI_SCHEME}{s3_template.bucket}/{s3_template.key}"
                ),
            }
        )

        expected = OrderedDict(
            {
                MappingConfigKey.MAPPING_SECTION: [mapping_value],
            }
        )
        actual = configurator.build_mapping_config(
            unmapped_templates=unmapped_templates
        )
        assert actual == expected


@pytest.mark.parametrize(
    ("params", "expected"),
    [
        (
            [
                "params.PROJECT",
                "params.DATASET",
                "params.TABLE",
            ],
            {
                "params": {
                    "PROJECT": None,
                    "DATASET": None,
                    "TABLE": None,
                }
            },
        ),
        (
            [
                "params.A.PROJECT_A",
                "params.A.DATASET_A",
                "params.A.TABLE_A",
                "params.B.PROJECT_B",
                "params.B.DATASET_B",
                "params.B.TABLE_B",
            ],
            {
                "params": {
                    "A": {
                        "PROJECT_A": None,
                        "DATASET_A": None,
                        "TABLE_A": None,
                    },
                    "B": {
                        "PROJECT_B": None,
                        "DATASET_B": None,
                        "TABLE_B": None,
                    },
                }
            },
        ),
    ],
    ids=["single_dot", "double_dots"],
)
def test_create_nested_dict(params, expected):
    actual: dict[str, Any] = {}
    for param in params:
        splitted_params = param.split(".")
        create_nested_dict(keys=splitted_params, results=actual)
    assert actual == expected

from __future__ import annotations

from collections import OrderedDict

import pytest

from src.stairlight.map import Map, create_dict_key_list
from src.stairlight.source.config import (
    MappingConfig,
    MappingConfigMappingTable,
    StairlightConfig,
)
from src.stairlight.source.config_key import MapKey
from src.stairlight.source.template import Template, TemplateSourceType


@pytest.fixture(scope="session")
def dependency_map(
    stairlight_config: StairlightConfig, mapping_config: MappingConfig
) -> Map:
    dependency_map = Map(
        stairlight_config=stairlight_config, mapping_config=mapping_config
    )
    dependency_map.write()
    return dependency_map


@pytest.fixture(scope="session")
def dependency_map_single(
    stairlight_config: StairlightConfig, mapping_config_single: MappingConfig
) -> Map:
    dependency_map = Map(
        stairlight_config=stairlight_config, mapping_config=mapping_config_single
    )
    dependency_map.write()
    return dependency_map


@pytest.mark.integration
class TestSuccess:
    def test_mapped(self, dependency_map: Map):
        print(dependency_map.mapped)
        assert len(dependency_map.mapped) > 0

    @pytest.mark.parametrize(
        "template_source_type",
        [
            TemplateSourceType.FILE.value,
            TemplateSourceType.GCS.value,
            # TemplateSourceType.REDASH.value,
            TemplateSourceType.DBT.value,
            TemplateSourceType.S3.value,
        ],
    )
    def test_mapped_items(self, dependency_map: Map, template_source_type: str):
        found: bool = False
        upstairs_items: dict
        upstairs_attributes: dict
        for _, upstairs_items in dependency_map.mapped.items():
            for _, upstairs_attributes in upstairs_items.items():
                if (
                    upstairs_attributes.get(MapKey.TEMPLATE_SOURCE_TYPE)
                    == template_source_type
                ):
                    found = True
                    break

        assert found

    def test_unmapped_size(self, dependency_map: Map):
        print(dependency_map.unmapped)
        assert len(dependency_map.unmapped) > 0

    @pytest.mark.parametrize(
        ("key", "expected"),
        [
            ("tests/sql/main/cte_multi_line.sql", []),
            ("tests/sql/main/one_line_1.sql", []),
            (
                "tests/sql/main/undefined.sql",
                [
                    "params.main_table",
                    "params.sub_table_01",
                    "params.sub_table_02",
                ],
            ),
            (
                "tests/sql/main/undefined_part.sql",
                [
                    "params.sub_table_01",
                    "params.sub_table_02",
                ],
            ),
        ],
        ids=[
            "tests/sql/main/cte_multi_line.sql",
            "tests/sql/main/one_line_1.sql",
            "tests/sql/main/undefined.sql",
            "tests/sql/main/undefined_part.sql",
        ],
    )
    def test_find_unmapped_params(
        self, dependency_map: Map, key: str, expected: list[str]
    ):
        actual = []
        for unmapped_attributes in dependency_map.unmapped:
            template: Template = unmapped_attributes.get(MapKey.TEMPLATE, None)
            if template.key == key:
                actual = sorted(unmapped_attributes.get(MapKey.PARAMETERS, []))
                break
        assert actual == expected

    def test_get_global_params(self, dependency_map: Map):
        actual = dependency_map.get_global_params()
        expected = {
            "DESTINATION_PROJECT": "PROJECT_GLOBAL",
            "params": {
                "PROJECT": "PROJECT_GLOBAL",
                "DATASET": "DATASET_GLOBAL",
                "TABLE": "TABLE_GLOBAL",
            },
        }
        assert actual == expected

    def test_get_global_params_if_params_none(self, dependency_map_single: Map):
        actual = dependency_map_single.get_global_params()
        assert actual == {}

    def test_merge_global_params_if_global_none(self, dependency_map_single: Map):
        expected = OrderedDict({"params": None})
        table_attributes = MappingConfigMappingTable(
            TableName="PROJECT_A.DATASET_B.TABLE_C",
            Parameters=expected,
        )
        actual = dependency_map_single.merge_global_params(
            table_attributes=table_attributes
        )
        assert actual == expected

    def test_merge_global_params_if_only_global(self, dependency_map: Map):
        table_attributes = MappingConfigMappingTable(TableName="")
        actual = dependency_map.merge_global_params(table_attributes=table_attributes)
        expected = dependency_map.get_global_params()
        assert actual == expected

    def test_merge_global_params_by_table(self, dependency_map: Map):
        table_attributes = MappingConfigMappingTable(
            TableName="PROJECT_g.DATASET_g.TABLE_g",
            Parameters=OrderedDict(
                {
                    "params": {
                        "PROJECT": "PROJECT_BY_TABLE",
                        "DATASET": "DATASET_BY_TABLE",
                        "TABLE": "TABLE_BY_TABLE",
                    },
                }
            ),
        )
        actual = dependency_map.merge_global_params(table_attributes=table_attributes)
        expected = {
            "DESTINATION_PROJECT": "PROJECT_GLOBAL",
            "params": {
                "PROJECT": "PROJECT_BY_TABLE",
                "DATASET": "DATASET_BY_TABLE",
                "TABLE": "TABLE_BY_TABLE",
            },
        }
        assert actual == expected


@pytest.mark.integration
class TestSuccessNoExtraLables:
    def test_find_unmapped_params(self, dependency_map: Map):
        assert dependency_map.unmapped


def test_create_dict_key_list():
    d = {
        "params": {
            "PROJECT": "PROJECT_BY_TABLE",
            "DATASET": "DATASET_BY_TABLE",
            "TABLE": "TABLE_BY_TABLE",
        }
    }
    actual = create_dict_key_list(d=d)
    expected = ["params.PROJECT", "params.DATASET", "params.TABLE"]
    assert actual == expected

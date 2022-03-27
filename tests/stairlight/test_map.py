import pytest

from src.stairlight import config_key, map_key


class TestSuccess:
    def test_mapped(self, dependency_map):
        assert len(dependency_map.mapped) > 0

    def test_unmapped(self, dependency_map):
        assert len(dependency_map.unmapped) > 0

    @pytest.mark.parametrize(
        "key, expected",
        [
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
    )
    def test_find_unmapped_params(self, dependency_map, key, expected):
        actual = None
        for unmapped_attributes in dependency_map.unmapped:
            template = unmapped_attributes.get(map_key.TEMPLATE)
            if template.key == key:
                actual = sorted(unmapped_attributes.get(map_key.PARAMETERS))
                break
        assert actual == expected

    def test_get_global_params(self, dependency_map):
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

    def test_get_combined_params_global_only(self, dependency_map):
        table_attributes = {
            config_key.TABLE_NAME: "PROJECT_g.DATASET_g.TABLE_g",
        }
        actual = dependency_map.get_combined_params(table_attributes)
        expected = dependency_map.get_global_params()
        assert actual == expected

    def test_get_combined_params_by_table(self, dependency_map):
        table_attributes = {
            config_key.TABLE_NAME: "PROJECT_g.DATASET_g.TABLE_g",
            config_key.PARAMETERS: {
                "params": {
                    "PROJECT": "PROJECT_BY_TABLE",
                    "DATASET": "DATASET_BY_TABLE",
                    "TABLE": "TABLE_BY_TABLE",
                },
            },
        }
        actual = dependency_map.get_combined_params(table_attributes)
        expected = {
            "DESTINATION_PROJECT": "PROJECT_GLOBAL",
            "params": {
                "PROJECT": "PROJECT_BY_TABLE",
                "DATASET": "DATASET_BY_TABLE",
                "TABLE": "TABLE_BY_TABLE",
            },
        }
        assert actual == expected


class TestSuccessNoMetadata:
    def test_find_unmapped_params(self, dependency_map):
        assert dependency_map.unmapped

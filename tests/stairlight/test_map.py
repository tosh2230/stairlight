import pytest

from src.stairlight import map_key


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
        for a in dependency_map.unmapped:
            sql_template = a.get(map_key.TEMPLATE)
            if sql_template.key == key:
                actual = sorted(a.get(map_key.PARAMETERS))
                break
        assert actual == expected


class TestSuccessNoMetadata:
    def test_find_unmapped_params(self, dependency_map):
        assert dependency_map.unmapped

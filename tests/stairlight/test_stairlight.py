from __future__ import annotations

import json
from typing import Any, Iterator

import pytest

from src.stairlight import ResponseType, SearchDirection, StairLight
from src.stairlight.source.config import MapKey
from tests.conftest import teardown_rm_file


@pytest.fixture(scope="session")
def stairlight_merge() -> Iterator[StairLight]:
    load_files = [
        "./tests/expected/file_01.json",
        "./tests/expected/file_02.json",
        "./tests/expected/gcs.json",
    ]
    save_file = "./tests/expected/actual.json"
    stairlight = StairLight(
        config_dir="tests/config", load_files=load_files, save_file=save_file
    )
    stairlight.create_map()
    yield stairlight
    teardown_rm_file(save_file)


class TestResponseType:
    def test_table(self):
        assert ResponseType.TABLE.value == "table"

    def test_file(self):
        assert ResponseType.URI.value == "uri"


class TestSearchDirection:
    def test_up(self):
        assert SearchDirection.UP.value == "Upstairs"

    def test_down(self):
        assert SearchDirection.DOWN.value == "Downstairs"


@pytest.mark.integration
class TestStairLight:
    stairlight = StairLight(config_dir="tests/config")
    stairlight.create_map()

    def test_has_stairlight_config(self):
        assert self.stairlight.has_stairlight_config()

    def test_mapped(self):
        actual = set()
        upstairs = self.stairlight.mapped["PROJECT_J.DATASET_K.TABLE_L"]
        for mapped_templates in upstairs.values():
            actual.update(
                set([mapped_template.Key for mapped_template in mapped_templates])
            )
        assert (
            set(
                [
                    "tests/sql/cte_multi_line_params.sql",
                    "tests/sql/cte_multi_line_params_copy.sql",
                ]
            )
            == actual
        )

    def test_mapped_Lines(self):
        upstairs = self.stairlight.mapped["PROJECT_A.DATASET_B.TABLE_C"]
        upstair_attribute = upstairs["PROJECT_A.DATASET_A.TABLE_A"][0]
        assert upstair_attribute.Lines

    def test_mapped_exclude_empty_value(self):
        assert "dummy.dummy.my_first_dbt_model" not in self.stairlight.mapped.keys()

    def test_unmapped(self):
        file_keys = [
            unmapped_file.get(MapKey.TEMPLATE)
            for unmapped_file in self.stairlight.unmapped
        ]
        assert file_keys

    def test_init(self, stairlight_template_prefix: str):
        assert (
            self.stairlight.init(prefix=stairlight_template_prefix)
            == f"tests/config/{stairlight_template_prefix}.yaml"
        )

    def test_check_unmapped(self, prefix_unmapped, prefix_not_found):
        file_names = self.stairlight.check(
            prefix_unmapped=prefix_unmapped, prefix_not_found=prefix_not_found
        )
        assert any(
            [
                file_name.startswith(f"tests/config/.{prefix_unmapped}")
                for file_name in file_names
            ]
        ) and any(
            [
                file_name.startswith(f"tests/config/.{prefix_not_found}")
                for file_name in file_names
            ]
        )

    @pytest.mark.parametrize(
        ("response_type", "expected"),
        [
            (ResponseType.TABLE.value, "PROJECT_A.DATASET_A.TABLE_A"),
            (ResponseType.URI.value, "gs://stairlight/sql/cte/cte_multi_line.sql"),
        ],
        ids=[
            ResponseType.TABLE.value,
            ResponseType.URI.value,
        ],
    )
    def test_list(
        self,
        response_type,
        expected,
    ):
        assert expected in self.stairlight.list_(response_type=response_type)

    def test_list_tables(self):
        actual = self.stairlight.list_tables()
        assert "PROJECT_A.DATASET_A.TABLE_A" in actual
        assert "PROJECT_A.DATASET_B.TABLE_C" in actual

    def test_list_uris(self):
        assert any(
            [
                uri
                for uri in self.stairlight.list_uris()
                if uri.endswith(
                    (
                        "tests/dbt/project_01/target/compiled/project_01/"
                        "models/example/my_first_dbt_model.sql"
                    )
                )
            ]
        )

    def test_up_next(self):
        table_name = "PROJECT_D.DATASET_E.TABLE_F"
        result = self.stairlight.up(
            table_name=table_name, recursive=False, verbose=False
        )
        assert sorted(result) == [
            "PROJECT_C.DATASET_C.TABLE_C",
            "PROJECT_J.DATASET_K.TABLE_L",
            "PROJECT_d.DATASET_d.TABLE_d",
        ]

    def test_up_recursive_verbose(self):
        actual: list[str] = []
        table_name: str = "PROJECT_D.DATASET_E.TABLE_F"
        result = self.stairlight.up(table_name=table_name, recursive=True, verbose=True)
        if isinstance(result, dict):
            actual = sorted(
                result[table_name][SearchDirection.UP.value][
                    "PROJECT_J.DATASET_K.TABLE_L"
                ][SearchDirection.UP.value].keys()
            )
        assert actual == [
            "PROJECT_P.DATASET_Q.TABLE_R",
            "PROJECT_S.DATASET_T.TABLE_U",
            "PROJECT_V.DATASET_W.TABLE_X",
        ]

    def test_up_recursive_plain_table(self):
        table_name = "PROJECT_D.DATASET_E.TABLE_F"
        result = self.stairlight.up(
            table_name=table_name,
            recursive=True,
            verbose=False,
            response_type=ResponseType.TABLE.value,
        )
        assert sorted(result) == [
            "PROJECT_C.DATASET_C.TABLE_C",
            "PROJECT_J.DATASET_K.TABLE_L",
            "PROJECT_P.DATASET_Q.TABLE_R",
            "PROJECT_S.DATASET_T.TABLE_U",
            "PROJECT_V.DATASET_W.TABLE_X",
            "PROJECT_d.DATASET_d.TABLE_d",
            "PROJECT_e.DATASET_e.TABLE_e",
        ]

    def test_up_recursive_plain_file(self, tests_abspath: str):
        table_name = "PROJECT_D.DATASET_E.TABLE_F"
        result = self.stairlight.up(
            table_name=table_name,
            recursive=True,
            verbose=False,
            response_type=ResponseType.URI.value,
        )
        assert sorted(result) == [
            f"{tests_abspath}/sql/cte_multi_line.sql",
            f"{tests_abspath}/sql/cte_multi_line_params.sql",
            f"{tests_abspath}/sql/cte_multi_line_params_copy.sql",
            f"{tests_abspath}/sql/one_line_3.sql",
        ]

    def test_down_next(self):
        table_name = "PROJECT_C.DATASET_C.TABLE_C"
        result = self.stairlight.down(
            table_name=table_name, recursive=False, verbose=False
        )
        assert sorted(result) == [
            "PROJECT_D.DATASET_E.TABLE_F",
            "PROJECT_G.DATASET_H.TABLE_I",
            "PROJECT_d.DATASET_e.TABLE_f",
            "PROJECT_ds.DATASET_es.TABLE_fs",
        ]

    def test_down_recursive_verbose(self):
        actual: list[str] = []
        table_name = "PROJECT_C.DATASET_C.TABLE_C"
        result = self.stairlight.down(
            table_name=table_name, recursive=True, verbose=True
        )
        if isinstance(result, dict):
            actual = sorted(
                result[table_name][SearchDirection.DOWN.value][
                    "PROJECT_d.DATASET_e.TABLE_f"
                ][SearchDirection.DOWN.value].keys()
            )
        assert actual == [
            "PROJECT_j.DATASET_k.TABLE_l",
        ]

    def test_down_recursive_plain_table(self):
        table_name = "PROJECT_C.DATASET_C.TABLE_C"
        result = self.stairlight.down(
            table_name=table_name,
            recursive=True,
            verbose=False,
            response_type=ResponseType.TABLE.value,
        )
        assert sorted(result) == [
            "PROJECT_D.DATASET_E.TABLE_F",
            "PROJECT_G.DATASET_H.TABLE_I",
            "PROJECT_d.DATASET_e.TABLE_f",
            "PROJECT_ds.DATASET_es.TABLE_fs",
            "PROJECT_j.DATASET_k.TABLE_l",
        ]

    def test_down_recursive_plain_file(self, tests_abspath: str):
        table_name = "PROJECT_C.DATASET_C.TABLE_C"
        result = self.stairlight.down(
            table_name=table_name,
            recursive=True,
            verbose=False,
            response_type=ResponseType.URI.value,
        )
        assert sorted(result) == [
            f"{tests_abspath}/sql/cte_multi_line.sql",
            f"{tests_abspath}/sql/one_line_1.sql",
            "gs://stairlight/sql/cte/cte_multi_line.sql",
            "s3://stairlight/sql/cte/cte_multi_line.sql",
        ]

    def test_create_relative_map_up(self):
        target_table_name = "PROJECT_d.DATASET_d.TABLE_d"
        result = self.stairlight.create_relative_map(
            target_table_name=target_table_name, direction=SearchDirection.UP
        )
        assert "PROJECT_e.DATASET_e.TABLE_e" in result

    def test_create_relative_map_down(self):
        target_table_name = "PROJECT_A.DATASET_A.TABLE_A"
        result = self.stairlight.create_relative_map(
            target_table_name=target_table_name, direction=SearchDirection.DOWN
        )
        assert "PROJECT_A.DATASET_B.TABLE_C" in result

    def test_find_tables_by_labels_single(self):
        target_labels = ["Test:b"]
        result = self.stairlight.find_tables_by_labels(target_labels=target_labels)
        assert result == [
            "PROJECT_D.DATASET_E.TABLE_F",
            "PROJECT_G.DATASET_H.TABLE_I",
            "PROJECT_d.DATASET_e.TABLE_f",
        ]

    def test_find_tables_by_labels_double(self):
        target_labels = ["Test:b", "Source:gcs"]
        result = self.stairlight.find_tables_by_labels(target_labels=target_labels)
        assert result == ["PROJECT_d.DATASET_e.TABLE_f"]

    def test_is_target_label_found_true(self):
        target_labels = ["test:a", "group:c"]
        configured_labels = {
            "test": "a",
            "category": "b",
            "group": "c",
        }
        assert self.stairlight.is_target_label_found(
            target_labels=target_labels, configured_labels=configured_labels
        )

    def test_is_target_label_found_false(self):
        target_labels = ["test:a", "category:b", "group:c", "app:d"]
        configured_labels = {"test": "a", "category": "b", "group": "c"}
        assert not self.stairlight.is_target_label_found(
            target_labels=target_labels, configured_labels=configured_labels
        )

    def test_check_on_load(self, stairlight_save: StairLight):
        stairlight_load = StairLight(
            config_dir="tests/config", load_files=[stairlight_save.save_file]
        )
        assert not stairlight_load.check()

    def test_merge(self, stairlight_merge: StairLight):
        stairlight_merge.load_map()
        actual: dict[str, Any] = stairlight_merge.mapped
        with open("tests/expected/merged.json", "r") as f:
            expected: dict[str, Any] = json.load(f)
        assert actual == expected

    def test_cast_mapped_dict_all(self):
        casted = self.stairlight.cast_mapped_dict_all(mapped=self.stairlight.mapped)
        all_mapped_templates: list = []
        for upstairs in casted.values():
            for mapped_templates in upstairs.values():
                all_mapped_templates = all_mapped_templates + mapped_templates

        assert all([isinstance(actual, dict) for actual in all_mapped_templates])

    def test_get_templates_not_found(self):
        not_found = self.stairlight.get_templates_not_found()
        assert len(not_found) == 1
        assert not_found[0].endswith("tests/sql/not_found_test.sql")


@pytest.mark.integration
class TestStairLightNoConfig:
    stairlight = StairLight(config_dir="none")
    stairlight.create_map()

    def test_has_stairlight_config(self):
        assert not self.stairlight.has_stairlight_config()

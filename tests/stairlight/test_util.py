import pytest

import src.stairlight.util as st_util


class TestIsCyclic:
    @pytest.mark.parametrize(
        ("node_list", "expected"),
        [
            (["1", "2", "1", "2", "1", "2", "1", "2"], True),
            (["1", "2", "3", "2", "3", "2", "3"], True),
            (["1", "2", "3", "4", "5", "3", "4", "5"], True),
            (["1", "2", "3", "4", "5", "1", "2", "3", "4"], True),
            (["1", "2", "3", "4", "5"], False),
            (
                [
                    "PROJECT_D.DATASET_E.TABLE_F",
                    "PROJECT_J.DATASET_K.TABLE_L",
                    "PROJECT_P.DATASET_Q.TABLE_R",
                    "PROJECT_S.DATASET_T.TABLE_U",
                    "PROJECT_V.DATASET_W.TABLE_X",
                    "PROJECT_C.DATASET_C.TABLE_C",
                    "PROJECT_d.DATASET_d.TABLE_d",
                    "PROJECT_J.DATASET_K.TABLE_L",
                ],
                True,
            ),
        ],
        ids=[
            "each",
            "except_first",
            "except_first_two",
            "first_four",
            "not cyclic",
            "cyclic_tables",
        ],
    )
    def test_is_cyclic(self, node_list, expected):
        actual = st_util.is_cyclic(node_list)
        assert expected == actual


class TestDeepMerge:
    def test_success(self):
        original = {
            "key_01": {"key_01_01": "value_01_01", "key_01_02": "value_01_02"},
            "key_02": {
                "key_02_01": ["value_02_01", "value_02_02"],
                "key_02_02": "value_02_02",
            },
            "key_03": {
                "key_03_01": [{"value": [1, 2, 3]}, {"value": [4, 5, 6]}],
                "key_03_02": [{"value": [7, 8, 9]}],
            },
        }
        add = {
            "key_01": {"key_01_01": "value_01_03", "key_01_02": "value_01_04"},
            "key_02": {
                "key_02_01": ["value_02_01", "value_02_02", "value_02_03"],
            },
            "key_03": {
                "key_03_01": [{"value": [1, 2, 3]}],
                "key_03_02": [{"value": [10]}],
                "key_03_03": [{"value": [11, 12]}],
            },
        }
        expected = {
            "key_01": {"key_01_01": "value_01_01", "key_01_02": "value_01_02"},
            "key_02": {
                "key_02_01": ["value_02_01", "value_02_02", "value_02_03"],
                "key_02_02": "value_02_02",
            },
            "key_03": {
                "key_03_01": [{"value": [1, 2, 3]}, {"value": [4, 5, 6]}],
                "key_03_02": [{"value": [7, 8, 9]}, {"value": [10]}],
                "key_03_03": [{"value": [11, 12]}],
            },
        }
        actual = st_util.deep_merge(original=original, add=add)
        assert expected == actual

import os

from src.stairlight.stairlight import ResponseType, is_cyclic


class TestProperty:
    def test_mapped(self, stair_light):
        assert len(stair_light.mapped) > 0

    def test_unmapped(self, stair_light):
        file_keys = [
            unmapped_file.get("template_file") for unmapped_file in stair_light.unmapped
        ]
        assert len(file_keys) > 0


class TestSuccess:
    def test_up_next(self, stair_light):
        table_name = "PROJECT_D.DATASET_E.TABLE_F"
        result = stair_light.up(table_name=table_name, recursive=False, verbose=False)
        assert sorted(result) == [
            "PROJECT_C.DATASET_C.TABLE_C",
            "PROJECT_J.DATASET_K.TABLE_L",
            "PROJECT_d.DATASET_d.TABLE_d",
        ]

    def test_up_recursive_verbose(self, stair_light):
        table_name = "PROJECT_D.DATASET_E.TABLE_F"
        result = stair_light.up(table_name=table_name, recursive=True, verbose=True)
        assert sorted(
            result[table_name]["upstream"]["PROJECT_J.DATASET_K.TABLE_L"][
                "upstream"
            ].keys()
        ) == [
            "PROJECT_P.DATASET_Q.TABLE_R",
            "PROJECT_S.DATASET_T.TABLE_U",
            "PROJECT_V.DATASET_W.TABLE_X",
        ]

    def test_up_recursive_plain_table(self, stair_light):
        table_name = "PROJECT_D.DATASET_E.TABLE_F"
        result = stair_light.up(
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

    def test_up_recursive_plain_file(self, stair_light):
        table_name = "PROJECT_D.DATASET_E.TABLE_F"
        result = stair_light.up(
            table_name=table_name,
            recursive=True,
            verbose=False,
            response_type=ResponseType.FILE.value,
        )
        current_dir = os.path.dirname(os.path.abspath(__file__))
        assert sorted(result) == [
            f"{current_dir}/sql/main/test_b.sql",
            f"{current_dir}/sql/main/test_c.sql",
            f"{current_dir}/sql/main/test_f.sql",
        ]

    def test_down_next(self, stair_light):
        table_name = "PROJECT_C.DATASET_C.TABLE_C"
        result = stair_light.down(table_name=table_name, recursive=False, verbose=False)
        assert sorted(result) == [
            "PROJECT_D.DATASET_E.TABLE_F",
            "PROJECT_G.DATASET_H.TABLE_I",
            "PROJECT_d.DATASET_e.TABLE_f",
        ]

    def test_down_recursive_verbose(self, stair_light):
        table_name = "PROJECT_C.DATASET_C.TABLE_C"
        result = stair_light.down(table_name=table_name, recursive=True, verbose=True)
        assert sorted(
            result[table_name]["downstream"]["PROJECT_d.DATASET_e.TABLE_f"][
                "downstream"
            ].keys()
        ) == [
            "PROJECT_j.DATASET_k.TABLE_l",
        ]

    def test_down_recursive_plain_table(self, stair_light):
        table_name = "PROJECT_C.DATASET_C.TABLE_C"
        result = stair_light.down(
            table_name=table_name,
            recursive=True,
            verbose=False,
            response_type=ResponseType.TABLE.value,
        )
        assert sorted(result) == [
            "PROJECT_D.DATASET_E.TABLE_F",
            "PROJECT_G.DATASET_H.TABLE_I",
            "PROJECT_d.DATASET_e.TABLE_f",
            "PROJECT_j.DATASET_k.TABLE_l",
        ]

    def test_down_recursive_plain_file(self, stair_light):
        table_name = "PROJECT_C.DATASET_C.TABLE_C"
        result = stair_light.down(
            table_name=table_name,
            recursive=True,
            verbose=False,
            response_type=ResponseType.FILE.value,
        )
        current_dir = os.path.dirname(os.path.abspath(__file__))
        assert sorted(result) == [
            f"{current_dir}/sql/main/test_b.sql",
            f"{current_dir}/sql/main/test_d.sql",
            "gs://stairlight/sql/test_b/test_b.sql",
        ]


class TestIsCyclic:
    def test_a(self):
        node_list = [1, 2, 1, 2, 1, 2, 1, 2]
        assert is_cyclic(node_list)

    def test_b(self):
        node_list = [1, 2, 3, 2, 3, 2, 3]
        assert is_cyclic(node_list)

    def test_c(self):
        node_list = [1, 2, 3, 4, 5, 3, 4, 5]
        assert is_cyclic(node_list)

    def test_d(self):
        node_list = [1, 2, 3, 4, 5, 1, 2, 3, 4]
        assert is_cyclic(node_list)

    def test_e(self):
        node_list = [1, 2, 3, 4, 5]
        assert not is_cyclic(node_list)

    def test_f(self):
        node_list = [
            "PROJECT_D.DATASET_E.TABLE_F",
            "PROJECT_J.DATASET_K.TABLE_L",
            "PROJECT_P.DATASET_Q.TABLE_R",
            "PROJECT_S.DATASET_T.TABLE_U",
            "PROJECT_V.DATASET_W.TABLE_X",
            "PROJECT_C.DATASET_C.TABLE_C",
            "PROJECT_d.DATASET_d.TABLE_d",
            "PROJECT_J.DATASET_K.TABLE_L",
        ]
        assert is_cyclic(node_list)

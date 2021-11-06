class TestProperty:
    def test_maps(self, stair_light):
        assert len(stair_light.maps) > 0

    def test_undefined_files(self, stair_light):
        file_keys = [
            undefined_file.get("template_file")
            for undefined_file in stair_light.undefined_files
        ]
        assert len(file_keys) > 0


class TestSuccess:
    def test_all(self, stair_light):
        assert stair_light.all() == stair_light.maps

    def test_up_recursive_verbose(self, stair_light):
        table_name = "PROJECT_D.DATASET_E.TABLE_F"
        result = stair_light.up(table_name=table_name, recursive=True, verbose=True)
        assert sorted(result[table_name]["upstream"].keys()) == [
            "PROJECT_C.DATASET_C.TABLE_C",
            "PROJECT_J.DATASET_K.TABLE_L",
            "PROJECT_d.DATASET_d.TABLE_d",
        ]

    def test_down_recursive_verbose(self, stair_light):
        table_name = "PROJECT_C.DATASET_C.TABLE_C"
        result = stair_light.down(table_name=table_name, recursive=True, verbose=True)
        assert sorted(result[table_name]["downstream"].keys()) == [
            "PROJECT_D.DATASET_E.TABLE_F",
            "PROJECT_G.DATASET_H.TABLE_I",
            "PROJECT_d.DATASET_e.TABLE_f",
        ]

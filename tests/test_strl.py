import json

from stairlight import StairLight


class TestSuccess:
    stair_light = StairLight()

    def test_all(self):
        assert json.loads(self.stair_light.all()) == self.stair_light.maps

    def test_up(self):
        result = self.stair_light.up(table_name="PROJECT_D.DATASET_E.TABLE_F")
        assert sorted(json.loads(result).keys()) == [
            "PROJECT_C.DATASET_C.TABLE_C",
            "PROJECT_J.DATASET_K.TABLE_L",
            "PROJECT_d.DATASET_d.TABLE_d",
        ]

    def test_down(self):
        result = self.stair_light.down(table_name="PROJECT_C.DATASET_C.TABLE_C")
        assert sorted(json.loads(result).keys()) == [
            "PROJECT_D.DATASET_E.TABLE_F",
            "PROJECT_G.DATASET_H.TABLE_I",
        ]

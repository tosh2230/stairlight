import json


class TestProperty:
    def test_map(self, stair_light):
        assert len(stair_light.maps) > 0

    def test_undefined_files(self, stair_light):
        assert len(stair_light.undefined_files) == 2


class TestSuccess:
    def test_all(self, stair_light):
        assert json.loads(stair_light.all()) == stair_light.maps

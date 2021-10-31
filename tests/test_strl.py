from stairlight import StairLight


class TestSearchFilesSuccess:
    stair_light = StairLight()

    def test_search_files(self):
        assert len(self.stair_light.maps) > 0

from stairlight.map import Map


class TestSuccess:
    dependency_map = Map()
    dependency_map.create()

    def test_maps(self):
        assert len(self.dependency_map.maps) > 0

    def test_undefined_files(self):
        assert len(self.dependency_map.undefined_files) == 3

from stairlight.map import Map


class TestSuccess:
    dependency_map = Map()
    dependency_map.create()

    def test_maps(self):
        assert len(self.dependency_map.maps) > 0

    def test_undefined_files(self):
        assert sorted(self.dependency_map.undefined_files) == [
            "tests/sql/test_a.sql",
            "tests/sql/test_b.sql",
        ]

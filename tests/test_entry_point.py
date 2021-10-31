import os

from stairlight.entrypoint import StairLight


class TestSearchFilesSuccess:
    config_file = "config/lddr.yaml"
    tests_dir = str(os.path.dirname(os.path.abspath(__file__)))
    template_dir = f"{tests_dir}/sql"
    condition = "**/*.sql"
    stair_light = StairLight(
        config_file=config_file, template_dir=template_dir, condition=condition
    )

    def test_search_files(self):
        assert len(self.stair_light.maps) > 0

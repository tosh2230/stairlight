import os

from lddr.entrypoint import Ladder


class TestSearchFilesSuccess:
    config_file = 'config/lddr.yaml'
    tests_dir = str(os.path.dirname(os.path.abspath(__file__)))
    template_dir = f'{tests_dir}/sql'
    condition = '**/*.sql'
    ladder = Ladder(
        config_file=config_file,
        template_dir=template_dir,
        condition=condition
    )

    def test_search_files(self):
        assert len(self.ladder.maps) > 0

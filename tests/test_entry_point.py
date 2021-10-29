import os
from lddr.entrypoint import Ladder
from lddr.query_parser import QueryParser

class TestSearchFilesSuccess:
    config_file = 'config/lddr.yaml'
    target_dir = os.path.dirname(__file__)
    condition = 'sql/*.sql'
    ladder = Ladder(
        config_file=config_file,
        target_dir=target_dir,
        condition=condition
    )

    def test_search_files(self):
        assert len(self.ladder.maps) > 0

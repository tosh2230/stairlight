import os
from msb.entrypoint import Msb
from msb.query_parser import QueryParser

class TestSearchFilesSuccess:
    target_dir = os.path.dirname(__file__)
    condition = 'sql/*.sql'
    msb = Msb(target_dir=target_dir, condition=condition)

    def test_search_files(self):
        results = self.msb.search_files()
        assert len(results) > 0

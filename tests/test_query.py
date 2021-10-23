import os
from msby import query

class TestSearchSuccess:
    def test_search_files(self):
        target_dir = os.path.dirname(__file__)
        condition = 'sql/*.sql'
        results = query.search_files(target_dir, condition)
        assert len(results) > 0

    def test_detect_tables_a(self):
        sql_path = './tests/sql/test_a.sql'
        parser = query.QueryParser(sql_path)
        results = parser.detect_tables()
        assert results == {
            'main': ['PROJECT_A.DATASET_A.TABLE_A'],
            'cte': [],
        }

    def test_detect_tables_b(self):
        sql_path = './tests/sql/test_b.sql'
        parser = query.QueryParser(sql_path)
        results = parser.detect_tables()
        assert results == {
            'main': ['PROJECT_B.DATASET_B.TABLE_B'],
            'cte': [
                'PROJECT_C.DATASET_C.TABLE_C',
                'PROJECT_d.DATASET_d.TABLE_d',
            ]
        }

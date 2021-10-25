import os
from msby import query

class TestSearchFilesSuccess:
    def test_search_files(self):
        target_dir = os.path.dirname(__file__)
        condition = 'sql/*.sql'
        results = query.search_files(target_dir, condition)
        assert len(results) > 0

class TestSearchSuccess:
    def test_parse_query_str(self):
        query_str = 'SELECT * FROM PROJECT_X.DATASET_X.TABLE_X INNER JOIN PROJECT_X.DATASET_X.TABLE_Y USING(ID)'
        parser = query.QueryParser(query_str=query_str)
        results = parser.parse_query()
        assert results == {
            'main': [
                'PROJECT_X.DATASET_X.TABLE_X',
                'PROJECT_X.DATASET_X.TABLE_Y',
            ],
            'cte': [],
        }

    def test_parse_query_file_a(self):
        sql_path = './tests/sql/test_a.sql'
        parser = query.QueryParser.read_file(sql_path=sql_path)
        results = parser.parse_query()
        assert results == {
            'main': ['PROJECT_A.DATASET_A.TABLE_A'],
            'cte': [],
        }

    def test_parse_query_file_b(self):
        sql_path = './tests/sql/test_b.sql'
        parser = query.QueryParser.read_file(sql_path=sql_path)
        results = parser.parse_query()
        assert results == {
            'main': ['PROJECT_B.DATASET_B.TABLE_B'],
            'cte': [
                'PROJECT_C.DATASET_C.TABLE_C',
                'PROJECT_d.DATASET_d.TABLE_d',
            ]
        }

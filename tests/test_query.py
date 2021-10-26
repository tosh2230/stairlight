import os
import json
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
        assert results == [
            {
                'table_name': 'PROJECT_X.DATASET_X.TABLE_X',
                'line': 1,
                'line_str': 'SELECT * FROM PROJECT_X.DATASET_X.TABLE_X INNER JOIN PROJECT_X.DATASET_X.TABLE_Y USING(ID)',
            },{
                'table_name': 'PROJECT_X.DATASET_X.TABLE_Y',
                'line': 1,
                'line_str': 'SELECT * FROM PROJECT_X.DATASET_X.TABLE_X INNER JOIN PROJECT_X.DATASET_X.TABLE_Y USING(ID)',
            }
        ]

    def test_parse_query_file_a(self):
        sql_path = './tests/sql/test_a.sql'
        parser = query.QueryParser.read_file(sql_path=sql_path)
        results = parser.parse_query()
        assert results == [
            {
                'table_name': 'PROJECT_A.DATASET_A.TABLE_A',
                'line': 1,
                'line_str': 'SELECT * FROM PROJECT_A.DATASET_A.TABLE_A WHERE 1 = 1',
            }
        ]

    def test_parse_query_file_b(self):
        sql_path = './tests/sql/test_b.sql'
        parser = query.QueryParser.read_file(sql_path=sql_path)
        results = parser.parse_query()
        assert results == [
            {
                'table_name': 'PROJECT_B.DATASET_B.TABLE_B',
                'line': 23,
                'line_str': '    PROJECT_B.DATASET_B.TABLE_B AS b',
            },{
                'table_name': 'PROJECT_C.DATASET_C.TABLE_C',
                'line': 6,
                'line_str': '        PROJECT_C.DATASET_C.TABLE_C',
            },{
                'table_name': 'PROJECT_d.DATASET_d.TABLE_d',
                'line': 15,
                'line_str': '        PROJECT_d.DATASET_d.TABLE_d',
            }
        ]

    def test_map_tables(self):
        map = {}
        sql_path = './tests/sql/test_b.sql'
        table_name = 'PROJECT_B.DATASET_B.TABLE_A'
        parser = query.QueryParser.read_file(sql_path=sql_path)
        ref_tables = parser.parse_query()
        map = parser.map_tables(map=map, table_name=table_name, ref_tables=ref_tables)
        expected = {
            table_name: {
                'PROJECT_d.DATASET_d.TABLE_d': {
                    'file': sql_path,
                    'line': 15,
                    'line_str': '        PROJECT_d.DATASET_d.TABLE_d',
                },
                'PROJECT_B.DATASET_B.TABLE_B': {
                    'file': sql_path,
                    'line': 23,
                    'line_str': '    PROJECT_B.DATASET_B.TABLE_B AS b',
                },
                'PROJECT_C.DATASET_C.TABLE_C': {
                    'file': sql_path,
                    'line': 6,
                    'line_str': '        PROJECT_C.DATASET_C.TABLE_C',
                },
            }
        }
        assert sorted(list(map[table_name].keys())) == sorted(list(expected[table_name].keys()))

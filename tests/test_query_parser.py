import os
from msb.query_parser import QueryParser

class TestSearchSuccess:
    def test_parse_query_str(self):
        query_str = 'SELECT * FROM PROJECT_X.DATASET_X.TABLE_X INNER JOIN PROJECT_X.DATASET_X.TABLE_Y USING(ID)'
        parser = QueryParser(query_str=query_str)
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
        parser = QueryParser.read_file(sql_path=sql_path)
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
        parser = QueryParser.read_file(sql_path=sql_path)
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

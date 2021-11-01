from stairlight.query import Query


class TestSuccess:
    def test_parse_query(self):
        query_str = (
            "SELECT * FROM PROJECT_X.DATASET_X.TABLE_X "
            "INNER JOIN PROJECT_X.DATASET_X.TABLE_Y USING(ID)"
        )
        query = Query(query_str=query_str)
        results = []
        for result in query.parse():
            results.append(result)
        assert results == [
            {
                "table_name": "PROJECT_X.DATASET_X.TABLE_X",
                "line": 1,
                "line_str": (
                    "SELECT * FROM PROJECT_X.DATASET_X.TABLE_X "
                    "INNER JOIN PROJECT_X.DATASET_X.TABLE_Y USING(ID)"
                ),
            },
            {
                "table_name": "PROJECT_X.DATASET_X.TABLE_Y",
                "line": 1,
                "line_str": (
                    "SELECT * FROM PROJECT_X.DATASET_X.TABLE_X "
                    "INNER JOIN PROJECT_X.DATASET_X.TABLE_Y USING(ID)"
                ),
            },
        ]

    def test_parse_file_a(self):
        with open("tests/sql/test_a.sql") as f:
            query_str = f.read()
        query = Query(query_str=query_str)
        results = []
        for result in query.parse():
            results.append(result)
        assert results == [
            {
                "table_name": "PROJECT_A.DATASET_A.TABLE_A",
                "line": 1,
                "line_str": (
                    "SELECT * FROM PROJECT_A.DATASET_A.TABLE_A " "WHERE 1 = 1"
                ),
            }
        ]

    def test_parse_file_b(self):
        with open("tests/sql/test_b.sql") as f:
            query_str = f.read()
        query = Query(query_str=query_str)
        results = []
        for result in query.parse():
            results.append(result)
        assert results == [
            {
                "table_name": "PROJECT_B.DATASET_B.TABLE_B",
                "line": 23,
                "line_str": "    PROJECT_B.DATASET_B.TABLE_B AS b",
            },
            {
                "table_name": "PROJECT_C.DATASET_C.TABLE_C",
                "line": 6,
                "line_str": "        PROJECT_C.DATASET_C.TABLE_C",
            },
            {
                "table_name": "PROJECT_d.DATASET_d.TABLE_d",
                "line": 15,
                "line_str": "        PROJECT_d.DATASET_d.TABLE_d",
            },
        ]

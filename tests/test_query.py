from stairlight.query import Query, solve_table_prefix


class TestSuccess:
    def test_parse_query(self):
        query_str = (
            "SELECT * FROM PROJECT_X.DATASET_X.TABLE_X "
            "INNER JOIN PROJECT_X.DATASET_X.TABLE_Y USING(ID)"
        )
        query = Query(query_str=query_str)
        results = []
        for result in query.parse_upstream():
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

    def test_parse_cte_one_line(self):
        with open("tests/sql/query/cte_one_line.sql") as f:
            query_str = f.read()
        query = Query(query_str=query_str)
        results = []
        for result in query.parse_upstream():
            results.append(result)
        assert results == [
            {
                "table_name": "PROJECT_B.DATASET_B.TABLE_B",
                "line": 1,
                "line_str": query_str,
            },
            {
                "table_name": "PROJECT_C.DATASET_C.TABLE_C",
                "line": 1,
                "line_str": query_str,
            },
            {
                "table_name": "PROJECT_d.DATASET_d.TABLE_d",
                "line": 1,
                "line_str": query_str,
            },
        ]

    def test_parse_cte_multi_line(self):
        with open("tests/sql/query/cte_multi_line.sql") as f:
            query_str = f.read()
        query = Query(query_str=query_str)
        results = []
        for result in query.parse_upstream():
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

    def test_parse_nested_join(self):
        with open("tests/sql/query/nested_join.sql") as f:
            query_str = f.read()
        query = Query(query_str=query_str)
        results = []
        for result in query.parse_upstream():
            results.append(result)
        assert results == [
            {
                "table_name": "PROJECT_B.DATASET_B.TABLE_B",
                "line": 4,
                "line_str": "    PROJECT_B.DATASET_B.TABLE_B AS b",
            },
            {
                "table_name": "PROJECT_C.DATASET_C.TABLE_C",
                "line": 10,
                "line_str": "            PROJECT_C.DATASET_C.TABLE_C",
            },
            {
                "table_name": "PROJECT_d.DATASET_d.TABLE_d",
                "line": 20,
                "line_str": "            PROJECT_d.DATASET_d.TABLE_d d",
            },
            {
                "table_name": "PROJECT_e.DATASET_e.TABLE_e",
                "line": 21,
                "line_str": "            LEFT OUTER JOIN PROJECT_e.DATASET_e.TABLE_e",
            },
        ]

    def test_solve_table_prefix_one(self):
        table = "DATASET_d.TABLE_d"
        default_table_prefix = "PROJECT_A"
        assert (
            solve_table_prefix(table=table, default_table_prefix=default_table_prefix)
            == "PROJECT_A.DATASET_d.TABLE_d"
        )

    def test_solve_table_prefix_two(self):
        table = "TABLE_d"
        default_table_prefix = "PROJECT_A.DATASET_A"
        assert (
            solve_table_prefix(table=table, default_table_prefix=default_table_prefix)
            == "PROJECT_A.DATASET_A.TABLE_d"
        )

    def test_solve_table_prefix_nothing(self):
        table = "PROJECT_d.DATASET_d.TABLE_d"
        default_table_prefix = "PROJECT_A"
        assert (
            solve_table_prefix(table=table, default_table_prefix=default_table_prefix)
            == "PROJECT_d.DATASET_d.TABLE_d"
        )

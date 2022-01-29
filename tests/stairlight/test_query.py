from src.stairlight.query import Query, solve_table_prefix
from src.stairlight import map_key


class TestSuccess:
    def test_parse_query(self):
        query_str = (
            "SELECT * FROM PROJECT_X.DATASET_X.TABLE_X "
            "INNER JOIN PROJECT_X.DATASET_X.TABLE_Y USING(ID)"
        )
        query = Query(query_str=query_str)
        results = []
        for result in query.get_upstairs_attributes_iter():
            results.append(result)
        assert results == [
            {
                map_key.TABLE_NAME: "PROJECT_X.DATASET_X.TABLE_X",
                map_key.LINE_NUMBER: 1,
                map_key.LINE_STRING: (
                    "SELECT * FROM PROJECT_X.DATASET_X.TABLE_X "
                    "INNER JOIN PROJECT_X.DATASET_X.TABLE_Y USING(ID)"
                ),
            },
            {
                map_key.TABLE_NAME: "PROJECT_X.DATASET_X.TABLE_Y",
                map_key.LINE_NUMBER: 1,
                map_key.LINE_STRING: (
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
        for result in query.get_upstairs_attributes_iter():
            results.append(result)
        assert results == [
            {
                map_key.TABLE_NAME: "PROJECT_B.DATASET_B.TABLE_B",
                map_key.LINE_NUMBER: 1,
                map_key.LINE_STRING: query_str,
            },
            {
                map_key.TABLE_NAME: "PROJECT_C.DATASET_C.TABLE_C",
                map_key.LINE_NUMBER: 1,
                map_key.LINE_STRING: query_str,
            },
            {
                map_key.TABLE_NAME: "PROJECT_d.DATASET_d.TABLE_d",
                map_key.LINE_NUMBER: 1,
                map_key.LINE_STRING: query_str,
            },
        ]

    def test_parse_cte_multi_line(self):
        with open("tests/sql/query/cte_multi_line.sql") as f:
            query_str = f.read()
        query = Query(query_str=query_str)
        results = []
        for result in query.get_upstairs_attributes_iter():
            results.append(result)
        assert results == [
            {
                map_key.TABLE_NAME: "PROJECT_B.DATASET_B.TABLE_B",
                map_key.LINE_NUMBER: 23,
                map_key.LINE_STRING: "    PROJECT_B.DATASET_B.TABLE_B AS b",
            },
            {
                map_key.TABLE_NAME: "PROJECT_C.DATASET_C.TABLE_C",
                map_key.LINE_NUMBER: 6,
                map_key.LINE_STRING: "        PROJECT_C.DATASET_C.TABLE_C",
            },
            {
                map_key.TABLE_NAME: "PROJECT_d.DATASET_d.TABLE_d",
                map_key.LINE_NUMBER: 15,
                map_key.LINE_STRING: "        PROJECT_d.DATASET_d.TABLE_d",
            },
        ]

    def test_parse_nested_join(self):
        with open("tests/sql/query/nested_join.sql") as f:
            query_str = f.read()
        query = Query(query_str=query_str)
        results = []
        for result in query.get_upstairs_attributes_iter():
            results.append(result)
        assert results == [
            {
                map_key.TABLE_NAME: "PROJECT_B.DATASET_B.TABLE_B",
                map_key.LINE_NUMBER: 4,
                map_key.LINE_STRING: "    PROJECT_B.DATASET_B.TABLE_B AS b",
            },
            {
                map_key.TABLE_NAME: "PROJECT_C.DATASET_C.TABLE_C",
                map_key.LINE_NUMBER: 10,
                map_key.LINE_STRING: "            PROJECT_C.DATASET_C.TABLE_C",
            },
            {
                map_key.TABLE_NAME: "PROJECT_d.DATASET_d.TABLE_d",
                map_key.LINE_NUMBER: 20,
                map_key.LINE_STRING: "            PROJECT_d.DATASET_d.TABLE_d d",
            },
            {
                map_key.TABLE_NAME: "PROJECT_e.DATASET_e.TABLE_e",
                map_key.LINE_NUMBER: 21,
                map_key.LINE_STRING: "            LEFT OUTER JOIN PROJECT_e.DATASET_e.TABLE_e",
            },
        ]

    def test_union_same_table(self):
        with open("tests/sql/query/union_same_table.sql") as f:
            query_str = f.read()
        query = Query(query_str=query_str)
        results = []
        for result in query.get_upstairs_attributes_iter():
            results.append(result)
        assert results == [
            {
                map_key.TABLE_NAME: "test_project.beam_streaming.taxirides_realtime",
                map_key.LINE_NUMBER: 6,
                map_key.LINE_STRING: "    test_project.beam_streaming.taxirides_realtime",
            },
            {
                map_key.TABLE_NAME: "test_project.beam_streaming.taxirides_realtime",
                map_key.LINE_NUMBER: 15,
                map_key.LINE_STRING: "    test_project.beam_streaming.taxirides_realtime",
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

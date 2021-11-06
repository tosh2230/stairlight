from stairlight.query import Query
from stairlight.template import SQLTemplate, SourceType


class TestSuccess:
    def test_render_fs(self):
        params = {
            "main_table": "PROJECT_P.DATASET_Q.TABLE_R",
            "sub_table_01": "PROJECT_S.DATASET_T.TABLE_U",
            "sub_table_02": "PROJECT_V.DATASET_W.TABLE_X",
        }
        sql_template = SQLTemplate(SourceType.FS, "tests/sql/main/test_c.sql", params)
        query_str = Query.render_fs(sql_template, params)
        expected = """WITH c AS (
    SELECT
        test_id,
        col_c
    FROM
        PROJECT_S.DATASET_T.TABLE_U
    WHERE
        0 = 0
),
d AS (
    SELECT
        test_id,
        col_d
    FROM
        PROJECT_V.DATASET_W.TABLE_X
    WHERE
        0 = 0
)

SELECT
    *
FROM
    PROJECT_P.DATASET_Q.TABLE_R AS b
    INNER JOIN c
        ON b.test_id = c.test_id
    INNER JOIN d
        ON b.test_id = d.test_id
WHERE
    1 = 1"""
        assert query_str == expected

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

    def test_parse_cte_one_line(self):
        with open("tests/sql/query/cte_one_line.sql") as f:
            query_str = f.read()
        query = Query(query_str=query_str)
        results = []
        for result in query.parse():
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

    def test_parse_nested_join(self):
        with open("tests/sql/query/nested_join.sql") as f:
            query_str = f.read()
        query = Query(query_str=query_str)
        results = []
        for result in query.parse():
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

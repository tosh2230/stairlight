import pytest

from src.stairlight.query import Query, UpstairTableReference, solve_table_prefix
from src.stairlight.source.config import MapKey


class TestSuccess:
    def test_parse_new_line(self):
        query_str = (
            "SELECT * FROM PROJECT_X.DATASET_X.TABLE_X \n"
            "INNER JOIN PROJECT_X.DATASET_X.TABLE_Y USING(ID)"
        )
        query = Query(query_str=query_str)
        results: list[UpstairTableReference] = []
        for result in query.detect_upstair_table_reference():
            results.append(result)
        assert results == [
            UpstairTableReference(
                TableName="PROJECT_X.DATASET_X.TABLE_X",
                Line={
                    MapKey.LINE_NUMBER: 1,
                    MapKey.LINE_STRING: "SELECT * FROM PROJECT_X.DATASET_X.TABLE_X ",
                },
            ),
            UpstairTableReference(
                TableName="PROJECT_X.DATASET_X.TABLE_Y",
                Line={
                    MapKey.LINE_NUMBER: 2,
                    MapKey.LINE_STRING: (
                        "INNER JOIN PROJECT_X.DATASET_X.TABLE_Y USING(ID)"
                    ),
                },
            ),
        ]

    @pytest.mark.parametrize(
        ("file", "expected"),
        [
            (
                "tests/sql/query/cte_one_line.sql",
                [
                    UpstairTableReference(
                        TableName="PROJECT_B.DATASET_B.TABLE_B",
                        Line={
                            MapKey.LINE_NUMBER: 1,
                            MapKey.LINE_STRING: (
                                "WITH c AS (SELECT test_id, col_c "
                                "FROM PROJECT_C.DATASET_C.TABLE_C WHERE 0 = 0),"
                                "d AS ("
                                "SELECT test_id, col_d "
                                "FROM PROJECT_d.DATASET_d.TABLE_d "
                                "WHERE 0 = 0) "
                                "SELECT * FROM PROJECT_B.DATASET_B.TABLE_B AS b "
                                "INNER JOIN c ON b.test_id = c.test_id "
                                "INNER JOIN d ON b.test_id = d.test_id WHERE 1 = 1"
                            ),
                        },
                    ),
                    UpstairTableReference(
                        TableName="PROJECT_C.DATASET_C.TABLE_C",
                        Line={
                            MapKey.LINE_NUMBER: 1,
                            MapKey.LINE_STRING: (
                                "WITH c AS (SELECT test_id, col_c "
                                "FROM PROJECT_C.DATASET_C.TABLE_C WHERE 0 = 0),"
                                "d AS ("
                                "SELECT test_id, col_d "
                                "FROM PROJECT_d.DATASET_d.TABLE_d "
                                "WHERE 0 = 0) "
                                "SELECT * FROM PROJECT_B.DATASET_B.TABLE_B AS b "
                                "INNER JOIN c ON b.test_id = c.test_id "
                                "INNER JOIN d ON b.test_id = d.test_id WHERE 1 = 1"
                            ),
                        },
                    ),
                    UpstairTableReference(
                        TableName="PROJECT_d.DATASET_d.TABLE_d",
                        Line={
                            MapKey.LINE_NUMBER: 1,
                            MapKey.LINE_STRING: (
                                "WITH c AS (SELECT test_id, col_c "
                                "FROM PROJECT_C.DATASET_C.TABLE_C WHERE 0 = 0),"
                                "d AS ("
                                "SELECT test_id, col_d "
                                "FROM PROJECT_d.DATASET_d.TABLE_d "
                                "WHERE 0 = 0) "
                                "SELECT * FROM PROJECT_B.DATASET_B.TABLE_B AS b "
                                "INNER JOIN c ON b.test_id = c.test_id "
                                "INNER JOIN d ON b.test_id = d.test_id WHERE 1 = 1"
                            ),
                        },
                    ),
                ],
            ),
            (
                "tests/sql/query/cte_multi_line.sql",
                [
                    UpstairTableReference(
                        TableName="PROJECT_B.DATASET_B.TABLE_B",
                        Line={
                            MapKey.LINE_NUMBER: 25,
                            MapKey.LINE_STRING: "    PROJECT_B.DATASET_B.TABLE_B AS b",
                        },
                    ),
                    UpstairTableReference(
                        TableName="PROJECT_C.DATASET_C.TABLE_C",
                        Line={
                            MapKey.LINE_NUMBER: 7,
                            MapKey.LINE_STRING: "        PROJECT_C.DATASET_C.TABLE_C",
                        },
                    ),
                    UpstairTableReference(
                        TableName="PROJECT_d.DATASET_d.TABLE_d",
                        Line={
                            MapKey.LINE_NUMBER: 17,
                            MapKey.LINE_STRING: "        PROJECT_d.DATASET_d.TABLE_d",
                        },
                    ),
                ],
            ),
            (
                "tests/sql/query/nested_join.sql",
                [
                    UpstairTableReference(
                        TableName="PROJECT_B.DATASET_B.TABLE_B",
                        Line={
                            MapKey.LINE_NUMBER: 4,
                            MapKey.LINE_STRING: "    PROJECT_B.DATASET_B.TABLE_B AS b",
                        },
                    ),
                    UpstairTableReference(
                        TableName="PROJECT_C.DATASET_C.TABLE_C",
                        Line={
                            MapKey.LINE_NUMBER: 10,
                            MapKey.LINE_STRING: "            PROJECT_C.DATASET_C.TABLE_C",
                        },
                    ),
                    UpstairTableReference(
                        TableName="PROJECT_d.DATASET_d.TABLE_d",
                        Line={
                            MapKey.LINE_NUMBER: 20,
                            MapKey.LINE_STRING: (
                                "            PROJECT_d.DATASET_d.TABLE_d d"
                            ),
                        },
                    ),
                    UpstairTableReference(
                        TableName="PROJECT_e.DATASET_e.TABLE_e",
                        Line={
                            MapKey.LINE_NUMBER: 21,
                            MapKey.LINE_STRING: (
                                "            LEFT OUTER JOIN "
                                "PROJECT_e.DATASET_e.TABLE_e"
                            ),
                        },
                    ),
                ],
            ),
            (
                "tests/sql/query/union_same_table.sql",
                [
                    UpstairTableReference(
                        TableName=("test_project.beam_streaming.taxirides_realtime"),
                        Line={
                            MapKey.LINE_NUMBER: 6,
                            MapKey.LINE_STRING: (
                                "    test_project.beam_streaming.taxirides_realtime"
                            ),
                        },
                    ),
                    UpstairTableReference(
                        TableName=("test_project.beam_streaming.taxirides_realtime"),
                        Line={
                            MapKey.LINE_NUMBER: 15,
                            MapKey.LINE_STRING: (
                                "    test_project.beam_streaming.taxirides_realtime"
                            ),
                        },
                    ),
                ],
            ),
            (
                "tests/sql/query/cte_multi_tables_01.sql",
                [
                    UpstairTableReference(
                        TableName="project.dataset.table_test_A",
                        Line={
                            MapKey.LINE_NUMBER: 6,
                            MapKey.LINE_STRING: "		project.dataset.table_test_A",
                        },
                    ),
                    UpstairTableReference(
                        TableName="project.dataset.table_test_B",
                        Line={
                            MapKey.LINE_NUMBER: 13,
                            MapKey.LINE_STRING: "		project.dataset.table_test_B AS test_B",
                        },
                    ),
                    UpstairTableReference(
                        TableName="project.dataset.table_test_C",
                        Line={
                            MapKey.LINE_NUMBER: 19,
                            MapKey.LINE_STRING: (
                                "FROM project.dataset.table_test_C AS test_C"
                            ),
                        },
                    ),
                ],
            ),
            (
                "tests/sql/query/cte_multi_tables_02.sql",
                [
                    UpstairTableReference(
                        TableName="project.dataset.table_test_A",
                        Line={
                            MapKey.LINE_NUMBER: 6,
                            MapKey.LINE_STRING: (
                                "		project.dataset.table_test_A -- table_test_B"
                            ),
                        },
                    ),
                    UpstairTableReference(
                        TableName="project.dataset.table_test_B",
                        Line={
                            MapKey.LINE_NUMBER: 12,
                            MapKey.LINE_STRING: "		project.dataset.table_test_B AS test_B",
                        },
                    ),
                    UpstairTableReference(
                        TableName="project.dataset.table_test_C",
                        Line={
                            MapKey.LINE_NUMBER: 19,
                            MapKey.LINE_STRING: "		project.dataset.table_test_C AS test_C",
                        },
                    ),
                    UpstairTableReference(
                        TableName="project.dataset.table_test_D",
                        Line={
                            MapKey.LINE_NUMBER: 26,
                            MapKey.LINE_STRING: (
                                "FROM project.dataset.table_test_D AS test_D"
                            ),
                        },
                    ),
                ],
            ),
            (
                "tests/sql/query/backtick_each_elements.sql",
                [
                    UpstairTableReference(
                        TableName="dummy.dummy.my_first_dbt_model",
                        Line={
                            MapKey.LINE_NUMBER: 4,
                            MapKey.LINE_STRING: (
                                "from `dummy`.`dummy`.`my_first_dbt_model`"
                            ),
                        },
                    ),
                ],
            ),
            (
                "tests/sql/query/backtick_whole_element.sql",
                [
                    UpstairTableReference(
                        TableName="dummy.dummy.my_first_dbt_model",
                        Line={
                            MapKey.LINE_NUMBER: 4,
                            MapKey.LINE_STRING: "from `dummy.dummy.my_first_dbt_model`",
                        },
                    ),
                ],
            ),
            (
                "tests/sql/query/google_bigquery_unnest_in_exists.sql",
                [
                    UpstairTableReference(
                        TableName="PROJECT_d.DATASET_e.TABLE_f",
                        Line={
                            MapKey.LINE_NUMBER: 5,
                            MapKey.LINE_STRING: "    PROJECT_d.DATASET_e.TABLE_f",
                        },
                    ),
                ],
            ),
            (
                "tests/sql/query/contains_str_from.sql",
                [
                    UpstairTableReference(
                        TableName="test.cte",
                        Line={
                            MapKey.LINE_NUMBER: 5,
                            MapKey.LINE_STRING: "        test.cte",
                        },
                    ),
                    UpstairTableReference(
                        TableName="test.main",
                        Line={
                            MapKey.LINE_NUMBER: 11,
                            MapKey.LINE_STRING: "    test.main",
                        },
                    ),
                    UpstairTableReference(
                        TableName="test.sub",
                        Line={
                            MapKey.LINE_NUMBER: 13,
                            MapKey.LINE_STRING: "    test.sub ON",
                        },
                    ),
                ],
            ),
            (
                "tests/sql/query/extract_date_from_timestamp.sql",
                [],
            ),
        ],
        ids=[
            "tests/sql/query/cte_one_line.sql",
            "tests/sql/query/cte_multi_line.sql",
            "tests/sql/query/nested_join.sql",
            "tests/sql/query/union_same_table.sql",
            "tests/sql/query/cte_multi_tables_01.sql",
            "tests/sql/query/cte_multi_tables_02.sql",
            "tests/sql/query/backtick_each_elements.sql",
            "tests/sql/query/backtick_whole_element.sql",
            "tests/sql/query/google_bigquery_unnest_in_exists.sql",
            "tests/sql/query/include_from_and_to_in_column_names.sql",
            "tests/sql/query/extract_date_from_timestamp.sql",
        ],
    )
    def test_detect_upstairs_attributes(self, file, expected):
        with open(file) as f:
            query_str = f.read()
        query = Query(query_str=query_str)
        actual = []
        for result in query.detect_upstair_table_reference():
            actual.append(result)
        assert actual == expected

    @pytest.mark.parametrize(
        ("table", "default_table_prefix", "expected"),
        [
            ("DATASET_d.TABLE_d", "PROJECT_A", "PROJECT_A.DATASET_d.TABLE_d"),
            ("TABLE_d", "PROJECT_A.DATASET_A", "PROJECT_A.DATASET_A.TABLE_d"),
            ("PROJECT_d.DATASET_d.TABLE_d", "PROJECT_A", "PROJECT_d.DATASET_d.TABLE_d"),
        ],
        ids=[
            "DATASET_d.TABLE_d",
            "TABLE_d",
            "PROJECT_d.DATASET_d.TABLE_d",
        ],
    )
    def test_solve_table_prefix(
        self,
        table: str,
        default_table_prefix: str,
        expected: str,
    ):
        actual = solve_table_prefix(
            table=table, default_table_prefix=default_table_prefix
        )
        assert actual == expected

import pytest

from src.stairlight.query import Query, UpstairsReference, solve_table_prefix


class TestSuccess:
    def test_parse_new_line(self):
        query_str = (
            "SELECT * FROM PROJECT_X.DATASET_X.TABLE_X \n"
            "INNER JOIN PROJECT_X.DATASET_X.TABLE_Y USING(ID)"
        )
        query = Query(query_str=query_str)
        results: list[UpstairsReference] = []
        for result in query.detect_upstairs_reference():
            results.append(result)
        assert results == [
            UpstairsReference(
                TableName="PROJECT_X.DATASET_X.TABLE_X",
                LineNumber=1,
                LineString="SELECT * FROM PROJECT_X.DATASET_X.TABLE_X ",
            ),
            UpstairsReference(
                TableName="PROJECT_X.DATASET_X.TABLE_Y",
                LineNumber=2,
                LineString=("INNER JOIN PROJECT_X.DATASET_X.TABLE_Y USING(ID)"),
            ),
        ]

    @pytest.mark.parametrize(
        ("file", "expected"),
        [
            (
                "tests/sql/query/cte_one_line.sql",
                [
                    UpstairsReference(
                        TableName="PROJECT_B.DATASET_B.TABLE_B",
                        LineNumber=1,
                        LineString=(
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
                    ),
                    UpstairsReference(
                        TableName="PROJECT_C.DATASET_C.TABLE_C",
                        LineNumber=1,
                        LineString=(
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
                    ),
                    UpstairsReference(
                        TableName="PROJECT_d.DATASET_d.TABLE_d",
                        LineNumber=1,
                        LineString=(
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
                    ),
                ],
            ),
            (
                "tests/sql/query/cte_multi_line.sql",
                [
                    UpstairsReference(
                        TableName="PROJECT_B.DATASET_B.TABLE_B",
                        LineNumber=25,
                        LineString="    PROJECT_B.DATASET_B.TABLE_B AS b",
                    ),
                    UpstairsReference(
                        TableName="PROJECT_C.DATASET_C.TABLE_C",
                        LineNumber=7,
                        LineString="        PROJECT_C.DATASET_C.TABLE_C",
                    ),
                    UpstairsReference(
                        TableName="PROJECT_d.DATASET_d.TABLE_d",
                        LineNumber=17,
                        LineString="        PROJECT_d.DATASET_d.TABLE_d",
                    ),
                ],
            ),
            (
                "tests/sql/query/nested_join.sql",
                [
                    UpstairsReference(
                        TableName="PROJECT_B.DATASET_B.TABLE_B",
                        LineNumber=4,
                        LineString="    PROJECT_B.DATASET_B.TABLE_B AS b",
                    ),
                    UpstairsReference(
                        TableName="PROJECT_C.DATASET_C.TABLE_C",
                        LineNumber=10,
                        LineString="            PROJECT_C.DATASET_C.TABLE_C",
                    ),
                    UpstairsReference(
                        TableName="PROJECT_d.DATASET_d.TABLE_d",
                        LineNumber=20,
                        LineString=("            PROJECT_d.DATASET_d.TABLE_d d"),
                    ),
                    UpstairsReference(
                        TableName="PROJECT_e.DATASET_e.TABLE_e",
                        LineNumber=21,
                        LineString=(
                            "            LEFT OUTER JOIN PROJECT_e.DATASET_e.TABLE_e"
                        ),
                    ),
                ],
            ),
            (
                "tests/sql/query/union_same_table.sql",
                [
                    UpstairsReference(
                        TableName=("test_project.beam_streaming.taxirides_realtime"),
                        LineNumber=6,
                        LineString=(
                            "    test_project.beam_streaming.taxirides_realtime"
                        ),
                    ),
                    UpstairsReference(
                        TableName=("test_project.beam_streaming.taxirides_realtime"),
                        LineNumber=15,
                        LineString=(
                            "    test_project.beam_streaming.taxirides_realtime"
                        ),
                    ),
                ],
            ),
            (
                "tests/sql/query/cte_multi_tables_01.sql",
                [
                    UpstairsReference(
                        TableName="project.dataset.table_test_A",
                        LineNumber=6,
                        LineString="		project.dataset.table_test_A",
                    ),
                    UpstairsReference(
                        TableName="project.dataset.table_test_B",
                        LineNumber=13,
                        LineString="		project.dataset.table_test_B AS test_B",
                    ),
                    UpstairsReference(
                        TableName="project.dataset.table_test_C",
                        LineNumber=19,
                        LineString=("FROM project.dataset.table_test_C AS test_C"),
                    ),
                ],
            ),
            (
                "tests/sql/query/cte_multi_tables_02.sql",
                [
                    UpstairsReference(
                        TableName="project.dataset.table_test_A",
                        LineNumber=6,
                        LineString=("		project.dataset.table_test_A -- table_test_B"),
                    ),
                    UpstairsReference(
                        TableName="project.dataset.table_test_B",
                        LineNumber=12,
                        LineString="		project.dataset.table_test_B AS test_B",
                    ),
                    UpstairsReference(
                        TableName="project.dataset.table_test_C",
                        LineNumber=19,
                        LineString="		project.dataset.table_test_C AS test_C",
                    ),
                    UpstairsReference(
                        TableName="project.dataset.table_test_D",
                        LineNumber=26,
                        LineString=("FROM project.dataset.table_test_D AS test_D"),
                    ),
                ],
            ),
            (
                "tests/sql/query/backtick_each_elements.sql",
                [
                    UpstairsReference(
                        TableName="dummy.dummy.my_first_dbt_model",
                        LineNumber=4,
                        LineString=("from `dummy`.`dummy`.`my_first_dbt_model`"),
                    ),
                ],
            ),
            (
                "tests/sql/query/backtick_whole_element.sql",
                [
                    UpstairsReference(
                        TableName="dummy.dummy.my_first_dbt_model",
                        LineNumber=4,
                        LineString="from `dummy.dummy.my_first_dbt_model`",
                    ),
                ],
            ),
            (
                "tests/sql/query/google_bigquery_unnest_in_exists.sql",
                [
                    UpstairsReference(
                        TableName="PROJECT_d.DATASET_e.TABLE_f",
                        LineNumber=5,
                        LineString="    PROJECT_d.DATASET_e.TABLE_f",
                    ),
                ],
            ),
            (
                "tests/sql/query/contains_str_from.sql",
                [
                    UpstairsReference(
                        TableName="test.cte",
                        LineNumber=5,
                        LineString="        test.cte",
                    ),
                    UpstairsReference(
                        TableName="test.main",
                        LineNumber=11,
                        LineString="    test.main",
                    ),
                    UpstairsReference(
                        TableName="test.sub",
                        LineNumber=13,
                        LineString="    test.sub ON",
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
        for result in query.detect_upstairs_reference():
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

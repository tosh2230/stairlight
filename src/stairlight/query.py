import re
from typing import Iterator

from .key import MapKey


class Query:
    """SQL query"""

    def __init__(self, query_str: str = None, default_table_prefix: str = None) -> None:
        """SQL query

        Args:
            query_str (str, optional): SQL query string. Defaults to None.
            default_table_prefix (str, optional):
                If project or dataset that configured table have are omitted,
                it will be complement this prefix. Defaults to None.
        """
        self.query_str = query_str
        self.default_table_prefix = default_table_prefix

    def detect_upstairs_attributes(self) -> Iterator[dict]:
        """Parse a SQL query string and detect upstream table attributes

        Yields:
            Iterator[dict]: upstream table attributes
        """
        upstairs_tables = self.parse_and_get_upstairs_tables()

        for upstairs_table in upstairs_tables:
            line_indexes = [
                i
                for i, line in enumerate(self.query_str.splitlines())
                if upstairs_table in line
                and "--" not in line.split(upstairs_table)[0]  # exclude comments
            ]

            for line_index in line_indexes:
                if self.default_table_prefix:
                    table_name = solve_table_prefix(
                        table=upstairs_table,
                        default_table_prefix=self.default_table_prefix,
                    )
                else:
                    table_name = upstairs_table
                yield {
                    MapKey.TABLE_NAME: table_name.replace("`", ""),  # for BigQuery
                    MapKey.LINE_NUMBER: line_index + 1,
                    MapKey.LINE_STRING: self.query_str.splitlines()[line_index],
                }

    def parse_and_get_upstairs_tables(self) -> "list[str]":
        """Parse query and get upstairs tables

        Returns:
            set: upstairs table set
        """
        # Get Common-Table-Expressions(CTE) from query string
        cte_pattern = r"(?:with|,)\s*(\w+)\s+as\s*"
        cte_alias: list[str] = re.findall(cte_pattern, self.query_str, re.IGNORECASE)

        # Search a boundary line number that main query starts
        boundary_num: int = 0
        main_pattern = r"\)[;\s]*select" if any(cte_alias) else r"select"
        main_search_result = re.search(main_pattern, self.query_str, re.IGNORECASE)
        if main_search_result:
            boundary_num = main_search_result.start()

        # Split query to main and CTE
        query_group = {}
        query_group["main"] = self.query_str[boundary_num:].strip()
        query_group["cte"] = self.query_str[:boundary_num].strip()

        # Exclude table alias from main query
        table_pattern = r"(?:from|join)\s+([`.\-\w]+)"
        main_tables_with_alias: list[str] = re.findall(
            table_pattern, query_group["main"], re.IGNORECASE
        )
        main_tables = [
            table for table in main_tables_with_alias if table not in cte_alias
        ]

        # Exclude table alias from CTEs
        cte_tables_with_alias: list[str] = re.findall(
            table_pattern, query_group["cte"], re.IGNORECASE
        )
        cte_tables = [
            cte_table
            for cte_table in cte_tables_with_alias
            if cte_table not in cte_alias
        ]

        return sorted(set(main_tables + cte_tables))


def solve_table_prefix(table: str, default_table_prefix: str) -> str:
    """Solve table name prefix

    Args:
        table (str): Table name
        default_table_prefix (str):
            If project or dataset that configured table have are omitted,
            it will be complement this prefix. Defaults to None.

    Returns:
        str: Solved table name
    """
    solved_name = table
    if table.count(".") <= 1:
        solved_name = default_table_prefix + "." + solved_name
    return solved_name

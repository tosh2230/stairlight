import re
from typing import Iterator


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

    def parse_upstairs(self) -> Iterator[dict]:
        """Parse a SQL query string and get upstream table attributes

        Yields:
            Iterator[dict]: upstream table attributes
        """
        # Check the query has cte or not
        cte_pattern = r"(?:with|,)\s*(\w+)\s+as\s*"
        ctes = re.findall(cte_pattern, self.query_str, re.IGNORECASE)

        # Check a boundary that main query starts
        boundary_num = 0
        main_pattern = r"\)[;\s]*select" if any(ctes) else r"select"
        main_search_result = re.search(main_pattern, self.query_str, re.IGNORECASE)
        if main_search_result:
            boundary_num = main_search_result.start()

        # Split the query to 'main' and 'cte'
        query_group = {}
        query_group["main"] = self.query_str[boundary_num:].strip()
        query_group["cte"] = self.query_str[:boundary_num].strip()

        table_pattern = r"(?:from|join)\s+([`.\-\w]+)"
        main_tables_with_cte_alias = re.findall(
            table_pattern, query_group["main"], re.IGNORECASE
        )

        # Exclude cte table alias from main tables
        main_tables = [
            table for table in main_tables_with_cte_alias if table not in ctes
        ]
        cte_tables = re.findall(table_pattern, query_group["cte"], re.IGNORECASE)
        upstairs_tables = sorted(set(main_tables + cte_tables))

        for upstairs_table in upstairs_tables:
            line_indexes = [
                i
                for i, line in enumerate(self.query_str.splitlines())
                if upstairs_table in line
            ]

            for line_index in line_indexes:
                yield {
                    "table_name": solve_table_prefix(
                        upstairs_table, self.default_table_prefix
                    ),
                    "line": line_index + 1,
                    "line_str": self.query_str.splitlines()[line_index],
                }


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

import os
import re

from jinja2 import Environment, FileSystemLoader

from stairlight.template import TYPES


class Query:
    def __init__(self, query_str: str = None):
        self.query_str = query_str

    @classmethod
    def render(cls, type, template_file, params):
        if type == TYPES["FS"]:
            query_str = cls._render_fs(template_file, params)
        elif type.casefold() == TYPES["GCS"]:
            pass
        elif type.casefold() == TYPES["S3"]:
            pass
        return cls(query_str=query_str)

    @staticmethod
    def _render_fs(template_file, params):
        env = Environment(loader=FileSystemLoader(os.path.dirname(template_file)))
        template = env.get_template(os.path.basename(template_file))
        return template.render(params=params)

    def parse(self):
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
        tables = [table for table in main_tables_with_cte_alias if table not in ctes]

        cte_tables = re.findall(table_pattern, query_group["cte"], re.IGNORECASE)
        tables.extend(cte_tables)

        for table in tables:
            line = [
                i for i, line in enumerate(self.query_str.splitlines()) if table in line
            ][0]

            yield {
                "table_name": table,
                "line": line + 1,
                "line_str": self.query_str.splitlines()[line],
            }

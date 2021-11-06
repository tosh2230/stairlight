import os
import re

from jinja2 import Environment, FileSystemLoader, BaseLoader
from google.cloud import storage

from stairlight.template import SourceType, SQLTemplate


class Query:
    def __init__(self, query_str: str = None):
        self.query_str = query_str

    @classmethod
    def render(cls, sql_template: SQLTemplate, params: dict):
        query_str = ""
        if sql_template.source_type == SourceType.FS:
            query_str = cls.render_fs(sql_template, params)
        elif sql_template.source_type == SourceType.GCS:
            query_str = cls.render_gcs(sql_template, params)
        elif sql_template.source_type == SourceType.S3:
            pass
        return cls(query_str=query_str)

    @staticmethod
    def render_fs(sql_template: SQLTemplate, params: dict):
        env = Environment(
            loader=FileSystemLoader(os.path.dirname(sql_template.file_path))
        )
        jinja_template = env.get_template(os.path.basename(sql_template.file_path))
        return jinja_template.render(params=params)

    @staticmethod
    def render_gcs(sql_template: SQLTemplate, params: dict):
        client = storage.Client(credentials=None, project=sql_template.project)
        bucket = client.get_bucket(sql_template.bucket)
        blob = bucket.blob(sql_template.file_path)
        template_str = blob.download_as_bytes().decode("utf-8")
        jinja_template = Environment(loader=BaseLoader()).from_string(template_str)
        return jinja_template.render(params=params)

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

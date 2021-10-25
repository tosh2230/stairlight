import pathlib
import re

def search_files(target_dir: str, condition: str):
    path_obj = pathlib.Path(target_dir)
    return [str(p) for p in path_obj.glob(condition)]

class QueryParser():
    def __init__(self, query_str: str=None):
        self.query_str = query_str

    @classmethod
    def read_file(cls, sql_path):
        with open(sql_path) as f:
            query_str = f.read()
        return cls(query_str=query_str)

    def parse_query(self):
        # Check the query has cte
        cte_pattern = r'(?:with|,)\s*(\w+)\s+as\s*'
        ctes = re.findall(cte_pattern, self.query_str, re.IGNORECASE)

        # Check a row number that main query starts
        main_pattern = r'\)[;\s]*select' if any(ctes) else r'select'
        main_row_num = re.search(main_pattern, self.query_str, re.IGNORECASE).span()[0]

        # Separate the query to 'main' and 'cte'
        query_group = {}
        query_group['main'] = self.query_str[main_row_num:].strip()
        query_group['cte'] = self.query_str[:main_row_num].strip()

        table_pattern = r'(?:from|join)\s+([`.\-\w]+)'
        main_tables_with_cte_alias = re.findall(table_pattern, query_group['main'], re.IGNORECASE)
        main_tables = [table for table in main_tables_with_cte_alias if table not in ctes]
        cte_tables = re.findall(table_pattern, query_group['cte'], re.IGNORECASE)

        return {
            'main': main_tables,
            'cte': cte_tables,
        }

import pathlib
import re

def search_files(target_dir: str, condition: str):
    path_obj = pathlib.Path(target_dir)
    return [str(p) for p in path_obj.glob(condition)]

class QueryParser():
    def __init__(self, sql_path: str=None, query_str: str=None):
        self.sql_path = sql_path
        self.query_str = query_str

    @classmethod
    def read_file(cls, sql_path):
        with open(sql_path) as f:
            query_str = f.read()
        return cls(sql_path=sql_path, query_str=query_str)

    def parse_query(self):
        # Check the query has cte or not
        cte_pattern = r'(?:with|,)\s*(\w+)\s+as\s*'
        ctes = re.findall(cte_pattern, self.query_str, re.IGNORECASE)

        # Check a row number that main query starts
        main_row_num = 0
        main_pattern = r'\)[;\s]*select' if any(ctes) else r'select'
        main_search_result = re.search(main_pattern, self.query_str, re.IGNORECASE)
        if main_search_result:
            main_row_num = main_search_result.start()

        # Split the query to 'main' and 'cte'
        query_group = {}
        query_group['main'] = self.query_str[main_row_num:].strip()
        query_group['cte'] = self.query_str[:main_row_num].strip()

        table_pattern = r'(?:from|join)\s+([`.\-\w]+)'
        main_tables_with_cte_alias = re.findall(table_pattern, query_group['main'], re.IGNORECASE)
        tables = [table for table in main_tables_with_cte_alias if table not in ctes]
        cte_tables = re.findall(table_pattern, query_group['cte'], re.IGNORECASE)
        tables.extend(cte_tables)

        ref_tables = []
        for table in tables:
            line = 0
            if self.sql_path:
                line = [i for i, line in enumerate(self.query_str.splitlines()) if table in line][0]
            ref_tables.append({
                'table_name': table,
                'line': line + 1,
                'line_str': self.query_str.splitlines()[line],
            })

        return ref_tables

    def map_tables(self, map: dict, table_name: str, ref_tables: list):
        if not table_name in map:
            map[table_name] = {}

        for ref_table in ref_tables:
            ref_table_name = ref_table['table_name']
            map[table_name][ref_table_name] = {
                'file': self.sql_path,
                'line': ref_table['line'],
                'line_str': ref_table['line_str']
            }
        return map

import pathlib
import re

def search_files(target_dir: str, condition: str):
    path_obj = pathlib.Path(target_dir)
    return [str(p) for p in path_obj.glob(condition)]

class QueryParser():
    def __init__(self, sql_path):
        self.sql_path = sql_path
        self.__read_query_file()

    def __read_query_file(self):
        with open(self.sql_path) as f:
            query = f.read()

        # Check the query has cte
        cte_pattern = r'(?:with|,)\s*(\w+)\s+as\s*'
        self.__ctes = re.findall(cte_pattern, query, re.IGNORECASE)

        # Check row number that main query starts
        self.__query_group = {}
        main_pattern = r'\)[;\s]*select' if any(self.__ctes) else r'select'
        main_row_num = re.search(main_pattern, query, re.IGNORECASE).span()[0]

        # Separate the query to 'main' and 'cte'
        self.__query_group['main'] = query[main_row_num:].strip()
        self.__query_group['cte'] = query[:main_row_num].strip()

    def detect_tables(self):
        table_pattern = r'(?:from|join)\s+([`.\-\w]+)'
        main_tables_with_cte_alias = re.findall(table_pattern, self.__query_group['main'], re.IGNORECASE)
        main_tables = [table for table in main_tables_with_cte_alias if table not in self.__ctes]
        cte_tables = re.findall(table_pattern, self.__query_group['cte'], re.IGNORECASE)
        return {
            'main': main_tables,
            'cte': cte_tables,
        }

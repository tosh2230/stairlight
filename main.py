import os
from pprint import pprint

from msby import query

if __name__ == "__main__":
    target_dir = str(os.path.dirname(__file__))
    condition = '**/*.sql'
    sql_files = query.search_files(target_dir, condition)

    results = []
    for sql_file in sql_files:
        parser = query.QueryParser(sql_file)
        tables = parser.detect_tables()
        results.append({
            'sql_file': sql_file,
            'tables': tables,
        })
    pprint(results)

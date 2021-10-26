import os
from pprint import pprint

from msby import query

if __name__ == "__main__":
    target_dir = str(os.path.dirname(__file__))
    condition = '**/*.sql'
    sql_files = query.search_files(target_dir, condition)

    results = []
    for sql_file in sql_files:
        parser = query.QueryParser.read_file(sql_path=sql_file)
        ref_tables = parser.parse_query()
        results.append({
            'sql_file': sql_file,
            'ref_tables': ref_tables,
        })
        map = {}
        table_name = 'PROJECT_B.DATASET_B.TABLE_A'
        parser.map_tables(map=map, table_name=table_name, ref_tables=ref_tables)
    pprint(results)

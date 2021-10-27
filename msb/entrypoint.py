import pathlib
from msb.query_parser import QueryParser

class Msb():
    def __init__(self, target_dir: str, condition: str):
        self.target_dir = target_dir
        self.condition = condition
        self.table_name = self.get_table_name()
        self.__nodes = {}

    def search_nodes(self):
        for sql_file in self.search_files():
            parser = QueryParser.read_file(sql_path=sql_file)
            self.__map_nodes(sql_path=sql_file, ref_tables=parser.parse_query())
        return self.__nodes

    def get_table_name(self):
        return 'aaa'

    def search_files(self):
        path_obj = pathlib.Path(self.target_dir)
        return [str(p) for p in path_obj.glob(self.condition)]

    def __map_nodes(self, sql_path: str, ref_tables: list):
        if not self.table_name in self.__nodes:
            self.__nodes[self.table_name] = {}

        for ref_table in ref_tables:
            ref_table_name = ref_table['table_name']
            self.__nodes[self.table_name][ref_table_name] = {
                'file': sql_path,
                'line': ref_table['line'],
                'line_str': ref_table['line_str']
            }

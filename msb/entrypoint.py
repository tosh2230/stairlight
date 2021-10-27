import os
import pathlib
import yaml

from msb.query_parser import QueryParser

class Msb():
    def __init__(self, config_file: str, target_dir: str, condition: str):
        self.target_dir = target_dir
        self.condition = condition
        self.config = self.read_config(config_file)
        self.__nodes = {}

    @staticmethod
    def read_config(config_file):
        config = {}
        if config_file and config_file.endswith(('.yml', '.yaml')) :
            with open(config_file) as file:
                config = yaml.load(file, Loader=yaml.SafeLoader)
        return config

    def search_nodes(self):
        for sql_file in self.search_files():
            if self.check_exclude_list(sql_file):
                parser = QueryParser.read_file(sql_path=sql_file)
                self.map_nodes(sql_path=sql_file, ref_tables=parser.parse_query())
        return self.__nodes

    def search_files(self):
        path_obj = pathlib.Path(self.target_dir)
        for p in path_obj.glob(self.condition):
            yield str(p)

    def check_exclude_list(self, sql_file):
        result = True
        for exclude in self.config.get('exclude'):
            if sql_file.endswith(exclude):
                result = False
        return result

    def map_nodes(self, sql_path: str, ref_tables: list):
        table_name = self.get_table_name(sql_path=sql_path)
        if not table_name in self.__nodes:
            self.__nodes[table_name] = {}

        for ref_table in ref_tables:
            ref_table_name = ref_table['table_name']
            self.__nodes[table_name][ref_table_name] = {
                'file': sql_path,
                'line': ref_table['line'],
                'line_str': ref_table['line_str']
            }

    def get_table_name(self, sql_path):
        table_name = ''
        mapping = self.config.get('mapping')
        for pair in mapping:
            if sql_path.endswith(pair['sql_file']):
                table_name = pair['table_name']
                break
        return table_name

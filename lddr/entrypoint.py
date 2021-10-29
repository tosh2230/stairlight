import pathlib
import yaml

from lddr.query_parser import QueryParser

class Ladder():
    def __init__(self, config_file: str, target_dir: str, condition: str):
        self.target_dir = target_dir
        self.condition = condition
        self.__config = self.read_config(config_file)
        self._maps = {}
        self.create_maps()

    @property
    def maps(self):
        return self._maps

    @staticmethod
    def read_config(config_file):
        config = {}
        if config_file and config_file.endswith(('.yml', '.yaml')) :
            with open(config_file) as file:
                config = yaml.load(file, Loader=yaml.SafeLoader)
        return config

    def create_maps(self):
        for sql_file in self.search_files():
            if self.check_exclude_list(sql_file):
                parser = QueryParser.read_file(sql_path=sql_file)
                self.map_tables(sql_path=sql_file, ref_tables=parser.parse_query())

    def search_files(self):
        path_obj = pathlib.Path(self.target_dir)
        for p in path_obj.glob(self.condition):
            yield str(p)

    def check_exclude_list(self, sql_file):
        result = True
        for exclude in self.__config.get('exclude'):
            if sql_file.endswith(exclude):
                result = False
                break
        return result

    def map_tables(self, sql_path: str, ref_tables: list):
        table_name = self.get_table_name(sql_path=sql_path)
        if not table_name in self._maps:
            self._maps[table_name] = {}

        for ref_table in ref_tables:
            ref_table_name = ref_table['table_name']
            self._maps[table_name][ref_table_name] = {
                'file': sql_path,
                'line': ref_table['line'],
                'line_str': ref_table['line_str']
            }

    def get_table_name(self, sql_path):
        table_name = ''
        mapping = self.__config.get('mapping')
        for pair in mapping:
            if sql_path.endswith(pair['sql_file']):
                table_name = pair['table_name']
                break
        return table_name

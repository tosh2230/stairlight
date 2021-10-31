import pathlib

from stairlight.config import SQL_CONFIG, read_config


class Template:
    def __init__(self):
        self._sql_config = read_config(SQL_CONFIG)

    def search(self):
        for source in self._sql_config.get("sources"):
            type = source.get("type")
            if type.casefold() in ["local", "fs"]:
                yield from self.search_fs(source)
            if type.casefold() in ["gcs", "gs"]:
                continue
            if type.casefold() == "s3":
                continue

    def search_fs(self, source):
        path_obj = pathlib.Path(source.get("path"))
        for p in path_obj.glob(source.get("pattern")):
            if self.is_excluded(str(p)):
                yield str(p)
            else:
                continue

    def is_excluded(self, sql_file):
        result = True
        for exclude_file in self._sql_config.get("exclude"):
            if sql_file.endswith(exclude_file):
                result = False
                break
        return result

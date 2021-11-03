import json
from logging import getLogger

import stairlight.config as config
from stairlight.map import Map

logger = getLogger(__name__)


class StairLight:
    def __init__(self, config_path="./config/", cached=False):
        config_reader = config.Reader(path=config_path)
        self.map_config = config_reader.read(config.MAP_CONFIG)
        self.strl_config = config_reader.read(config.STRL_CONFIG)
        self._maps = {}
        self._undefined_files = []

        if cached:
            pass
        else:
            dependency_map = Map(
                map_config=self.map_config, strl_config=self.strl_config
            )
            dependency_map.create()
            self._maps = dependency_map.maps
            self._undefined_files = dependency_map.undefined_files

    @property
    def maps(self):
        return self._maps

    @property
    def undefined_files(self):
        return self._undefined_files

    def all(self):
        logger.debug(json.dumps(self.maps, indent=2))
        return self.maps

    def up(self, table_name, recursive=False):
        result = self._maps.get(table_name)
        response = {table_name: {}}
        if result and recursive:
            for upstream_table_name in result.keys():
                upstream_result = self.up(
                    table_name=upstream_table_name,
                    recursive=recursive,
                )

                if not upstream_result.get(upstream_table_name):
                    continue
                result[upstream_table_name] = {
                    **result[upstream_table_name],
                    **upstream_result[upstream_table_name],
                }

        response[table_name]["upstream"] = result
        logger.debug(json.dumps(response, indent=2))
        return response

    def down(self, table_name, recursive=False):
        result = {}
        response = {table_name: {}}
        for key in [k for k, v in self._maps.items() if v.get(table_name)]:
            result[key] = self._maps[key][table_name]

        if result and recursive:
            for downstream_table_name in result.keys():
                downstream_result = self.down(
                    table_name=downstream_table_name,
                    recursive=recursive,
                )
                if not downstream_result.get(downstream_table_name):
                    continue
                result[downstream_table_name] = {
                    **result[downstream_table_name],
                    **downstream_result[downstream_table_name],
                }

        if result:
            response[table_name]["downstream"] = result
        else:
            response[table_name]["downstream"] = None
        logger.debug(json.dumps(response, indent=2))
        return response

    def make_config(self):
        if self._undefined_files:
            return
        config.make_template(self._undefined_files)
        logger.info("Undefined files are detected!: " + str(self._undefined_files))

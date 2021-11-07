import enum
import json
from logging import getLogger

import stairlight.config as config
from stairlight.map import Map

logger = getLogger(__name__)


class ResponseType(enum.Enum):
    TABLE = "table"
    FILE = "file"

    def __str__(self):
        return self.name


class StairLight:
    def __init__(self, config_path="./config/", cached=False):
        self.configurator = config.Configurator(path=config_path)
        self.map_config = self.configurator.read(config.MAP_CONFIG)
        self.strl_config = self.configurator.read(config.STRL_CONFIG)
        self._maps = {}
        self._undefined_files = []

        if cached:
            pass
        else:
            dependency_map = Map(
                strl_config=self.strl_config,
                map_config=self.map_config,
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

    def up(
        self,
        table_name,
        recursive=False,
        verbose=False,
        response_type=ResponseType.TABLE.value,
    ):
        direction = "upstream"
        if verbose:
            return self.search_verbose(
                table_name=table_name, direction=direction, recursive=recursive
            )
        if response_type in [type.value for type in ResponseType]:
            return self.search_plain(
                table_name=table_name,
                direction=direction,
                recursive=recursive,
                response_type=response_type,
            )
        return None

    def down(
        self,
        table_name,
        recursive=False,
        verbose=False,
        response_type=ResponseType.TABLE.value,
    ):
        direction = "downstream"
        if verbose:
            return self.search_verbose(
                table_name=table_name, direction=direction, recursive=recursive
            )
        if response_type in [type.value for type in ResponseType]:
            return self.search_plain(
                table_name=table_name,
                direction=direction,
                recursive=recursive,
                response_type=response_type,
            )
        return None

    def search_verbose(self, table_name, direction, recursive=False):
        current_map = self.get_current_map(table_name, direction)
        response = {table_name: {}}

        if not current_map:
            return response
        if recursive:
            for next_table_name in current_map.keys():
                next_response = self.search_verbose(
                    table_name=next_table_name,
                    direction=direction,
                    recursive=recursive,
                )

                if not next_response.get(next_table_name):
                    continue
                current_map[next_table_name] = {
                    **current_map[next_table_name],
                    **next_response[next_table_name],
                }

        response[table_name][direction] = current_map
        logger.debug(json.dumps(response, indent=2))
        return response

    def search_plain(self, table_name, direction, recursive, response_type):
        current_map = self.get_current_map(table_name, direction)
        response = []

        if not current_map:
            return response
        for next_table_name in current_map.keys():
            if recursive:
                next_response = self.search_plain(
                    table_name=next_table_name,
                    direction=direction,
                    recursive=recursive,
                    response_type=response_type,
                )
                response = response + next_response

            if response_type == ResponseType.TABLE.value:
                response.append(next_table_name)
            elif response_type == ResponseType.FILE.value:
                response.append(current_map[next_table_name].get("file"))
            logger.debug(json.dumps(response, indent=2))

        return sorted(list(set(response)))

    def get_current_map(self, table_name, direction):
        current_map = {}
        if direction == "upstream":
            current_map = self._maps.get(table_name)
        elif direction == "downstream":
            for key in [k for k, v in self._maps.items() if v.get(table_name)]:
                current_map[key] = self._maps[key][table_name]
        return current_map

    def make_config(self):
        if self._undefined_files:
            return
        self.configurator.make_template(undefined_files=self._undefined_files)
        logger.info("Undefined files are detected!: " + str(self._undefined_files))

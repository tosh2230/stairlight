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


class SearchDirection(enum.Enum):
    UP = "upstream"
    DOWN = "downstream"

    def __str__(self):
        return self.name


class StairLight:
    def __init__(self, config_path="./config/"):
        self.configurator = config.Configurator(path=config_path)
        self.map_config = self.configurator.read(config.MAP_CONFIG)
        self.strl_config = self.configurator.read(config.STRL_CONFIG)
        self._maps = {}
        self._undefined_files = []

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
        return self.maps

    def up(
        self,
        table_name,
        recursive=False,
        verbose=False,
        response_type=ResponseType.TABLE.value,
    ):
        return self.search(
            table_name=table_name,
            recursive=recursive,
            verbose=verbose,
            response_type=response_type,
            direction=SearchDirection.UP,
        )

    def down(
        self,
        table_name,
        recursive=False,
        verbose=False,
        response_type=ResponseType.TABLE.value,
    ):
        return self.search(
            table_name=table_name,
            recursive=recursive,
            verbose=verbose,
            response_type=response_type,
            direction=SearchDirection.DOWN,
        )

    def search(
        self,
        table_name,
        recursive,
        verbose,
        response_type,
        direction,
    ):
        if verbose:
            return self.search_verbose(
                table_name=table_name, recursive=recursive, direction=direction
            )
        if response_type in [type.value for type in ResponseType]:
            return self.search_plain(
                table_name=table_name,
                recursive=recursive,
                response_type=response_type,
                direction=direction,
            )
        return None

    def search_verbose(self, table_name, recursive, direction, ref_tables=[]):
        relative_map = self.get_relative_map(table_name, direction)
        response = {table_name: {}}
        ref_tables.append(table_name)

        if not relative_map:
            return response
        if recursive:
            for next_table_name in relative_map.keys():
                if self.has_circular_reference(
                    table_name=table_name,
                    next_table_name=next_table_name,
                    ref_tables=ref_tables,
                ):
                    continue

                next_response = self.search_verbose(
                    table_name=next_table_name,
                    direction=direction,
                    recursive=recursive,
                )

                if not next_response.get(next_table_name):
                    continue
                relative_map[next_table_name] = {
                    **relative_map[next_table_name],
                    **next_response[next_table_name],
                }

        response[table_name][direction.value] = relative_map
        logger.debug(json.dumps(response, indent=2))
        return response

    def search_plain(
        self, table_name, recursive, response_type, direction, ref_tables=[]
    ):
        relative_map = self.get_relative_map(table_name, direction)
        response = []

        if not relative_map:
            return response
        for next_table_name in relative_map.keys():
            if recursive:
                ref_tables.append(next_table_name)
                if self.has_circular_reference(
                    table_name=table_name,
                    next_table_name=next_table_name,
                    ref_tables=ref_tables,
                ):
                    continue
                next_response = self.search_plain(
                    table_name=next_table_name,
                    direction=direction,
                    recursive=recursive,
                    response_type=response_type,
                    ref_tables=ref_tables,
                )
                response = response + next_response

            if response_type == ResponseType.TABLE.value:
                response.append(next_table_name)
            elif response_type == ResponseType.FILE.value:
                response.append(relative_map[next_table_name].get("uri"))
            logger.debug(json.dumps(response, indent=2))

        return sorted(list(set(response)))

    def get_relative_map(self, table_name, direction):
        relative_map = {}
        if direction == SearchDirection.UP:
            relative_map = self._maps.get(table_name)
        elif direction == SearchDirection.DOWN:
            for key in [k for k, v in self._maps.items() if v.get(table_name)]:
                relative_map[key] = self._maps[key][table_name]
        return relative_map

    def has_circular_reference(self, table_name, next_table_name, ref_tables):
        if ref_tables.count(table_name) > 2:
            details = {
                "table_name": table_name,
                "next_table_name": next_table_name,
                "ref_tables": list(set(ref_tables)),
            }
            logger.info(f"circular_reference detected!: {details}")
            return True
        return False

    def make_config(self):
        if self._undefined_files:
            return
        self.configurator.make_template(undefined_files=self._undefined_files)
        logger.info("Undefined files are detected!: " + str(self._undefined_files))

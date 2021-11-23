import enum
import json
import os
from logging import getLogger

from .config import MAP_CONFIG_PREFIX, STRL_CONFIG_PREFIX, Configurator
from .map import Map

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


class Node:
    def __init__(self, val, next=None):
        self.val = val
        self.next = None


class StairLight:
    def __init__(self, config_path=".", load_file=None, save_file=None):
        self.load_file = load_file
        self.save_file = save_file
        self._configurator = Configurator(path=config_path)
        self._mapped = {}
        self._unmapped = []
        self._strl_config = self._configurator.read(prefix=STRL_CONFIG_PREFIX)
        if self._strl_config:
            if self.load_file:
                self._load()
            else:
                self._set()
                if self.save_file:
                    self._save()

    @property
    def mapped(self):
        return self._mapped

    @property
    def unmapped(self):
        return self._unmapped

    def has_strl_config(self):
        return self._strl_config is not None

    def _set(self):
        if not self._strl_config:
            logger.warning(f"{STRL_CONFIG_PREFIX}.y(a)ml' is not found.")
            return

        map_config_prefix = MAP_CONFIG_PREFIX
        if "settings" in self._strl_config:
            settings = self._strl_config["settings"]
            if "mapping_prefix" in settings:
                map_config_prefix = settings["mapping_prefix"]
        map_config = self._configurator.read(prefix=map_config_prefix)

        dependency_map = Map(
            strl_config=self._strl_config,
            map_config=map_config,
        )
        if map_config:
            dependency_map.write()
            self._mapped = dependency_map.mapped
        else:
            dependency_map.write_blank()

        self._unmapped = dependency_map.unmapped

    def init(self):
        return self._configurator.create_stairlight_template()

    def check(self):
        if self.load_file:
            logger.warning("Load option is used, skip checking.")
            return None
        elif not self._unmapped:
            return None
        return self._configurator.create_mapping_template(unmapped=self._unmapped)

    def _save(self):
        with open(self.save_file, "w") as f:
            json.dump(self._mapped, f, indent=2)

    def _load(self):
        if not os.path.exists(self.load_file):
            logger.error(f"{self.load_file} is not found.")
            exit()
        with open(self.load_file) as f:
            self._mapped = json.load(f)

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
                table_name=table_name,
                recursive=recursive,
                direction=direction,
                searched_tables=[],
                head=True,
            )
        if response_type in [type.value for type in ResponseType]:
            return self.search_plain(
                table_name=table_name,
                recursive=recursive,
                response_type=response_type,
                direction=direction,
                searched_tables=[],
                head=True,
            )
        return None

    def search_verbose(self, table_name, recursive, direction, searched_tables, head):
        relative_map = self.get_relative_map(table_name, direction)
        response = {table_name: {}}
        if not relative_map:
            return response

        if recursive:
            for next_table_name in relative_map.keys():
                if head:
                    searched_tables = []
                    searched_tables.append(table_name)

                searched_tables.append(next_table_name)

                if is_cyclic(tables=searched_tables):
                    details = {
                        "table_name": table_name,
                        "next_table_name": next_table_name,
                        "searched_tables": searched_tables,
                    }
                    logger.warning(f"Circular reference detected!: {details}")
                    continue

                next_response = self.search_verbose(
                    table_name=next_table_name,
                    direction=direction,
                    recursive=recursive,
                    searched_tables=searched_tables,
                    head=False,
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
        self, table_name, recursive, response_type, direction, searched_tables, head
    ):
        relative_map = self.get_relative_map(table_name, direction)
        response = []
        if not relative_map:
            return response

        for next_table_name in relative_map.keys():
            if recursive:
                if head:
                    searched_tables = []
                    searched_tables.append(table_name)

                searched_tables.append(next_table_name)

                if is_cyclic(tables=searched_tables):
                    details = {
                        "table_name": table_name,
                        "next_table_name": next_table_name,
                        "searched_tables": searched_tables,
                    }
                    logger.info(f"Circular reference detected!: {details}")
                    continue

                next_response = self.search_plain(
                    table_name=next_table_name,
                    direction=direction,
                    recursive=recursive,
                    response_type=response_type,
                    searched_tables=searched_tables,
                    head=False,
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
            relative_map = self._mapped.get(table_name)
        elif direction == SearchDirection.DOWN:
            for key in [k for k, v in self._mapped.items() if v.get(table_name)]:
                relative_map[key] = self._mapped[key][table_name]
        return relative_map


def is_cyclic(tables):
    nodes = {}
    for table in tables:
        if table not in nodes.keys():
            nodes[table] = Node(table)
    for i, table in enumerate(tables):
        if not nodes[table].next and i < len(tables) - 1:
            nodes[table].next = nodes[tables[i + 1]]
    slow = fast = nodes[tables[0]]
    while fast and fast.next:
        slow = slow.next
        fast = fast.next.next
        if slow == fast:
            return True
    return False

import enum
import json
import os
from logging import getLogger
from typing import Union

from .config import MAP_CONFIG_PREFIX, STRL_CONFIG_PREFIX, Configurator
from .map import Map

logger = getLogger(__name__)


class ResponseType(enum.Enum):
    """Enum: Execution result type of up/down command"""

    TABLE: str = "table"
    FILE: str = "file"

    def __str__(self):
        return self.name


class SearchDirection(enum.Enum):
    """Enum: Search direction"""

    UP = "upstairs"
    DOWN = "downstairs"

    def __str__(self):
        return self.name


class Node:
    """A Node of singly-linked list"""

    def __init__(self, val, next=None):
        self.val = val
        self.next = None


class StairLight:
    """Table dependency detector"""

    def __init__(
        self, config_dir: str = ".", load_file: str = None, save_file: str = None
    ) -> None:
        """Table dependency detector

        Args:
            config_dir (str, optional):
                Stairlight configuration directory. Defaults to ".".
            load_file (str, optional):
                A file name of loading results if load option set. Defaults to None.
            save_file (str, optional):
                A file name of saving results if save option set. Defaults to None.
        """
        self.load_file = load_file
        self.save_file = save_file
        self._configurator = Configurator(dir=config_dir)
        self._mapped = {}
        self._unmapped = []
        self._map_config = None
        self._strl_config = self._configurator.read(prefix=STRL_CONFIG_PREFIX)
        if self._strl_config:
            if self.load_file:
                self._load()
            else:
                self._set()
                if self.save_file:
                    self._save()

    @property
    def mapped(self) -> dict:
        """Return mapped

        Returns:
            dict: Mapped results
        """
        return self._mapped

    @property
    def unmapped(self) -> list:
        """Return unmapped

        Returns:
            list: Unmapped results
        """
        return self._unmapped

    def has_strl_config(self) -> bool:
        """Exists stairlight configuration file or not

        Returns:
            bool: Exists stairlight configuration file or not
        """
        return self._strl_config is not None

    def _set(self) -> None:
        """Set config and get a dependency map"""
        if not self._strl_config:
            logger.warning(f"{STRL_CONFIG_PREFIX}.y(a)ml' is not found.")
            return

        map_config_prefix = MAP_CONFIG_PREFIX
        if "settings" in self._strl_config:
            settings = self._strl_config["settings"]
            if "mapping_prefix" in settings:
                map_config_prefix = settings["mapping_prefix"]
        self._map_config = self._configurator.read(prefix=map_config_prefix)

        dependency_map = Map(
            strl_config=self._strl_config,
            map_config=self._map_config,
        )
        if self._map_config:
            dependency_map.write()
            self._mapped = dependency_map.mapped
        else:
            dependency_map.write_blank()

        self._unmapped = dependency_map.unmapped

    def init(self, prefix: str = STRL_CONFIG_PREFIX) -> str:
        """Create Stairlight template file

        Args:
            prefix (str, optional):
                Template file prefix. Defaults to STRL_CONFIG_PREFIX.

        Returns:
            str: Template file name
        """
        return self._configurator.create_stairlight_template(prefix=prefix)

    def check(self, prefix: str = MAP_CONFIG_PREFIX) -> str:
        """Check mapped results and create a mapping template file

        Args:
            prefix (str, optional): Template file prefix. Defaults to MAP_CONFIG_PREFIX.

        Returns:
            str: Template file name
        """
        if self.load_file:
            logger.warning("Load option is used, skip checking.")
            return None
        elif not self._unmapped:
            return None
        return self._configurator.create_mapping_template(
            unmapped=self._unmapped, prefix=prefix
        )

    def _save(self) -> None:
        """Save mapped results"""
        with open(self.save_file, "w") as f:
            json.dump(self._mapped, f, indent=2)

    def _load(self) -> None:
        """Load mapped results"""
        if not os.path.exists(self.load_file):
            logger.error(f"{self.load_file} is not found.")
            exit()
        with open(self.load_file) as f:
            self._mapped = json.load(f)

    def up(
        self,
        table_name: str,
        recursive: bool = False,
        verbose: bool = False,
        response_type: str = ResponseType.TABLE.value,
    ) -> Union[list, dict]:
        """Search upstream nodes

        Args:
            table_name (str): Table name
            recursive (bool, optional): Search recursively or not. Defaults to False.
            verbose (bool, optional): Return verbose results or not. Defaults to False.
            response_type (str, optional):
                Response type. Defaults to ResponseType.TABLE.value.

        Returns:
            Union[list, dict]: [description]
        """
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
    ) -> Union[list, dict]:
        """Search downstream nodes

        Args:
            table_name (str): Table name
            recursive (bool, optional): Search recursively or not. Defaults to False.
            verbose (bool, optional): Return verbose results or not. Defaults to False.
            response_type (str, optional):
                Response type. Defaults to ResponseType.TABLE.value.

        Returns:
            Union[list, dict]: [description]
        """
        return self.search(
            table_name=table_name,
            recursive=recursive,
            verbose=verbose,
            response_type=response_type,
            direction=SearchDirection.DOWN,
        )

    def search(
        self,
        table_name: str,
        recursive: bool,
        verbose: bool,
        response_type: str,
        direction: SearchDirection,
    ) -> Union[list, dict]:
        """Search nodes

        Args:
            table_name (str): Table name
            recursive (bool): Search recursively or not
            verbose (bool): Return verbose results or not
            response_type (str): Response type value
            direction (SearchDirection): Search direction

        Returns:
            Union[list, dict]: [description]
        """
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

    def search_verbose(
        self,
        table_name: str,
        recursive: bool,
        direction: SearchDirection,
        searched_tables: list,
        head: bool,
    ) -> dict:
        """Search nodes and return verbose results

        Args:
            table_name (str): Table name
            recursive (bool): Search recursively or not
            direction (SearchDirection): Search direction
            searched_tables (list): a list of searched tables
            head (bool): Current position is head or not

        Returns:
            dict: Search results
        """
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
        self,
        table_name: str,
        recursive: bool,
        response_type: str,
        direction: SearchDirection,
        searched_tables: list,
        head: bool,
    ) -> list:
        """Search nodes and return simple results

        Args:
            table_name (str): Table name
            recursive (bool): Search recursively or not
            response_type (str): Response type value
            direction (SearchDirection): Search direction
            searched_tables (list): a list of searched tables
            head (bool): Current position is head or not

        Returns:
            list: Search results
        """
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

    def get_relative_map(self, table_name: str, direction: SearchDirection) -> dict:
        """Get a relative map for the specified direction

        Args:
            table_name (str): Table name
            direction (SearchDirection): Search direction

        Returns:
            dict: Relative map
        """
        relative_map = {}
        if direction == SearchDirection.UP:
            relative_map = self._mapped.get(table_name)
        elif direction == SearchDirection.DOWN:
            for key in [k for k, v in self._mapped.items() if v.get(table_name)]:
                relative_map[key] = self._mapped[key][table_name]
        return relative_map

    def get_tables_by_labels(self, targets: list) -> list:
        """Get tables to search by labels

        Args:
            targets (list): Target labels

        Returns:
            list: Tables to search
        """
        tables_to_search = []

        # "mapping" section in mapping.yaml
        for configurations in self._map_config.get("mapping"):
            for table_attributes in configurations.get("tables"):
                if self.is_target_found(
                    targets=targets,
                    labels=table_attributes.get("labels", {}),
                ):
                    tables_to_search.append(table_attributes["table"])

        # "metadata" section in mapping.yaml
        for table_attributes in self._map_config.get("metadata"):
            if self.is_target_found(
                targets=targets,
                labels=table_attributes.get("labels", {}),
            ):
                tables_to_search.append(table_attributes["table"])

        return tables_to_search

    @staticmethod
    def is_target_found(targets: list, labels: dict) -> bool:
        result = False
        found = 0
        for label_key, label_value in labels.items():
            for target in targets:
                target_key = target.split(":")[0]
                target_value = target.split(":")[1]
                if target_key == label_key and target_value == label_value:
                    found += 1
        if found == len(targets):
            result = True
        return result


def is_cyclic(tables: list) -> bool:
    """Floyd's cycle-finding algorithm

    Args:
        tables (list): Detected tables

    Returns:
        bool: Table dependencies are cyclic or not
    """
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

import enum
import json
from logging import getLogger
from typing import Any, Dict, List, Union

from .configurator import (
    MAPPING_CONFIG_PREFIX_DEFAULT,
    STAIRLIGHT_CONFIG_PREFIX_DEFAULT,
    Configurator,
)
from .map import Map
from .source.config import (
    MapKey,
    MappingConfig,
    StairlightConfig,
    StairlightConfigSettings,
)
from .source.controller import LoadMapController, SaveMapController

logger = getLogger(__name__)


class ResponseType(enum.Enum):
    """Enum: Execution result type of up/down command"""

    TABLE: str = "table"
    FILE: str = "file"

    def __str__(self):
        return self.name


class SearchDirection(enum.Enum):
    """Enum: Search direction"""

    UP: str = "Upstairs"
    DOWN: str = "Downstairs"

    def __str__(self):
        return self.name


class Node:
    """A Node of singly-linked list"""

    def __init__(self, val: str, next: "Node" = None):
        self.val: str = val
        self.next: "Node" = None


class StairLight:
    """Table dependency detector"""

    def __init__(
        self,
        config_dir: str = ".",
        load_files: List[str] = None,
        save_file: str = "",
    ) -> None:
        """Table dependency detector

        Args:
            config_dir (str, optional):
                Stairlight configuration directory. Defaults to ".".
            load_files (list, optional):
                file names of loading results if load option set. Defaults to None.
            save_file (str, optional):
                A file name of saving results if save option set. Defaults to None.
        """
        self.load_files = load_files
        self.save_file: str = save_file
        self._configurator = Configurator(dir=config_dir)
        self._mapped: Dict[str, Any] = {}
        self._unmapped: List[Dict[str, Any]] = []
        self._mapping_config: MappingConfig = None
        self._stairlight_config: StairlightConfig = self._configurator.read_stairlight(
            prefix=STAIRLIGHT_CONFIG_PREFIX_DEFAULT
        )

    def create_map(self) -> None:
        if not self._stairlight_config:
            return

        if self.load_files:
            self.load_map()
        else:
            self._set_config()
            self._write_map()

        if self.save_file:
            self.save_map()

    @property
    def mapped(self) -> Dict[str, Any]:
        """Return mapped

        Returns:
            dict: Mapped results
        """
        return self._mapped

    @property
    def unmapped(self) -> List[Dict[str, Any]]:
        """Return unmapped

        Returns:
            list[dict]: Unmapped results
        """
        return self._unmapped

    def has_stairlight_config(self) -> bool:
        """Exists stairlight configuration file or not

        Returns:
            bool: Exists stairlight configuration file or not
        """
        return self._stairlight_config is not None

    def _set_config(self) -> None:
        """Set config"""
        if not self._stairlight_config:
            logger.warning(f"{STAIRLIGHT_CONFIG_PREFIX_DEFAULT}.y(a)ml' is not found.")
            return

        mapping_config_prefix: str = MAPPING_CONFIG_PREFIX_DEFAULT
        if self._stairlight_config.Settings:
            settings: StairlightConfigSettings = StairlightConfigSettings(
                **self._stairlight_config.Settings
            )
            if settings.MappingPrefix:
                mapping_config_prefix = settings.MappingPrefix
        self._mapping_config = self._configurator.read_mapping(
            prefix=mapping_config_prefix
        )

    def _write_map(self) -> None:
        """write a dependency map"""
        dependency_map = Map(
            stairlight_config=self._stairlight_config,
            mapping_config=self._mapping_config,
        )

        dependency_map.write()
        if self._mapping_config:
            self._mapped = dependency_map.mapped

        self._unmapped = dependency_map.unmapped

    def init(self, prefix: str = STAIRLIGHT_CONFIG_PREFIX_DEFAULT) -> str:
        """Create Stairlight template file

        Args:
            prefix (str, optional):
                Template file prefix. Defaults to STAIRLIGHT_CONFIG_PREFIX.

        Returns:
            str: Template file name
        """
        return self._configurator.create_stairlight_file(prefix=prefix)

    def check(self, prefix: str = MAPPING_CONFIG_PREFIX_DEFAULT) -> str:
        """Check mapped results and create a mapping template file

        Args:
            prefix (str, optional):
                Template file prefix. Defaults to MAPPING_CONFIG_PREFIX.

        Returns:
            str: Template file name
        """
        if self.load_files:
            logger.warning("Load option is used, skip checking.")
            return ""
        elif not self._unmapped:
            return ""

        return self._configurator.create_mapping_file(
            unmapped=self._unmapped, prefix=prefix
        )

    def save_map(self) -> None:
        """Save mapped results"""
        save_map_controller = SaveMapController(
            save_file=self.save_file, mapped=self._mapped
        )
        save_map_controller.save()

    def load_map(self) -> None:
        """Load mapped results"""
        if not self.load_files:
            return
        for load_file in self.load_files:
            load_map_controller = LoadMapController(load_file=load_file)
            loaded_map = load_map_controller.load()

            if self._mapped:
                self._mapped = deep_merge(org=self._mapped, add=loaded_map)
            else:
                self._mapped = loaded_map

    def up(
        self,
        table_name: str,
        recursive: bool = False,
        verbose: bool = False,
        response_type: str = ResponseType.TABLE.value,
    ) -> Union[List[str], Dict[str, Any]]:
        """Search upstream nodes

        Args:
            table_name (str): Table name
            recursive (bool, optional): Search recursively or not. Defaults to False.
            verbose (bool, optional): Return verbose results or not. Defaults to False.
            response_type (str, optional):
                Response type. Defaults to ResponseType.TABLE.value.

        Returns:
            Union[list[str], dict]: Search results
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
    ) -> Union[List[str], Dict[str, Any]]:
        """Search downstream nodes

        Args:
            table_name (str): Table name
            recursive (bool, optional): Search recursively or not. Defaults to False.
            verbose (bool, optional): Return verbose results or not. Defaults to False.
            response_type (str, optional):
                Response type. Defaults to ResponseType.TABLE.value.

        Returns:
            Union[list[str], dict]: Search results
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
    ) -> Union[List[str], Dict[str, Any]]:
        """Search nodes

        Args:
            table_name (str): Table name
            recursive (bool): Search recursively or not
            verbose (bool): Return verbose results or not
            response_type (str): Response type value
            direction (SearchDirection): Search direction

        Returns:
            Union[list[str], dict]: Search results
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

        return []

    def search_verbose(
        self,
        table_name: str,
        recursive: bool,
        direction: SearchDirection,
        searched_tables: List[str],
        head: bool,
    ) -> dict:
        """Search nodes and return verbose results

        Args:
            table_name (str): Table name
            recursive (bool): Search recursively or not
            direction (SearchDirection): Search direction
            searched_tables (list[str]): a list of searched tables
            head (bool): Current position is head or not

        Returns:
            dict: Search results
        """
        relative_map = self.create_relative_map(
            table_name=table_name, direction=direction
        )
        response: Dict[str, Any] = {table_name: {}}
        if not relative_map:
            return response

        if recursive:
            for next_table_name in relative_map.keys():
                if head:
                    searched_tables = [table_name]

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
        return response

    def search_plain(
        self,
        table_name: str,
        recursive: bool,
        response_type: str,
        direction: SearchDirection,
        searched_tables: List[str],
        head: bool,
    ) -> List[str]:
        """Search nodes and return simple results

        Args:
            table_name (str): Table name
            recursive (bool): Search recursively or not
            response_type (str): Response type value
            direction (SearchDirection): Search direction
            searched_tables (list[str]): a list of searched tables
            head (bool): Current position is head or not

        Returns:
            list[str]: Search results
        """
        relative_map = self.create_relative_map(
            table_name=table_name, direction=direction
        )
        response: List[str] = []
        if not relative_map:
            return response

        next_table_name: str
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
                response.append(relative_map[next_table_name].get(MapKey.URI))
            logger.debug(json.dumps(response, indent=2))

        return sorted(list(set(response)))

    def create_relative_map(
        self, table_name: str, direction: SearchDirection
    ) -> Dict[str, Any]:
        """Create a relative map for the specified direction

        Args:
            table_name (str): Table name
            direction (SearchDirection): Search direction

        Returns:
            dict: Relative map
        """
        relative_map: Dict[str, Any] = {}
        if direction == SearchDirection.UP:
            relative_map = self._mapped.get(table_name, {})
        elif direction == SearchDirection.DOWN:
            for key in [k for k, v in self._mapped.items() if v.get(table_name)]:
                relative_map[key] = self._mapped[key][table_name]
        return relative_map

    def find_tables_by_labels(self, target_labels: List[str]) -> List[str]:
        """Find tables by labels

        Args:
            target_labels (list[str]): Target labels

        Returns:
            list[str]: Tables to search
        """
        tables_to_search: List[str] = []

        # "mapping" section in mapping.yaml
        for mapping in self._mapping_config.get_mapping():
            for mapping_table in mapping.get_table():
                if (
                    mapping_table.TableName not in tables_to_search
                    and self.is_target_label_found(
                        target_labels=target_labels,
                        configured_labels=mapping_table.Labels,
                    )
                ):
                    tables_to_search.append(mapping_table.TableName)

        # "metadata" section in mapping.yaml
        for metadata in self._mapping_config.get_metadata():
            if (
                metadata.TableName not in tables_to_search
                and self.is_target_label_found(
                    target_labels=target_labels,
                    configured_labels=metadata.Labels,
                )
            ):
                tables_to_search.append(metadata.TableName)

        return tables_to_search

    @staticmethod
    def is_target_label_found(
        target_labels: List[str], configured_labels: Dict[str, Any]
    ) -> bool:
        found_count: int = 0
        configured_label_key: str
        configured_label_value: str
        for configured_label_key, configured_label_value in configured_labels.items():
            for target_label in target_labels:
                target_label_key = target_label.split(":")[0]
                target_label_value = target_label.split(":")[1]
                if (
                    target_label_key == configured_label_key
                    and target_label_value == configured_label_value
                ):
                    found_count += 1

        return found_count == len(target_labels)


def is_cyclic(tables: List[str]) -> bool:
    """Floyd's cycle-finding algorithm

    Args:
        tables (list[str]): Detected tables

    Returns:
        bool: Table dependencies are cyclic or not
    """
    nodes: Dict[str, Node] = {}
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


def deep_merge(org: Dict[str, Any], add: Dict[str, Any]) -> Dict[str, Any]:
    """Merge nested dicts

    Args:
        org (dict[str, Any]): original dict
        add (dict[str, Any]): dict to add

    Returns:
        dict: merged dict
    """
    new: Dict[str, Any] = org
    for add_key, add_value in add.items():
        org_value: Dict[str, Any] = org.get(add_key, {})
        if add_key not in org:
            new[add_key] = add_value
        elif isinstance(add_value, dict):
            new[add_key] = deep_merge(org=org_value, add=add_value)
    return new

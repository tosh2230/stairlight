import enum
import json
import os
from logging import getLogger
from typing import Union

from google.cloud import storage

from . import config_key, map_key
from .config import Configurator
from .map import Map
from .source.gcs import GCS_URI_PREFIX

logger = getLogger(__name__)


class ResponseType(enum.Enum):
    """Enum: Execution result type of up/down command"""

    TABLE: str = "table"
    FILE: str = "file"

    def __str__(self):
        return self.name


class SearchDirection(enum.Enum):
    """Enum: Search direction"""

    UP = "Upstairs"
    DOWN = "Downstairs"

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
        self.mapping_config = None
        self._stairlight_config = self._configurator.read(
            prefix=config_key.STAIRLIGHT_CONFIG_FILE_PREFIX
        )
        if self._stairlight_config:
            if self.load_file:
                self.load_map()
            else:
                self._set_config()
                self._get_map()
                if self.save_file:
                    self.save_map()

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

    def has_stairlight_config(self) -> bool:
        """Exists stairlight configuration file or not

        Returns:
            bool: Exists stairlight configuration file or not
        """
        return self._stairlight_config is not None

    def _set_config(self) -> None:
        """Set config"""
        if not self._stairlight_config:
            logger.warning(
                f"{config_key.STAIRLIGHT_CONFIG_FILE_PREFIX}.y(a)ml' is not found."
            )
            return

        mapping_config_prefix = config_key.MAPPING_CONFIG_FILE_PREFIX
        if config_key.STAIRLIGHT_CONFIG_SETTING_SECTION in self._stairlight_config:
            settings = self._stairlight_config[
                config_key.STAIRLIGHT_CONFIG_SETTING_SECTION
            ]
            if config_key.MAPPING_PREFIX in settings:
                mapping_config_prefix = settings[config_key.MAPPING_PREFIX]
        self._mapping_config = self._configurator.read(prefix=mapping_config_prefix)

    def _get_map(self) -> None:
        """get a dependency map"""
        dependency_map = Map(
            stairlight_config=self._stairlight_config,
            mapping_config=self._mapping_config,
        )

        dependency_map.write()
        if self._mapping_config:
            self._mapped = dependency_map.mapped

        self._unmapped = dependency_map.unmapped

    def init(self, prefix: str = config_key.STAIRLIGHT_CONFIG_FILE_PREFIX) -> str:
        """Create Stairlight template file

        Args:
            prefix (str, optional):
                Template file prefix. Defaults to STAIRLIGHT_CONFIG_PREFIX.

        Returns:
            str: Template file name
        """
        return self._configurator.create_stairlight_template_file(prefix=prefix)

    def check(self, prefix: str = config_key.MAPPING_CONFIG_FILE_PREFIX) -> str:
        """Check mapped results and create a mapping template file

        Args:
            prefix (str, optional):
                Template file prefix. Defaults to MAPPING_CONFIG_PREFIX.

        Returns:
            str: Template file name
        """
        if self.load_file:
            logger.warning("Load option is used, skip checking.")
            return None
        elif not self._unmapped:
            return None

        return self._configurator.create_mapping_template_file(
            unmapped=self._unmapped, prefix=prefix
        )

    def save_map(self) -> None:
        """Save mapped results"""
        if self.save_file.startswith(GCS_URI_PREFIX):
            self.save_map_gcs()
        else:
            self.save_map_fs()

    def save_map_gcs(self) -> None:
        """Save mapped results to Google Cloud Storage"""
        blob = self.get_gcs_blob(self.save_file)
        blob.upload_from_string(
            data=json.dumps(obj=self._mapped, indent=2),
            content_type="application/json",
        )

    def save_map_fs(self) -> None:
        """Save mapped results to file system"""
        with open(self.save_file, "w") as f:
            json.dump(self._mapped, f, indent=2)

    def load_map(self) -> None:
        """Load mapped results"""
        if self.load_file.startswith(GCS_URI_PREFIX):
            self.load_map_gcs()
        else:
            self.load_map_fs()

    def load_map_gcs(self) -> None:
        """Load mapped results from Google Cloud Storage"""
        blob = self.get_gcs_blob(self.load_file)
        if not blob.exists():
            logger.error(f"{self.load_file} is not found.")
            exit()
        self._mapped = json.loads(blob.download_as_string())

    def load_map_fs(self) -> None:
        """Load mapped results from file system"""
        if not os.path.exists(self.load_file):
            logger.error(f"{self.load_file} is not found.")
            exit()
        with open(self.load_file) as f:
            self._mapped = json.load(f)

    @staticmethod
    def get_gcs_blob(gcs_uri: str) -> storage.Blob:
        bucket_name = gcs_uri.replace(GCS_URI_PREFIX, "").split("/")[0]
        key = gcs_uri.replace(f"{GCS_URI_PREFIX}{bucket_name}/", "")

        client = storage.Client(credentials=None, project=None)
        bucket = client.get_bucket(bucket_name)
        return bucket.blob(key)

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
                response.append(relative_map[next_table_name].get(map_key.URI))
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
        for configurations in self._mapping_config.get(
            config_key.MAPPING_CONFIG_MAPPING_SECTION
        ):
            for table_attributes in configurations.get(config_key.TABLES):
                if self.is_target_found(
                    targets=targets,
                    labels=table_attributes.get(config_key.LABELS, {}),
                ):
                    tables_to_search.append(table_attributes[config_key.TABLE_NAME])

        # "metadata" section in mapping.yaml
        for table_attributes in self._mapping_config.get(
            config_key.MAPPING_CONFIG_METADATA_SECTION
        ):
            if self.is_target_found(
                targets=targets,
                labels=table_attributes.get(config_key.LABELS, {}),
            ):
                tables_to_search.append(table_attributes[config_key.TABLE_NAME])

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

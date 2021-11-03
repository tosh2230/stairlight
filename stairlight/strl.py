import json
from logging import getLogger

import stairlight.config as config
from stairlight.map import Map

logger = getLogger(__name__)


class StairLight:
    def __init__(self, cached=False):
        self._maps = {}
        self._undefined_files = []

        if cached:
            pass
        else:
            dependency_map = Map()
            dependency_map.create()
            self._maps = dependency_map.maps
            self._undefined_files = dependency_map.undefined_files

    @property
    def maps(self):
        return self._maps

    @property
    def undefined_files(self):
        return [
            undefined_file.get("template_file")
            for undefined_file in self._undefined_files
        ]

    def all(self):
        return json.dumps(self._maps, indent=2)

    def up(self, table_name):
        result = self._maps.get(table_name)
        logger.debug(result)
        return json.dumps(result, indent=2)

    def down(self, table_name):
        result = {}
        for key in [k for k, v in self._maps.items() if v.get(table_name)]:
            result[key] = self._maps[key][table_name]
        logger.debug(result)
        return json.dumps(result, indent=2)

    def make_config(self):
        if self._undefined_files:
            config.make_template(self._undefined_files)
            logger.info("Undefined files are detected!: " + str(self._undefined_files))

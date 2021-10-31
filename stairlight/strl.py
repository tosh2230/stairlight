from stairlight.map import Map


class StairLight:
    def __init__(self):
        self._dependency_map = Map()
        self._dependency_map.create()
        self._maps = self._dependency_map.maps
        self._undefined_files = self._dependency_map.undefined_files

    @property
    def maps(self):
        return self._maps

    @property
    def undefined_files(self):
        return self._undefined_files

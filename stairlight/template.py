import pathlib
import re

import stairlight.config as config


class Template:
    def __init__(self):
        self._strl_config = config.read(config.STRL_CONFIG)

    def search(self):
        for source in self._strl_config.get("sources"):
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
                continue
            yield str(p)

    def is_excluded(self, template_file):
        result = False
        for exclude_file in self._strl_config.get("exclude"):
            if template_file.endswith(exclude_file):
                result = True
                break
        return result


def get_jinja_params(template_file):
    with open(template_file) as f:
        template_str = f.read()
    jinja_expressions = "".join(re.findall("{{[^}]*}}", template_str, re.IGNORECASE))
    return re.findall("[^{} ]+", jinja_expressions, re.IGNORECASE)

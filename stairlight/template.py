import pathlib
import re

TYPES = {
    "FS": "fs",
    "GCS": "gcs",
    "S3": "s3",
}


class Template:
    def __init__(self, strl_config):
        self._strl_config = strl_config

    def search(self):
        for source in self._strl_config.get("sources"):
            type = source.get("type")
            if type.casefold() == TYPES["FS"]:
                yield from self.search_fs(source)
            elif type.casefold() == TYPES["GCS"]:
                continue
            elif type.casefold() == TYPES["S3"]:
                continue

    def search_fs(self, source):
        path_obj = pathlib.Path(source.get("path"))
        for p in path_obj.glob(source.get("pattern")):
            if self.is_excluded(str(p)):
                continue
            yield TYPES["FS"], str(p)

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

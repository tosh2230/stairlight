import stairlight.config as config
import stairlight.template as template


class TestSuccess:
    config_reader = config.Reader("./config/")
    strl_config = config_reader.read(config.STRL_CONFIG)
    template = template.Template(strl_config=strl_config)

    def test_search_fs(self):
        source = {
            "type": "local",
            "path": "./tests/sql/main",
            "pattern": "*.sql",
        }
        result = []
        for file in self.template.search_fs(source=source):
            result.append(file)
        assert len(result) > 0

    def test_is_excluded(self):
        assert self.template.is_excluded("tests/sql/main/test_exclude.sql")

    @staticmethod
    def test_get_jinja_params():
        assert sorted(
            template.get_jinja_params(template.TYPES["FS"], "tests/sql/main/test_c.sql")
        ) == [
            "params.main_table",
            "params.sub_table_01",
            "params.sub_table_02",
        ]

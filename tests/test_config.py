import src.stairlight.config as config
import src.stairlight.template as template


class TestSuccess:
    configurator = config.Configurator(path="./config/")

    def test_read_map(self):
        assert self.configurator.read(prefix=config.MAP_CONFIG_PREFIX)

    def test_read_sql(self):
        assert self.configurator.read(prefix=config.STRL_CONFIG_PREFIX)

    def test_build_template_dict_fs(self):
        sql_template = template.SQLTemplate(
            map_config=self.configurator.read(prefix=config.MAP_CONFIG_PREFIX),
            source_type=template.SourceType.FS,
            file_path="tests/sql/main/test_undefined.sql",
        )
        undefined_files = [
            {
                "sql_template": sql_template,
                "params": [
                    "main_table",
                    "sub_table_01",
                    "sub_table_02",
                ],
            }
        ]

        value = {
            "file_suffix": sql_template.file_path,
            "tables": {
                "table": None,
                "params": {
                    "main_table": None,
                    "sub_table_01": None,
                    "sub_table_02": None,
                },
            },
        }

        expected = {"mapping": [value]}
        actual = self.configurator.build_template_dict(undefined_files=undefined_files)
        assert actual == expected

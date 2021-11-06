import stairlight.config as config
import stairlight.template as template


class TestSuccess:
    configurator = config.Configurator(path="./config/")

    def test_read_map(self):
        assert self.configurator.read(config.MAP_CONFIG)

    def test_read_sql(self):
        assert self.configurator.read(config.STRL_CONFIG)

    def test_build_template_dict(self):
        sql_template = template.SQLTemplate(
            map_config=self.configurator.read(config.MAP_CONFIG),
            source_type=template.SourceType.FS,
            file_path="tests/sql/test_c.sql",
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
        assert type(self.configurator.build_template_dict(undefined_files)) == dict

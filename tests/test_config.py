import stairlight.config as config


class TestSuccess:
    @staticmethod
    def test_read_map():
        assert config.read(config.MAP_CONFIG)

    @staticmethod
    def test_read_sql():
        assert config.read(config.STRL_CONFIG)

    @staticmethod
    def test_build_template_dict():
        undefined_files = [
            {
                "template_file": "tests/sql/test_c.sql",
                "params": [
                    "main_table",
                    "sub_table_01",
                    "sub_table_02",
                ],
            }
        ]
        assert type(config.build_template_dict(undefined_files)) == dict

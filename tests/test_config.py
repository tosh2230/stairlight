import stairlight.config as config


class TestSuccess:
    @staticmethod
    def test_read_map():
        assert config.read(config.MAP_CONFIG)

    @staticmethod
    def test_read_sql():
        assert config.read(config.SQL_CONFIG)

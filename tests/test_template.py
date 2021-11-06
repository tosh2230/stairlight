import pytest

import stairlight.config as config
import stairlight.template as template


class TestTemplateSourceSuccess:
    config_reader = config.Reader("./config/")
    strl_config = config_reader.read(config.STRL_CONFIG)
    map_config = config_reader.read(config.MAP_CONFIG)
    template_source = template.TemplateSource(
        strl_config=strl_config, map_config=map_config
    )

    def test_search_fs(self):
        source = {
            "type": "fs",
            "path": "./tests/sql/main",
            "pattern": "*.sql",
        }
        result = []
        for file in self.template_source.search_fs(source=source):
            result.append(file)
        assert len(result) > 0

    def test_search_gcs(self):
        source = {
            "type": "gcs",
            "project": None,
            "bucket": "stairlight",
            "prefix": "sql/",
        }
        result = []
        for file in self.template_source.search_gcs(source=source):
            result.append(file)
        assert len(result) > 0

    def test_is_excluded(self):
        assert self.template_source.is_excluded("tests/sql/main/test_exclude.sql")


@pytest.mark.parametrize(
    "source_type, file_path, params",
    [
        (
            template.SourceType.FS,
            "tests/sql/main/test_c.sql",
            {
                "main_table": "PROJECT_P.DATASET_Q.TABLE_R",
                "sub_table_01": "PROJECT_S.DATASET_T.TABLE_U",
                "sub_table_02": "PROJECT_V.DATASET_W.TABLE_X",
            },
        )
    ],
)
class TestSQLTemplateSuccess:
    config_reader = config.Reader("./config/")
    map_config = config_reader.read(config.MAP_CONFIG)

    def test_get_param_list(self, source_type, file_path, params):
        sql_template = template.SQLTemplate(source_type, file_path, self.map_config)
        assert sql_template.get_param_list() == [params]

    def test_get_mapped_table(self, source_type, file_path, params):
        sql_template = template.SQLTemplate(source_type, file_path, self.map_config)
        assert (
            sql_template.get_mapped_table(params=params)
            == "PROJECT_J.DATASET_K.TABLE_L"
        )

    def test_get_jinja_params(self, source_type, file_path, params):
        sql_template = template.SQLTemplate(source_type, file_path, self.map_config)
        assert sorted(sql_template.get_jinja_params()) == [
            "params.main_table",
            "params.sub_table_01",
            "params.sub_table_02",
        ]
